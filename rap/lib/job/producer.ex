defmodule RAP.Job.Spec do
  @moduledoc """
  At the moment, this module just includes a single struct definition.
  The aim here is to separate the declaration of the structure of the
  job from the structure of the producer, which holds its own state.

  This is better than having the job producer functions return a list
  of its named struct.
  """
  defstruct [ :title, :description, :type, :pairs_descriptive, :pairs_collected, :pairs_modelled ]
  
end

defmodule RAP.Job.Staging do

  defstruct [ :signal, :variable, :label, :target ]
  
end

defmodule RAP.Job.Producer do
  @moduledoc """
  This is the producer stage of the RAP, which, given a turtle manifest
  path, attempts to read this into an RDF graph, and then load it into a
  struct.

  The producer further generates a list of jobs which are to be run,
  based on the manifest contents.

  It is possible that there will be no jobs possible, probably because
  the manifest is ill-formed.
  """
  alias RAP.Storage.Transactions
  alias RAP.Vocabulary.SAVED
  alias RAP.Manifest.{TableDesc, ColumnDesc, JobDesc, ManifestDesc}
  alias RAP.Job.{Producer, Spec, Staging}

  alias GoogleApi.Storage.V1.Connection,    as: GcpConn
  alias GoogleApi.Storage.V1.Model.Object,  as: GcpObj
  alias GoogleApi.Storage.V1.Model.Objects, as: GcpObjs
  alias GoogleApi.Storage.V1.Api.Objects,   as: GcpReqObjs
  
  use GenStage
  require Logger

  import :timer, only: [ sleep: 1 ]

  defstruct [ :title, :description, :staging_jobs ]
  @scope "https://www.googleapis.com/auth/cloud-platform"
  
  def start_link initial_ts do
    Logger.info "Called Job.Producer.start_link (initial_ts = #{inspect initial_ts})"
    GenStage.start_link __MODULE__, initial_ts, name: __MODULE__
  end

  def init initial_ts do
    Logger.info "Called Job.Producer.init (initial_ts = #{inspect initial_ts})"
    :ets.new(:uuid, [:set, :public, :named_table])
    { :producer, initial_ts }
  end
  
  def handle_demand demand, ts do
    insd = inspect demand
    inss = inspect ts
    events = []
    Logger.info "Called Job.Producer.handle_demand (demand = #{insd}, state = #{inss})"
    { :noreply, events, ts }
  end

  @doc """
  Given a directory, check for sub-directories which will have UUIDs
  This is primarily useful if we're monitoring local storage.
  """
  def get_local_uuids(directory) do
    directory
    |> File.ls()
    |> elem(1)
    |> Enum.map(fn(dir) -> {dir, File.stat("/etc" <> "/" <> dir)} end)
    |> Enum.filter(fn ({_, {:ok, %File.Stat{type: type}}}) -> type == :dir end)
    |> Enum.map(&elem &1, 0)
  end
  
  def watch_gcp(bucket_name) do
    Logger.info("Call `Producer.watch_gcp (bucket: #{bucket_name})'")
    with {:ok, token} <- Goth.Token.for_scope(@scope),
	 session      <- GcpConn.new(token.token)
      do
      initial_ts = DateTime.utc_now() |> DateTime.to_unix()
      monitor_session(session, bucket_name, initial_ts, initial: true)
    else
      :error -> {:error, "Cannot obtain token"}
    end
  end

  def features_helper(%GcpObj{} = obj) do
    Logger.info "Called `features_helper' on object #{obj.name}"
     %{id:         obj.id,
       bucket:     obj.bucket,
       md5:        obj.md5Hash,
       uri_media:  obj.mediaLink,
       uri_self:   obj.selfLink,
       ts_created: obj.timeCreated,
       ts_updated: obj.updated,
       mime_type:  obj.contentType,
       name:       obj.name       }
  end

  @doc """
  Given an object returned by GCP, the files of which are uploaded in the
  following format, extract the useful parts from this well-known
  object name.

  The flat objects' names should be a string in the form
  `<owner>/<yyyymmdd>/<uuid>/<file>', with or without a trailing slash.
  The trailing slash conveniently tells us that it's a directory; objects
  have `text/plain' for directories, `application/octet_stream' for empty
  files.
  """
  def uuid_helper(gcp_object) do
    Logger.info "Called `uuid_helper' on object #{gcp_object.name}"
    with [owner, _date, uuid, res | k] <- String.split(gcp_object.name, "/"),
	 is_directory <- length(k) > 0
    do
      gcp_object
      |> Map.merge(
	%{owner: owner, uuid: uuid, file: res, dir: is_directory}
      )
    else
      [x] -> {:error, gcp_object}
    end
  end

  @doc """
  Monitors the GCP objects every five minutes.

  The Google GCP buckets have a fake directory structure: they're by
  default stored in this flat object-based structure which the web
  interface hides.

  The API has several 'Folder' and 'ManagedFolder' functions but these
  only work on buckets which have a 'hierarchical' namespace, and the
  documentation is so bad that there does not appear to be a way to set
  this on the web interface.

  If the request is successful, cast to the check_then_run function
  which will check the UUIDs against 1. completed jobs (an mnesia
  database) and 2. jobs, if any, currently running.
  """
  def monitor_session(session, bucket_name, ts, initial \\ false) do
    if initial do
      Logger.info "Initial call to monitor_session (_session, bucket: #{bucket_name}, time stamp: #{inspect ts})"
    end
    curr_ts = DateTime.utc_now() |> DateTime.to_unix
    if (curr_ts - ts) < 300 and !initial do
      monitor_session(session, bucket_name, ts)
    else
      with {:ok, %GcpObjs{items: objects}} <- GcpReqObjs.storage_objects_list(session, bucket_name)
      do
	objects
	|> Enum.map(&features_helper/1)
	|> Enum.map(&uuid_helper/1)
	|> then(&GenStage.cast(__MODULE__, {:check_then_run, &1}))
	
	new_ts = DateTime.utc_now() |> DateTime.to_unix
	monitor_session(session, bucket_name, new_ts)
      else
	{:error, error = %Tesla.Env{status: code, url: uri, body: msg}} ->
	  Logger.info "Query of GCP failed with code #{code} and error message #{msg}"
	  {:error, uri, code, msg}
      end
    end
  end
  
#  defp extract_gcp_object(gcp_object) do
#    acc_pattern  = "[A-z]|[0-9]+"
#    date_pattern = "[0-9]{8}"
#    uuid_pattern = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
#    atom_pattern = "[A-z]+[[0-9]|[A-z]|_|\.]*"
#    string_regex = "^(#{acc_pattern})\/(#{date_pattern})\/(#{uuid_pattern})\/(#{atom_pattern})$"
#    actual_regex = ~r"#{string_regex}"
#
#    case Regex.run(actual_regex, gcp_object.name) do
#      [orig, owner, date, uuid, resource] -> {:ok, owner, date, uuid, resource, gcp_object.created}
#      nil -> {:error, :invalid_gcp_object}
#    end
#  end
  
  defp ets_feasible?(uuid) do
    case :ets.lookup(:uuid, uuid) do
      [] -> true
      _  ->
	Logger.info("Found job UUID #{uuid} is running!")
	false
    end
  end

  # Group into enum:
  # |> Enum.group_by(&(&1.uuid))
  #
  @doc """
  There are two elements of this. Firstly, we have a notion of UUIDs
  which have been cached. There is a single row per UUID because the UUID
  has a notion of a job, or collection of jobs. Secondly, we have a
  notion of files, and it is expected that there would be many different
  files associated with an UUID.

  The most efficient way to check which jobs are feasible is to summarise
  the list of objects into unique UUIDs. This is just a map to extract
  the UUIDs then run `Enum.uniq/2'. We then filter these unique UUIDs
  using `Transactions.feasible/1' and `ets_feasible/1', which produce a
  set of jobs which are neither cached (finished) nor currently running
  (in Erlang term storage ~ `:ets').

  We further group all objects provided by UUID. For each UUID which made
  it out of the filtering, use this as the key to lookup the collection
  of files associated with the UUID.

  There are certain well-founded assumptions which are governed by the
  functionality of `fisdat' and/or `fisup'. Firstly, there is at most one
  manifest per UUID, because that's the single file we fed into `fisup'
  in order to upload the files (and a unique UUID is created per
  invocation of `fisup'). Secondly, the `fisup' program will have
  normalised the name of the manifest to `manifest.ttl'. Indeed, this is
  the only normalisation that need take place, because as it currently
  stands, the manifest refers to all of the dependent files.
  """
  def handle_cast({:check_then_run, objs}, state) do
    Logger.info "Received cast :check_then_run"
    uncached_jobs = objs
    |> Enum.map(& &1.uuid)
    |> Enum.uniq()
    |> Enum.filter(&Transactions.feasible?/1)
    |> Enum.filter(&ets_feasible?/1)
    
    grouped_file_objs = objs
    |> Enum.filter(&  !&1.dir)
    |> Enum.group_by(& &1.uuid)

    show_obj_tiny = fn(uuid) ->
      fps = grouped_file_objs |> Map.get(uuid) 
      Logger.info "#{uuid} => #{inspect fps}"
    end
    
    #grouped_file_objs |> Enum.each(&show_obj_tiny.(&1))
    uncached_jobs |> Enum.each(show_obj_tiny)
    
    {:noreply, [], state}
  end

  def handle_cast({:storage_changed, str_invocation}, state) do
    Logger.info "Received fake :storage_changed signal for #{str_invocation}"
    { :noreply, [str_invocation], state }
  end

  @doc """
  Generic helper function which sends the :try_jobs signal. This is
  a place-holder which lets us manually trigger a job, since the aim is
  to monitor GCP / other storage backend for new manifests to run. This
  will probably be performed by an earlier stage.
  """
  def trigger manifest_path do
    Logger.info "Called Job.Producer.trigger ()"
    #({ #{inspect job_code} ,#{inspect col0} ,#{inspect col1} })"
    GenStage.cast __MODULE__, {:try_jobs, manifest_path}
  end

  @doc """
  Given the :try_jobs signal, attempt to devise a set of jobs which are
  to be run.
  """
  def handle_cast {:try_jobs, manifest_path}, state do
    Logger.info "Received :try_jobs signal"
    Logger.info "Building RDF graph from turtle manifest #{manifest_path}"
    {:ok, graph}  = RDF.Turtle.read_file manifest_path

    Logger.info "Loading RDF graph into Elixir/Grax structs"
     {:ok, struct} = Grax.load graph, SAVED.RootManifest, ManifestDesc

    Logger.info "Detecting feasible jobs"
    base_iri  = RDF.IRI.to_string graph.base_iri
    feasible_jobs = generate_jobs base_iri, struct
    {:noreply, [feasible_jobs], state}
  end

  @doc """
  Given a manifest, check the following:

  1. Each job has an ID (`atomic_name'), a title (not important for a job),
     a flag to say whether it is auto-generated (important), and a list of
     sources.
     
  2. For the list of sources, while there is an ID (the `atomic_name') field,
     for each element, there is an additional field which defines which table the
     source is derived from. This is distinct because it is not possible to have
     duplicate fields in the RDF, so this field cannot be an identifier.

     When auto-generating these sources, all I do is prepend 'source_example_'
     to the table field. This is probably reasonably self-explanatory, so it is
     good style to suggest.

  In terms of naming:

  These are a bunch of nested maps which preserve the structure of the job
  manifest. I.e., for a given job, and some number of tables against which to
  check there exist the table and/or columns, return the same job source
  structure annotated with 1. the table and 2. valid/extrenuous columns.

  The mapping functions over the *job* should be called something like
  <operation>_over_job and the mapping functions over the *source* component of
  the job should be called something like <operation>_over_job_source.
  """
  defp compare_table_call(base_iri_text, tables, %ColumnDesc{table: table, column: label, variable: var}) do
    target_iri = RDF.iri(base_iri_text <> table)
    target_table = tables
    |> Enum.find(fn (tab) ->
      target_iri == tab.__id__
    end)
    case target_table do
      nil -> %Staging{ signal: :invalid_table, variable: var, label: label, target: target_iri   }
      tab -> %Staging{ signal: :valid_table,   variable: var, label: label, target: target_table }
    end
  end

  def sort_scope(%Staging{signal: :valid_table}, %Staging{signal: :invalid_table}), do: true
  def sort_scope(%Staging{signal: :invalid_table}, %Staging{signal: :valid_table}), do: false
  def sort_scope(%Staging{variable: var0}, %Staging{variable: var1}), do: var0 < var1

  defp job_scope_against_tables(base_iri_text, tables, job) do
    run_scope = fn (src) -> src
      |> Enum.map(&compare_table_call(base_iri_text, tables, &1))
      |> Enum.sort(&sort_scope/2)
    end   
    %Spec{
      title:             job.title,
      description:       job.description,
      type:              job.job_type,
      pairs_descriptive: run_scope.(job.job_scope_descriptive),
      pairs_collected:   run_scope.(job.job_scope_collected),
      pairs_modelled:    run_scope.(job.job_scope_modelled)
    }
  end

  def generate_jobs(base_iri_text, %ManifestDesc{} = manifest) do
    processed_jobs = manifest.jobs
    |> Enum.map(&job_scope_against_tables(base_iri_text, manifest.tables, &1))
    %Producer{
      title:        manifest.title,
      description:  manifest.description,
      staging_jobs: processed_jobs
    }    
  end
end
