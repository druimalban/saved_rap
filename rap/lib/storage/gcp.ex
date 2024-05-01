defmodule RAP.Storage.GCP do

  use GenStage
  require Logger

  alias RAP.Storage.Transactions
  alias GoogleApi.Storage.V1.Connection,    as: GCPConn
  alias GoogleApi.Storage.V1.Model.Object,  as: GCPObj
  alias GoogleApi.Storage.V1.Model.Objects, as: GCPObjs
  alias GoogleApi.Storage.V1.Api.Objects,   as: GCPReqObjs

  @gcp_scope "https://www.googleapis.com/auth/cloud-platform"

  def start_link initial_ts do
    Logger.info "Called Storage.GCP.start_link (initial_ts = #{inspect initial_ts})"
    GenStage.start_link __MODULE__, initial_ts, name: __MODULE__
  end

  def init initial_ts do
    Logger.info "Called Storage.GCP.init (initial_ts = #{inspect initial_ts})"
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
  Trigger the monitoring of a given GCP bucket
  """
  def watch_gcp(bucket_name) do
    Logger.info("Call `Producer.watch_gcp (bucket: #{bucket_name})'")
    with {:ok, token} <- Goth.Token.for_scope(@gcp_scope),
	 session      <- GCPConn.new(token.token)
      do
      initial_ts = DateTime.utc_now() |> DateTime.to_unix()
      monitor_session(session, bucket_name, initial_ts, initial: true)
    else
      :error -> {:error, "Cannot obtain token"}
    end
  end

  def features_helper(%GCPObj{} = obj) do
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
  Simple wrapper around Erlang term storage table of UUIDs
  """
  defp ets_feasible?(uuid) do
    case :ets.lookup(:uuid, uuid) do
      [] -> true
      _  ->
	Logger.info("Found job UUID #{uuid} is running!")
	false
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
      with {:ok, %GCPObjs{items: objects}} <- GCPReqObjs.storage_objects_list(session, bucket_name)
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

    insert_uuid = fn(uuid) ->
      fps = grouped_file_objs |> Map.get(uuid) 
      Logger.info "#{uuid} => #{inspect fps}"
      :ets.insert({uuid})
      Logger.info("Inserted #{uuid} into Erlang term storage (:ets)")
    end
    uncached_jobs
    |> Enum.each(insert_uuid)
    
    {:noreply, [], state} # Don't yet produce events
  end

  def handle_cast({:storage_changed, str_invocation}, state) do
    Logger.info "Received fake :storage_changed signal for #{str_invocation}"
    { :noreply, [str_invocation], state }
  end

end
