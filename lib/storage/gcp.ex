defmodule RAP.Storage.GCP do
  @moduledoc """
  This is the first stage of the pipeline, but the first stage could
  easily be monitoring local storage or some other cloud storage thing.

  The files are uploaded as if there's a directory structure, but there
  isn't really because the GCP storage buckets are a flat object-based
  structure. Thus associating and gathering files into jobs is somewhat
  non-trivial and more involved than it should be.
  """
  use GenStage
  require Logger

  alias GoogleApi.Storage.V1.Api.Objects, as: GCPReqObjs

  alias RAP.Storage.{PreRun, MidRun, Monitor}
  alias RAP.Provenance.Work

  @type stage_dispatcher   :: GenStage.BroadcastDispatcher | GenStage.DemandDispatcher | GenStage.PartitionDispatcher
  @type stage_type         :: :consumer | :producer_consumer | :producer
  @type stage_subscription :: {atom(), min_demand: integer(), max_demand: integer()}
  @type stage_state :: %{
    stage_dispatcher:    stage_dispatcher(),
    stage_invoked_at:    integer(),
    stage_subscriptions: [stage_subscription()],
    stage_type:          atom()
  }

  def start_link([] = initial_state) do
    Logger.info "Start link to Storage.GCP"
    GenStage.start_link __MODULE__, initial_state, name: __MODULE__
  end

  def init([] = _initial_state) do
    Logger.info "Initialise Storage.GCP"
    curr_ts = DateTime.utc_now() |> DateTime.to_unix()
    stage_state = %{
      stage_invoked_at:    curr_ts,
      stage_type:          :producer_consumer,
      stage_subscriptions: [{ Monitor, min_demand: 0, max_demand: 1 }],
      stage_dispatcher:    GenStage.DemandDispatcher
    }
    { stage_state.stage_type,
      stage_state,
      subscribe_to: stage_state.stage_subscriptions,
      dispatcher:   stage_state.stage_dispatcher   }
  end
  
  def handle_events(events, _from, %{} = stage_state) do
    Logger.info "Called `Storage.GCP.handle_events (events = #{inspect events}, …)'"
    input_work = events |> Enum.map(& &1.work)
    Logger.info "Storage.GCP received objects with the following work defined: #{inspect input_work}"
    
    processed = events
    |> Enum.map(&coalesce_job(&1, stage_state))
    
    { :noreply, processed, stage_state }
  end

  @spec wrap_gcp_fetch(%Monitor{}, Tesla.Client.t()) :: {:ok, Tesla.Env.t()} | {:error, any()}
  defp wrap_gcp_fetch(%Monitor{} = obj, %Tesla.Client{} = session) do
    GCPReqObjs.storage_objects_get(session, obj.gcp_bucket, obj.gcp_name, [alt: "media"], decode: false)
  end

  @doc """
  Given an MD5 checksum by the API listing (in the %Monitor{} struct):
  1. Check that the file exists.
  2. If so, verify checksum.
  3. If not, download and verify checksum.
  (Make sure this isn't recursive.)
  
  Events are a `%Storage.PreRun{}' struct: UUID, index object, and all other
  objects. Objects are a `%Monitor{}' struct, which includes all the
  information needed to fetch.
  
  GCP OAuth tokens apparently have an expiry of one hour. Sessions are
  retrieved from the monitor process (which handles renewing these) with
  the GenStage call `:yield_session'. There is relatively low risk of
  race conditions here, because the stage runs immediately after the
  monitor process hands over new events, while catching expired sessions.

  The data cache directory effectively mirrors the GCP object store's
  purported structure but we have the benefit of the mnesia database to
  cache results.
  """
  @spec fetch_object(%Monitor{}, String.t()) :: {:ok, String.t()} | {:error, String.t()} | {:error, String.t(), integer(), String.t()}
  def fetch_object(%Monitor{} = obj, target_dir) do
    target_base = obj.path
    # output_file => target_full
    target_full = "#{target_dir}/#{target_base}"
    Logger.info "Polling GCP storage bucket for flat object #{obj.gcp_name}, with target #{target_full}"
    with false <- File.exists?(target_full) && PreRun.dl_success?(obj.gcp_md5, File.read!(target_full), opts: [input_md5: true]),
         session <- GenStage.call(Monitor, :yield_session),
	 {:ok, %Tesla.Env{body: body, status: 200}} <- wrap_gcp_fetch(obj, session),
	 :ok <- File.write(target_full, body) do
      Logger.info "Successfully wrote #{target_full}, file base name is #{target_base}"
      {:ok, target_base}
    else
      true ->
	Logger.info "File #{target_base} already exists and MD5 checksum matches API's"
        {:ok, target_base}
      {:error, %Tesla.Env{status: code, url: uri, body: msg}} ->
	Logger.info "Query of GCP failed with code #{code} and error message #{msg}"
        {:error, uri, code, msg}
      {:error, reason} ->
	Logger.info "Error writing file due to #{inspect reason}"
        {:error, reason}
    end
  end

  @spec fetch_job_deps(%PreRun{}, String.t()) :: {:ok, String.t(), [String.t()]} | {:error, String.t()} | {:error, String.t(), [String.t()]}
  def fetch_job_deps(%PreRun{} = job, cache_dir) do
    Logger.info "Called `Storage.GCP.fetch_job_deps' for job with UUID #{job.uuid}"
    all_resources = [job.index | job.resources]
    target_dir = "#{cache_dir}/#{job.uuid}"
    
    with :ok     <- File.mkdir_p(target_dir),
	 signals <- Enum.map(all_resources, &fetch_object(&1, target_dir)) do

      errors = signals
      |> Enum.reject(fn {:ok, _fp} -> true
	                 _         -> false
                     end)
      if (length errors) > 0 do
	Logger.info "Job #{job.uuid}: Found errors #{inspect errors}"
	{:error, job.uuid, errors}	
      else
	Logger.info "Job #{job.uuid}: No errors to report"
	file_bases = signals |> Enum.map(&elem(&1, 1))
	{:ok, job.uuid, file_bases}
      end
    else
      error -> error
    end
  end

  @spec coalesce_job(%PreRun{}, stage_state()) :: %MidRun{}
  def coalesce_job(%PreRun{} = job, %{} = stage_state) do
    work_started_at = DateTime.utc_now() |> DateTime.to_unix()
    
    with {:ok, cache_dir}  <- Application.fetch_env(:rap, :cache_directory),
         {:ok, index_base} <- Application.fetch_env(:rap, :index_file),
	 {:ok, uuid, file_bases}      <- fetch_job_deps(job, cache_dir),
         {:ok, index_contents}        <- File.read("#{cache_dir}/#{job.uuid}/#{index_base}"),
         [m_yaml, m_ttl, base, m_uri] <- String.split(index_contents, "\n")
    do
      Logger.info "Index file is #{inspect index_base}"

      manifest_id = RDF.IRI.new(m_uri)
      
      non_manifest_bases = file_bases
      |> List.delete(m_yaml)
      |> List.delete(m_ttl)
      |> List.delete(index_base)
      Logger.info "Non-manifest files are #{inspect non_manifest_bases}"      

      new_work =
	Work.append_work(
	  job.work, __MODULE__, :working,
	  work_started_at,
	  stage_state, [manifest_id], []
	)

      %MidRun{ uuid:          uuid,
	       signal:        :working,
	       work:          new_work,
	       data_source:   "gcp",
	       manifest_iri:  manifest_id,
	       manifest_yaml: m_yaml,
	       manifest_ttl:  m_ttl,
	       resources:     non_manifest_bases,
	       base_prefix:   base              }
    else
      {:error, uuid, errors} -> {:error, uuid, errors}
      {:error, reason} ->
	Logger.info "Could not read index"
	new_work = Work.append_work(job.work, __MODULE__, reason, work_started_at, stage_state)
	%MidRun{ uuid: job.uuid, signal: reason, work: new_work, data_source: "gcp" }
      [] ->
	Logger.info "Index file is empty!"
	{:error, :empty_index, job.uuid}
	new_work = Work.append_work(job.work, __MODULE__, :empty_index, work_started_at, stage_state)
        %MidRun{ uuid: job.uuid, signal: :empty_index, work: new_work, data_source: "gcp" }
      _ ->
	Logger.info "Malformed index file!"
	new_work = Work.append_work(job.work, __MODULE__, :bad_index, work_started_at, stage_state)
	%MidRun{ uuid: job.uuid, signal: :bad_index, work: new_work, data_source: "gcp" }
    end
  end

end
