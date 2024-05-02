defmodule RAP.Storage do
  @moduledoc """
  This module really serves as 'plumbing' to name state and to store
  results. The named struct is purposefully near-identical to the table
  in the mnesia cache row because it should hold state and let us call
  this immediately prior to injecting them into the cache.

  There is a slight complication of different data formats for some of
  these fields (e.g. LinkML YAML schemata vs turtle equivalent). Make it
  work for one and then extend it.
  """
  use Amnesia

  alias RAP.Job.Runner

  defstruct [ :uuid, :manifest, :resources, :results ]

  defdatabase DB do
    deftable Job, [:uuid, :manifest, :resources, :results], type: :bag
  end 
end

defmodule RAP.Storage.Transactions do

  require Amnesia
  require Amnesia.Helper
  require RAP.Storage.DB.Job
  
  def prep_data_set(uuid, fp, schema, manifest, results) do
    Amnesia.transaction do
      %RAP.Storage.DB.Job{
	uuid:            uuid,
	resource_file:   fp,
	resource_schema: schema,
	job_manifest:    manifest,
	job_results:     results }
      |> RAP.Storage.DB.Job.write()
    end
  end
  
  def feasible?(uuid) do
    Amnesia.transaction do
      case RAP.Storage.DB.Job.read(uuid) do
	nil -> true
	_   ->
	  Logger.info("Found job UUID #{uuid} in job cache!")
	  false
      end
    end
  end
  
end

defmodule RAP.Storage.Monitor do
  @moduledoc """
  The idea is that this producer only finds UUIDs, and then produces
  events which are used to fetch the data.

  This locates separates the recursive checks from the actual disk usage,
  since the concurrency matters more for fetching data, as there may be
  race conditions and there's quite an extensive set of checks necessary
  to fetch data in a way that doesn't produce race conditions or over-use
  the network.

  Additionally, there is a distinctive issue of having to renew GCP
  authentication tokens / cookies. Locating this here with a bunch of
  callbacks / IPC is more robust than really recursive functions.
  """
  use GenStage
  require Logger

  alias GoogleApi.Storage.V1.Connection,    as: GCPConn
  alias GoogleApi.Storage.V1.Model.Object,  as: GCPObj
  alias GoogleApi.Storage.V1.Model.Objects, as: GCPObjs
  alias GoogleApi.Storage.V1.Api.Objects,   as: GCPReqObjs

  alias RAP.Storage.Monitor
  alias RAP.Storage.Transactions

  # Hard-code these for brevity, should be configurable at program init.
  # Same cache dir for both the 'local' store and GCP
  @gcp_scope "https://www.googleapis.com/auth/cloud-platform"
  @interval_seconds 300
  @cache_dir "./data_cache"

  # This is the one file name which must be hard-coded, because there's
  # no way to know which file is which. The manifest describes all other
  # files of interest, and further describes the shape of the job and
  # the results to be run.
  @manifest "manifest.ttl"

  def start_link initial_ts do
    Logger.info "Called Storage.Monitor.start_link (initial_ts = #{inspect initial_ts})"
    GenStage.start_link __MODULE__, initial_ts, name: __MODULE__
  end

  def init initial_ts do
    @doc """
    Note that there is a single cache of UUIDs in memory, just like there
    is a single cache of data and a single database of results.

    The idea is that any given store uses randomly generated UUIDs, which
    are de-facto unique, so there is no chance of conflict here.
    """
    Logger.info "Called Storage.Monitor.init (initial_ts = #{inspect initial_ts})"
    :ets.new(:uuid, [:set, :public, :named_table])
    { :producer, initial_ts }
  end

  def handle_demand demand, ts do
    @doc """
    This is the best place to put the polling for new UUIDs,
    i.e. make a GenStage call which will poll for UUIDs, providing
    that there the interval in question has elapsed.
    """
    Logger.info "Called `Storage.Monitor.handle_demand (demand = #{insd}, state = #{inss})'"
    { :noreply, events, ts }
  end
  
  def new_connection() do
    with {:ok, token} <- Goth.Token.for_scope(@gcp_scope),
         session      <- GCPConn.new(token.token) do
      Logger.info "Called Storage.GCP.new_connection/0"
      {:ok, session}
    else
      :error -> {:error, "Cannot obtain token/session" }
    end
  end

  @doc """
  Trigger the monitoring of a given GCP bucket
  """
  def watch_bucket(bucket_name) do
    Logger.info("Call `Storage.GCP.watch_gcp (bucket: #{bucket_name})'")
    with {:ok, session} <- new_connection()
      do
      initial_ts = DateTime.utc_now() |> DateTime.to_unix()
      monitor_session(session, bucket_name, initial_ts, initial: true)
    else
      { :error, _ } = err -> err
    end
  end

  def features_helper(%GCPObj{} = obj) do
    Logger.info "Called `Storage.GCP.features_helper' on object #{obj.name}"
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
    Logger.info "Called `Storage.GCP.uuid_helper' on object #{gcp_object.name}"
    with [owner, _date, uuid, res | k] <- String.split(gcp_object.name, "/"),
	 is_directory <- length(k) > 0
    do
      gcp_object
      |> Map.merge(
	%{owner: owner, uuid: uuid, file: res, dir: is_directory}
      )
    else
      [_|_] -> nil
    end
  end

  @doc """
  Simple wrapper around Erlang term storage table of UUIDs
  """
  defp ets_feasible?(uuid) do
    case :ets.lookup(:uuid, uuid) do
      [] -> true
      _  ->
	Logger.info("Job UUID #{uuid} is already running, cannot add to ETS.")
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
      Logger.info "Initial call to `GCP.Storage.monitor_session' with interval #{inspect @interval_seconds}s"
    end
    curr_ts = DateTime.utc_now() |> DateTime.to_unix
    if (curr_ts - ts) < @interval_seconds and !initial do
      monitor_session(session, bucket_name, ts)
    else
      with {:ok, %GCPObjs{items: objects}} <- GCPReqObjs.storage_objects_list(session, bucket_name)
	do
	objects
	|> Enum.map(&features_helper/1)
	|> Enum.map(&uuid_helper/1)
	|> then(fn (uuids) ->
	  GenStage.cast(
	    __MODULE__,
	    {:spew_uuids, :source_gcp, session, bucket_name, uuids})
	end)
	new_ts = DateTime.utc_now() |> DateTime.to_unix
	monitor_session(session, bucket_name, new_ts)
      else
	{:error, error = %Tesla.Env{status: 401}} ->
	  Logger.info "Query of GCP bucket #{bucket_name} appeared to time out, seek new session"
	  # If this bit fails, we know there's really something up!
	  {:ok, new_session} = new_connection()
	  monitor_session(new_session, bucket_name, ts) 
        {:error, error = %Tesla.Env{status: code, url: uri, body: msg}} ->
	  Logger.info "Query of GCP failed with code #{code} and error message #{msg}"
	  {:error, uri, code, msg}
      end
    end
  end

  defp dl_success?(obj, body) do
    purported = obj.md5
    actual    = :crypto.hash(:md5, body) |> Base.encode64()
    if purported == actual do
      { :ok, "correct", purported }
    else
      { :error, "incorrect", purported }
    end  
  end

  def handle_cast({:spew_uuids, :source_gcp, session, bucket_name, uuids}, state) do
    {:noreply, {:source_gcp, session, bucket_name, uuids}, state}
  end
end
