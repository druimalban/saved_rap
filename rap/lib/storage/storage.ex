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
  
  def prep_data_set(uuid, manifest, resources, results) do
    Amnesia.transaction do
      %RAP.Storage.DB.Job{
	uuid:      uuid,
	manifest:  manifest,
	resources: resources,
	results:   results  }
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

  alias RAP.Application
  alias RAP.Storage.Monitor
  alias RAP.Storage.Transactions

  # No circumstances in which this is configurable: it's so tied to API usage
  @gcp_scope "https://www.googleapis.com/auth/cloud-platform"
  
  def start_link initial_state do
    Logger.info "Called Storage.Monitor.start_link (initial_state = #{inspect initial_state})"
    GenStage.start_link __MODULE__, initial_state, name: __MODULE__
  end

  @doc """
  Note that there is a single cache of UUIDs in memory, just like there
  is a single cache of data and a single database of results.

  The idea is that any given store uses randomly generated UUIDs, which
  are de-facto unique, so there is no chance of conflict here.
  """
  def init(%RAP.Application{} = initial_state) do
    Logger.info "Called Storage.Monitor.init (initial_state: #{inspect initial_state})"
    :ets.new(:uuid, [:set, :public, :named_table])
    with {:ok, initial_session} <- new_connection() do
      revised_state = initial_state |> Map.put(:gcp_session, initial_session)
      {:producer, revised_state}
    else
      {:error, _msg} -> {:producer, []}
    end
  end
  
  @doc """
  This is the best place to put the polling for new UUIDs, i.e. make a
  GenStage call which will poll for UUIDs, providing that there the
  interval in question has elapsed.

  This avoids the need at all for the watch_bucket/1 function calling
  the recursive monitor_session function.
  """
  def handle_demand(demand, %RAP.Application{interval_seconds: interval, time_stamp: ts} = state) do
    Logger.info "Called `Storage.Monitor.handle_demand (demand = #{inspect demand}, ts = #{inspect ts})'"

    current_ts = DateTime.utc_now() |> DateTime.to_unix()
    elapsed = current_ts - ts
    
    with true <- elapsed >= interval,
	 {:ok, uuids, new_state} <- monitor_proper(state) do
      # {:noreply, uuids, new_state}
      {:noreply, [], new_state}

    else
      false ->
	Logger.info "Received demand for new events!"
	{:noreply, [], state}
      {:error, _, _, _} -> {:noreply, [], state}
    end
  end

  def monitor_proper(%RAP.Application{} = state) do
    session = state.gcp_session
    bucket  = state.gcp_bucket
    with {:ok, %GCPObjs{items: objects}} <- GCPReqObjs.storage_objects_list(session, bucket)
      do
        uuids = objects
	|> Enum.map(&features_helper/1)
	|> Enum.map(&uuid_helper/1)
      
        current_ts = DateTime.utc_now() |> DateTime.to_unix()
	new_state  = state |> Map.put(:time_stamp, current_ts)
	{:ok, uuids, new_state}
      else
	{:error, error = %Tesla.Env{status: 401}} ->
	  Logger.info "Query of GCP bucket #{bucket} appeared to time out, seek new session"
	  # If this bit fails, we know there's really something up!
	  {:ok, new_session} = new_connection()
	  new_state = state
	  |> Map.put(:gcp_session, new_session)
	  monitor_proper(new_state)
        {:error, error = %Tesla.Env{status: code, url: uri, body: msg}} ->
	  Logger.info "Query of GCP failed with code #{code} and error message #{msg}"
	  {:error, uri, code, msg}
      end
  end
  
  def new_connection() do
    with {:ok, token} <- Goth.Token.for_scope(@gcp_scope),
         session      <- GCPConn.new(token.token) do
      Logger.info "Called Storage.Monitor.new_connection/0"
      {:ok, session}
    else
      :error -> {:error, "Cannot obtain token/session" }
    end
  end


  def features_helper(%GCPObj{} = obj) do
    Logger.info "Called `Storage.Monitor.features_helper' on object #{obj.name}"
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
    Logger.info "Called `Storage.Monitor.uuid_helper' on object #{gcp_object.name}"
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
  def handle_cast({:spew_uuids, :source_gcp, session, bucket_name, uuids}, state) do
      {:noreply, {:source_gcp, session, bucket_name, uuids}, state}
  end
end
