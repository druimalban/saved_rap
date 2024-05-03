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
    deftable Job, [ :uuid, :manifest, :resources, :results ], type: :bag
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
    with {:ok, initial_session} <- new_connection()
      do
      state = initial_state |> Map.put(:gcp_session, initial_session)
      Task.start(fn() ->
	monitor_gcp(
	  state.gcp_session,
	  state.gcp_bucket,
	  state.interval_seconds,
	  state.last_poll
	)
      end)
      {:producer, state}
    else
      {:error, _msg} = error -> error
    end
  end

  def handle_cast({:update_session, new_session}, %RAP.Application{} = state) do
    Logger.info "Received cast :update_session"
    new_state = state |> Map.put(:gcp_session, new_session)
    {:noreply, [], new_state}
  end
  # Take arbitrary time-stamp to avoid code repetition and race conditions
  def handle_cast({:update_last_poll, new_time_stamp}, %RAP.Application{} = state) do
    Logger.info "Received cast :update_last_poll"
    new_state = state |> Map.put(:last_poll, new_time_stamp)
    {:noreply, [], new_state}
  end
  def handle_cast({:stage_uuids, additional}, %RAP.Application{staging_uuids: extant} = state) do
    Logger.info "Received cast :stage_uuids"
    new_uuid_queue = extant ++ additional
    new_state = state |> Map.put(:staging_uuids, new_uuid_queue)
    {:noreply, [], new_state}
  end

  @doc "Boilerplate handle_demand/2 for now"
  def handle_demand demand, state do
    Logger.info "Storage.Monitor: Received demand for #{inspect demand} event"
    yielded_uuids   = state.staging_uuids |> Enum.take(demand)
    remaining_uuids = state.staging_uuids |> Enum.drop(demand)
    new_state       = state |> Map.put(:staging_uuids, remaining_uuids)
    Logger.info "Yielded #{inspect(length yielded_uuids)} UUIDs, with #{inspect(length remaining_uuids)} held in state"
    {:noreply, yielded_uuids, new_state}
  end
  
  @doc """
  Monitors the GCP objects every five minutes, updating both global
  state and calling this recursively.

  The Google GCP buckets have a fake directory structure: they're by
  default stored in this flat object-based structure which the web
  interface hides.

  The API has several 'Folder' and 'ManagedFolder' functions but these
  only work on buckets which have a 'hierarchical' namespace, and the
  documentation is so bad that there does not appear to be a way to set
  this on the web interface.
  """
  def monitor_gcp(session, bucket, interval, last_poll) do
    invocation_ts = DateTime.utc_now() |> DateTime.to_unix()
    elapsed = invocation_ts - last_poll

    with true <- elapsed > interval,
	 {:ok, %GCPObjs{items: objects}} <- wrap_gcp_request(session, bucket)
	do
          Logger.info "Time elapsed (#{inspect elapsed}s) is greater than interval (#{inspect interval}s)"

	  uuids = objects
	  |> Enum.map(&features_helper/1)
	  |> Enum.map(&uuid_helper/1)
	  
	  Logger.info "Found UUIDs: #{inspect uuids}"
	  GenStage.cast(__MODULE__, {:stage_uuids, uuids})
	  
	  # Do check this again because it's possible (if unlikely) that 
	  new_ts    = DateTime.utc_now() |> DateTime.to_unix()
	  Logger.info "Update last poll UNIX time stamp to #{inspect new_ts}"
	  GenStage.cast(__MODULE__, {:update_last_poll, new_ts})
	  
	  monitor_gcp(session, bucket, interval, new_ts)
	else
	  false ->
	    # Don't log here as this isn't an error
	    monitor_gcp(session, bucket, interval, last_poll)
	  {:error, error = %Tesla.Env{status: 401}} ->
	    Logger.info "Query of GCP bucket #{bucket} appeared to time out, seek new session"
            new_session = new_connection()
	    GenStage.cast(__MODULE__, {:update_last_poll, new_session})
	    monitor_gcp(new_session, bucket, interval, last_poll)
	  {:error, error = %Tesla.Env{status: code, url: uri, body: msg}} ->
	    Logger.info "Query of GCP failed with code #{code} and error message #{msg}"
            {:error, uri, code, msg}
    end
  end
  

  @doc """
  Added this for testing with statements, but it was still productive to
  keep it around as informative messages in the log are good.
  """
  defp wrap_gcp_request(session, bucket) do
    Logger.info "Polling GCP storage bucket #{bucket} for flat objects"
    GCPReqObjs.storage_objects_list(session, bucket)
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
    #Logger.info "Called `Storage.Monitor.features_helper' on object #{obj.name}"
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
    #Logger.info "Called `Storage.Monitor.uuid_helper' on object #{gcp_object.name}"
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

end
