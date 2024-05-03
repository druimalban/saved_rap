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

  import :timer, only: [ sleep: 1 ]

  defstruct [:owner, :uuid, :path,
	     :gcp_name, :gcp_id, :gcp_bucket, :gcp_md5]

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

  def handle_call(:update_session, _from, %RAP.Application{} = state) do
    Logger.info "Received call :update_session"
    new_session = new_connection()
    new_state   = state |> Map.put(:gcp_session, new_session)
    {:reply, new_session, [], new_state}
  end

  def handle_call(:update_last_poll, _from, %RAP.Application{} = state) do
    Logger.info "Received call :update_last_poll"
    new_time_stamp = DateTime.utc_now() |> DateTime.to_unix()
    new_state      = state |> Map.put(:last_poll, new_time_stamp)
    {:reply, new_time_stamp, [], new_state}
  end

  # This variant of the function just duplicates work - both by immediately
  # generating events to send upstream and by appending to the queue. This means
  # that after the next stage has finished consuming the events sent upstream
  # immediately, it asks for more, but it's already used them. This is really a
  # race condition.
  #def handle_cast({:stage_objects, additional}, %RAP.Application{staging_objects: extant} = state) do
  #  Logger.info "Received cast :stage_objects"
  #  new_state = state |> Map.put(:staging_objects, extant ++ additional)
  #  {:noreply, extant ++ additional, new_state}
  #end

  def handle_cast({:stage_objects, additional}, %RAP.Application{staging_objects: extant} = state) do
    Logger.info "Received cast :stage_objects"
    with [queue_head | queue_tail] <- extant ++ additional do    
      new_state = state |> Map.put(:staging_objects, queue_tail)
      {:noreply, [queue_head], new_state}
    else
      [] -> {:noreply, [], state}
    end
  end

  @doc """
  Allows us to retrieve the current session from the producer.

  This is necessary since including the session in each event is annoying
  and does not feel ideal.
  """
  def handle_call(:yield_session, subscriber, %RAP.Application{} = state) do
    Logger.info "Received call to produce return current session, from subscriber #{subscriber}"
    {:reply, state.gcp_session, [], state}
  end

  @doc """
  The idea here is that subsequent parts of the pipeline will ask for
  events in this manner, i.e. there is this notion of back-pressure
  fundamental to the `GenStage' library.
  """
  def handle_demand demand, state do
    Logger.info "Storage.Monitor: Received demand for #{inspect demand} event"
    yielded   = state.staging_objects |> Enum.take(demand)
    remaining = state.staging_objects |> Enum.drop(demand)
    pretty_yielded = yielded |> Enum.map(&pretty_print_object/1)
    new_state = state |> Map.put(:staging_objects, remaining)
    Logger.info "Yielded #{inspect(length yielded)} objects, with #{inspect(length remaining)} held in state: #{inspect pretty_yielded}"
    {:noreply, yielded, new_state}
  end

  @doc """
  Effectual retrieval of object associated with UUID, storing the assumed
  valid UUID in Erlang term storage.
  """
  defp prep_job(uuid, grouped_objects) do
    yielded_files = grouped_objects |> Map.get(uuid)
    find_manifest = fn(k) -> k.path == "manifest.ttl" end

    with %Monitor{} = manifest <- Enum.find(yielded_files, find_manifest),
         resources <- List.delete(yielded_files, manifest) do
      :ets.insert(:uuid, {uuid})
      ds = %RAP.Storage{uuid: uuid, manifest: manifest, resources: resources}
      Logger.info("Inserted #{uuid} into Erlang term storage (:ets)")
      Logger.info("UUID/content map: #{pretty_print_object ds}")
      ds
    else
      nil -> nil
    end
  end

  def pretty_print_object(%Monitor{path: fp}), do: fp
  def pretty_print_object(%RAP.Storage{uuid: uuid, resources: res}) do
    pretty_resources = res |> Enum.map(&pretty_print_object/1)
    "%{UUID: #{uuid}, resources: #{inspect pretty_resources}}"
  end
  def pretty_print_object(n), do: inspect n

  @doc """
  This function monitors the GCP objects every five minutes (or whatever
  interval has been set), updating both global state and calling this
  recursively.

  The GCP storage buckets have a fake directory structure: they're by
  default stored in this flat object-based structure which the web
  interface hides.

  The API has several 'Folder' and 'ManagedFolder' functions but these
  only work on buckets which have a 'hierarchical' namespace, and the
  documentation is so bad that there does not appear to be a way to set
  this on the web interface.
  
  As for the UUIDs proper, there are two elements of this. Firstly, we
  have a notion of UUIDs which have been cached. There is a single row
  per UUID because the UUID has a notion of a job, or collection of jobs.
  Secondly, we have a notion of files, and it is expected that there
  would be many different files associated with an UUID.

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
  def monitor_gcp(session, bucket, interval, last_poll) do
    invocation_ts = DateTime.utc_now() |> DateTime.to_unix()
    elapsed = invocation_ts - last_poll

    with true <- elapsed > interval,
	 {:ok, %GCPObjs{items: objects}} <- wrap_gcp_request(session, bucket) do
      Logger.info "Time elapsed (#{inspect elapsed}s) is greater than interval (#{inspect interval}s)"
      normalised_objects = objects
      |> Enum.map(&uuid_helper/1)
      |> Enum.reject(&is_nil/1)

      remote_uuids = normalised_objects
      |> Enum.map(& &1.uuid)
      |> Enum.uniq
      Logger.info "Found UUIDs on GCP: #{inspect remote_uuids}"

      staging_uuids = remote_uuids
      |> Enum.filter(&Transactions.feasible?/1)
      |> Enum.filter(&ets_feasible?/1)

      grouped_objects = normalised_objects
      |> Enum.group_by(& &1.uuid)
	
      staging_objects = staging_uuids
      |> Enum.map(&prep_job(&1, grouped_objects))
      |> Enum.reject(&is_nil/1)
	
      GenStage.cast(__MODULE__, {:stage_objects, staging_objects})
      new_time_stamp = GenStage.call(__MODULE__, :update_last_poll)
      monitor_gcp(session, bucket, interval, new_time_stamp)
    else
      false ->
	monitor_gcp(session, bucket, interval, last_poll)
      {:error, error = %Tesla.Env{status: 401}} ->
	Logger.info "Query of GCP bucket #{bucket} appeared to time out, seek new session"
        new_session = GenStage.call(__MODULE__, :update_session)
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
  def uuid_helper(%GCPObj{name: nom} = gcp_object) do
    atom_pattern = "[^\\/]+"
    date_pattern = "[0-9]{8}"
    uuid_pattern = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    with {:ok, re} <- Regex.compile("^#{atom_pattern}/#{date_pattern}/#{uuid_pattern}/#{atom_pattern}$"),
	 true      <- Regex.match?(re, nom),
         [owner, _date, uuid, fp] <- String.split(nom, "/") do
      %Monitor{
	owner:      owner,
	uuid:       uuid,
	path:       fp,
	gcp_name:   gcp_object.name,
	gcp_id:     gcp_object.id,
	gcp_bucket: gcp_object.bucket, 
	gcp_md5:    gcp_object.md5Hash }
    else
      _ -> nil
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
