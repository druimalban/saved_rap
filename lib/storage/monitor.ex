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
  authentication tokens / cookies. This is handled primarily by this
  producer module and communicating with it is via GenServer
  callbacks.
  """
  use GenStage
  require Logger

  alias GoogleApi.Storage.V1.Connection,    as: GCPConn
  alias GoogleApi.Storage.V1.Model.Object,  as: GCPObj
  alias GoogleApi.Storage.V1.Model.Objects, as: GCPObjs
  alias GoogleApi.Storage.V1.Api.Objects,   as: GCPReqObjs

  alias RAP.Miscellaneous, as: Misc

  alias RAP.Storage.PreRun
  alias RAP.Provenance.Work
  
  defstruct [:owner, :uuid, :path,
	     :gcp_name, :gcp_id, :gcp_bucket, :gcp_md5]

  @type stage_dispatcher   :: GenStage.BroadcastDispatcher | GenStage.DemandDispatcher | GenStage.PartitionDispatcher
  @type stage_type         :: :consumer | :producer_consumer | :producer
  @type stage_subscription :: {atom(), min_demand: integer(), max_demand: integer()}
  @type stage_state :: %{  
    gcp_session:         Tesla.Client.t(),
    staging_objects:     [GCPObj.t()],
    stage_dispatcher:    stage_dispatcher(),
    stage_invoked_at:    integer(),
    stage_subscriptions: [stage_subscription()],
    stage_type:          atom()
  }

  # There are no circumstances in which this ought to be configurable.
  # It's very much tied to API usage, and it's only necessary for
  # acquiring an OAuth token.
  @gcp_scope "https://www.googleapis.com/auth/cloud-platform"
  
  def start_link([] = initial_state) do
    Logger.info "Start link to Storage.Monitor"
    GenStage.start_link __MODULE__, initial_state, name: __MODULE__
  end

  @doc """
  Note that there is a single cache of UUIDs in memory, just like there
  is a single cache of data and a single database of results.

  The idea is that any given store uses randomly generated UUIDs, which
  are de-facto unique, so there is no chance of conflict here.
  """
  def init([] = _initial_state) do
    Logger.info "Initialise Storage.Monitor"
    
    with {:ok, gcp_bucket}       <- Application.fetch_env(:rap, :gcp_bucket),
         {:ok, index_file}       <- Application.fetch_env(:rap, :index_file),
	 {:ok, interval_seconds} <- Application.fetch_env(:rap, :interval_seconds),
	 {:ok, ets_table}        <- Application.fetch_env(:rap, :ets_table),
         ^ets_table              <- :ets.new(ets_table, [:set, :public, :named_table]),
	 {:ok, initial_session}  <- new_connection() do
      curr_ts = DateTime.utc_now() |> DateTime.to_unix()

      stage_state = %{
	stage_invoked_at:    curr_ts,
	stage_type:          :producer,
	stage_subscriptions: [],
	stage_dispatcher:    GenStage.DemandDispatcher,
	gcp_session:         initial_session,
	staging_objects:     []
      }
      Task.start_link(fn() ->
	monitor_gcp(
	  stage_state.gcp_session,
	  gcp_bucket,
	  index_file,
	  interval_seconds * 1000,
	  stage_state
	)
      end)
      {stage_state.stage_type, stage_state, dispatcher: stage_state.stage_dispatcher }
    else
      {:error, _msg} = error -> error
      :error -> { :error, "Could not fetch keywords from RAP configuration" }
    end
  end

  # Update session state from our recursive `monitor_gcp/4' function
  # {:ok, Tesla.Client.t()} | {:error, String.t()}
  def handle_call(:update_session, _from, state) do
    Logger.info "Received call :update_session"
    with {:ok, %Tesla.Client{} = new_session} = signal <- new_connection(),
	 new_state <- Map.put(state, :gcp_session, new_session) do
      {:reply, new_session, [], new_state}
    else
      {:error, error} ->
	Logger.info "Error updating session: #{error}"
        {:reply, nil, [], state}
    end
  end

  # Allows us to retrieve the current session from the producer
  def handle_call(:yield_session, subscriber, state) do
    Logger.info "Received call to produce return current session, from subscriber #{inspect subscriber}"
    {:reply, state.gcp_session, [], state}
  end

  @doc """
  A previous variant of the function was fatally flawed as it duplicated
  work both by immediately generating events to send upstream and by
  appending to the queue. This means that after the next stage has
  finished consuming the events sent upstream immediately, it asks for
  more, but it's already used them. This is really bad because the
  events we're interested in have a cost for retrieval, both
  computationally and in terms of the subscription to the GCP storage
  platform.
  """
  def handle_cast({:stage_objects, additional}, %{staging_objects: extant} = state) do
    Logger.info "Received cast :stage_objects for objects #{inspect additional}"
    with [queue_head | queue_tail] <- extant ++ additional do
      new_state = state |> Map.put(:staging_objects, queue_tail)
      {:noreply, [queue_head], new_state}
    else
      [] -> {:noreply, [], state}
    end
  end

  @doc """
  The idea here is that subsequent parts of the pipeline will ask for
  events in this manner, i.e. there is this notion of back-pressure
  fundamental to the `GenStage' library.
  """
  def handle_demand(demand, %{} = state) do
    Logger.info "Storage.Monitor: Received demand for #{inspect demand} event"
    yielded   = state.staging_objects |> Enum.take(demand)
    remaining = state.staging_objects |> Enum.drop(demand)
    pretty    = yielded |> Enum.map(&Misc.pretty_print_object/1)
    new_state = state |> Map.put(:staging_objects, remaining)
    Logger.info "Yielded #{inspect(length yielded)} objects, with #{inspect(length remaining)} held in state: #{inspect pretty}"
    {:noreply, yielded, new_state}
  end

  @doc """
  Effectual retrieval of object associated with UUID, storing the assumed
  valid UUID in Erlang term storage.
  """
  @type grouped_map :: %{required(String.t()) => [String.t(), ...]}
  @spec prep_job(String.t(), grouped_map(), String.t()) :: {:ok, %PreRun{}} | {:error, String.t()}
  def prep_job(uuid, grouped_objects, index_file) do
    target_files = grouped_objects |> Map.get(uuid)
    find_index   = fn(k) -> k.path == index_file end
    curr_ts      = DateTime.utc_now() |> DateTime.to_unix()

    with {:ok, ets_table}      <- Application.fetch_env(:rap, :ets_table),
	 %__MODULE__{} = index <- Enum.find(target_files, find_index),
         resources             <- List.delete(target_files, index),
	 true                  <- :ets.insert(ets_table, {uuid, curr_ts}) do
      
      Logger.info("Inserted #{uuid} and current UNIX time-stamp #{inspect curr_ts} into ETS table #{ets_table}")
      ds = %PreRun{uuid: uuid, index: index, resources: resources}

      Logger.info("UUID/content map: #{Misc.pretty_print_object ds}")
      {:ok, ds}
    else
      :error ->
	msg = "Could not fetch keywords from RAP configuration"
        Logger.info msg
	{:error, msg}
      false  ->
	msg = "Could not insert UUID #{uuid} and current UNIX time-stamp #{inspect curr_ts} into ETS"
        Logger.info msg
	{:error, msg}
      nil    ->
	msg = "Job (UUID #{uuid}) not associated with index (file `#{index_file}')"
        Logger.info msg
	{:error, msg}
    end
  end

  @doc """
  This function monitors the GCP objects every five minutes (or whatever
  interval has been set), updating both global te and calling this
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
  using `Storage.PreRun.feasible/1' and `Storage.PreRun.ets_feasible/1',
  which produce a set of UUIDs of manifest descriptions which have
  neither finished being processed (cached) nor currently running
  (UUID + start time-stamp is registered in-memory in Erlang term
  storage ~ `:ets').

  We further group all objects provided by UUID. For each UUID which made
  it out of the filtering, use this as the key to lookup the collection
  of files associated with the UUID.

  There are certain well-founded assumptions which are governed by the
  functionality of `fisdat' and/or `fisup'. Firstly, there is at most one
  manifest per UUID, because that's the single file we fed into `fisup'
  in order to upload the files (and a unique UUID is created per
  invocation of `fisup'). The original idea was to have the manifest file
  hard-coded, and for `fisup' program to normalise prior to upload, which
  is something which it does for YAML LinkML schema vs equivalent turtle.
  This is not necessary if there is a hard-coded index file called
  `.index' which simply has the file-name of the manifest file. Adding
  other files into this index file is unneccesary because the manifest
  describes these.
  """
  @spec monitor_gcp(Tesla.Client.t(), String.t(), String.t(), integer(), stage_state()) :: nil | {:error, String.t(), integer(), String.t()}
  def monitor_gcp(%Tesla.Client{} = session, bucket, index_file, interval_ms, %{} = stage_state) do
    with {:ok, objects} <- wrap_gcp_request(session, bucket) do
      Logger.info "Called RAP.Storage.Monitor.monitor_gcp/4 with interval #{interval_ms} milliseconds"
      work_started_at = DateTime.utc_now() |> DateTime.to_unix()
      
      normalised_objects = objects
      |> Enum.map(&uuid_helper/1)
      |> Enum.reject(&is_nil/1)

      remote_uuids = normalised_objects
      |> Enum.map(& &1.uuid)
      |> Enum.uniq
      Logger.info "Found UUIDs on GCP: #{inspect remote_uuids}"

      staging_uuids = remote_uuids
      |> Enum.filter(&PreRun.mnesia_feasible?/1)
      |> Enum.filter(&PreRun.ets_feasible?/1)

      grouped_objects = normalised_objects
      |> Enum.group_by(& &1.uuid)

      annotate = fn({:ok, ds}) ->
	initial_work = Work.append_work([], __MODULE__, :working, work_started_at, stage_state)
	%{ds | work: initial_work}
      end
      
      staging_objects = staging_uuids
      |> Enum.map(&prep_job(&1, grouped_objects, index_file))
      |> Enum.reject(&match?({:error, _msg}, &1))
      |> Enum.map(annotate)

      Logger.info "RAP.Storage.Monitor.monitor_gcp/4: casting staged objects and sleeping for #{interval_ms} milliseconds"
      GenStage.cast(__MODULE__, {:stage_objects, staging_objects})
      :timer.sleep(interval_ms)
      monitor_gcp(session, bucket, index_file, interval_ms, stage_state)
    else
      {:error, %Tesla.Env{status: 401}} ->
	Logger.info "Query of GCP bucket #{bucket} appeared to time out, seek new session"
        :timer.sleep(interval_ms)
        new_session = GenStage.call(__MODULE__, :update_session)
	Logger.info "Call to seek new session returned #{inspect new_session}"
	monitor_gcp(new_session, bucket, index_file, interval_ms, stage_state)
      {:error, %Tesla.Env{status: code, url: uri, body: msg}} ->
	Logger.info "Query of GCP failed with code #{code} and error message #{msg}"
        {:error, uri, code, msg}
      {:error, :econnrefused} ->
	Logger.info "Query of GCP failed with possible SSL handshake issue?"
        :timer.sleep(interval_ms)
        new_session = GenStage.call(__MODULE__, :update_session)
        monitor_gcp(new_session, bucket, index_file, interval_ms, stage_state)
      other_error ->
	Logger.info "Query of GCP failed with error #{inspect other_error}"
	other_error
    end
  end
  
  # The {:error, any()} type specification derives from GoogleApi.Storage.V1.Objects.storage_objects_list/4.
  @spec wrap_gcp_request(Tesla.Client.t(), String.t()) :: {:ok, [%GCPObj{}]} | {:error, any()}
  defp wrap_gcp_request(%Tesla.Client{} = session, bucket) do
    Logger.info "Polling GCP storage bucket #{bucket} for flat objects"
    case GCPReqObjs.storage_objects_list(session, bucket) do
      {:ok, %GCPObjs{items: nil}}   -> {:ok, []}
      {:ok, %GCPObjs{items: items}} -> {:ok, items}
      error                         -> error
    end
  end

  @spec new_connection() :: {:ok, Tesla.Client.t()} | {:error, String.t()}
  defp new_connection() do
    with {:ok, token}              <- Goth.Token.for_scope(@gcp_scope),
         %Tesla.Client{} = session <- GCPConn.new(token.token) do
      Logger.info "Called Storage.Monitor.new_connection/0"
      {:ok, session}
    else
      :error -> {:error, "Cannot obtain token/session" }
    end
  end

  @doc """
  Given an object returned by GCP, the files of which are uploaded in the
  following format, extract the useful parts from this well-known
  object name. Separate the regex into constituent patterns so that they
  are easier to change separately, and actually compile the thing so that
  any breaking changes break the function.

  The flat objects' names should be a string in the form
  `<owner>/<yyyymmdd>/<uuid>/<file>', with or without the trailing slash.
  The trailing slash conveniently tells us that it's a directory; objects
  have `text/plain' for directories, `application/octet_stream' for empty
  files.
  """
  @spec uuid_helper(%GCPObj{}) :: %__MODULE__{} | nil
  def uuid_helper(%GCPObj{name: nil}), do: nil
  def uuid_helper(%GCPObj{name: nom} = gcp_object) do
    atom_pattern = "[^\\/]+"
    date_pattern = "[0-9]{8}"
    uuid_pattern = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    full_pattern = "^#{atom_pattern}/#{date_pattern}/#{uuid_pattern}/#{atom_pattern}$"
    with {:ok, re} <- Regex.compile(full_pattern),
	 true      <- Regex.match?(re, nom),
         [owner, _date, uuid, fp] <- String.split(nom, "/") do
      %__MODULE__{
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

end
