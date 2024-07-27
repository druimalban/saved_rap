defmodule RAP.Storage do
  @moduledoc """

  Original %RAP.Bakery.ManifestOutput module named struct:
      defstruct [ :uuid, :data_source,
                  :name, :title, :description,
	          :start_time, :end_time,
	          :manifest_pre_base_ttl,
	          :manifest_pre_base_yaml,
	          :resource_bases,
	          :pre_signal,
	          :producer_signal,
	          :runner_signal,
	          :result_bases,
	          :results,
	          :staged_tables,
	          :staged_jobs          ]
  """
  use Amnesia
  
  defdatabase DB do
    deftable Manifest, [
      :uuid, :data_source,
      :name, :title, :description,
      :start_time, :end_time,
      :manifest_pre_base_ttl,
      :manifest_pre_base_yaml,
      :resource_bases,
      :pre_signal,
      :producer_signal,
      :runner_signal,
      :result_bases,
      :results,
      :staged_tables,
      :staged_jobs
    ]
  end
  
end

defmodule RAP.Storage.PreRun do
  @moduledoc """
  This module largely serves to provide utility functions
  """
  require Amnesia
  require Amnesia.Helper
  require RAP.Storage.DB.Manifest, as: ManifestTable

  require Logger

  defstruct [ :uuid, :index, :resources, :signal, :work ]

  @doc """
  Simple wrapper around Erlang term storage table of UUIDs with 
  """
  def ets_feasible?(uuid, table \\ :uuid) do
    case :ets.lookup(table, uuid) do
      [] -> true
      _  ->
	Logger.info("Job UUID #{uuid} is already running, cannot add to ETS (table #{table}).")
	false
    end
  end

  @doc """
  Simple helper function to compare MD5 checksums given by the storage
  objects API to the actual file downloaded.

  Erlang's `:crypto' works on a binary, not a file path, which is very
  convenient because it avoids writing to disk duff file responses.

  Further note that in `fetch_job_deps/3', sets `:decode' to false, as
  there may be a risk that the decoding breaks this workflow.
  """
  def dl_success?(purported_md5, body, opts: [input_md5: true]) do
    actual_md5 = :crypto.hash(:md5, body) |> Base.encode64()
    purported_md5 == actual_md5
  end
  def dl_success?(body0, body1, opts: [input_md5: false]) do
    body0_md5 = :crypto.hash(:md5, body0) |> Base.encode64()
    body1_md5 = :crypto.hash(:md5, body1) |> Base.encode64()
    body0_md5 == body1_md5
  end  
  def dl_success?(purported_md5, body) do
    dl_success?(purported_md5, body, opts: [input_md5: true])
  end
  
  def mnesia_feasible?(uuid) do
    Amnesia.transaction do
      case ManifestTable.read!(uuid) do
	nil -> true
	_   ->
	  Logger.info("Found job UUID #{uuid} in job cache!")
	  false
      end
    end
  end

  def append_work(past_work, stage_atom, curr_signal, work_started_at, stage_invoked_at, stage_type, stage_subscriptions, stage_dispatcher, work_input \\ [], work_output \\ []) do    
    work_ended_at =  DateTime.utc_now() |> DateTime.to_unix()
    work = [{stage_atom, %{
		stage_pid:           self(),
		stage_invoked_at:    stage_invoked_at,
		stage_type:          stage_type,
		stage_subscriptions: stage_subscriptions,
		stage_dispatcher:    stage_dispatcher,
		signal:              curr_signal,
		work_started_at:     work_started_at,
		work_ended_at:       work_ended_at,
		work_input:          work_input,
		work_output:         work_output
	     }}]
    if is_nil(past_work) do
      work
    else
      past_work ++ work
    end
  end

end

defmodule RAP.Storage.MidRun do
  @moduledoc """
  This is a generic minimal named module to describe a 'package'
  of data files, and where they are sourced from.

  The basic idea is, we have a bunch of modules which can fetch
  data from various places. Once they've actually fetched the data,
  it's stored in the local data cache, and it's got the same components.

  To make feedback richer, we can record also the source (e.g. GCP, some
  local directory we're monitoring, some other object store like S3), so
  it's clear where failures occur.
  """
  defstruct [ :uuid, :signal, :work, :data_source, :manifest_iri, :base_prefix,
	      :manifest_yaml, :manifest_ttl, :resources ]
end

defmodule RAP.Storage.PostRun do

  # Need to require Exquisite to actually use Amnesia.Selection
  require Amnesia
  require Amnesia.Helper
  require Exquisite
  require Logger
  
  require RAP.Storage.DB.Manifest, as: ManifestTable

  alias RAP.Miscellaneous, as: Misc
  alias RAP.Job.{Result, Runner}
  alias RAP.Job.ManifestSpec

  @doc """
  Remove the UUID from ETS and add a manifest row in the Mnesia DB

  This is mostly boilerplate and the error cases should never trigger due
  to the way we set up the :uuid ETS table.

  Unlike jobs, where the time it took for the job to complete is in the
  object, the start and end time-stamps here are simply the difference
  between the initial time stamp cached in ETS and the time stamp here.

  The start and end time-stamps associated with running a job only 
  concern the running of it. Jobs may share data and draw it from
  multiple sources, so calculating the completion time-stamp for a
  given job which includes the caching or subsequent processing is not
  at all meaningful.

  In contrast, the start and end time-stamps associated with a given
  manifest must include these data, since we're interested in seeing how
  long it took end-to-end, compared to specific components.

  A further piece of context is that there is relatively little cost to
  restarting or retrying an individual job, whereas there may be
  significant cost to resumbitting the manifest, since it is often
  associated with a lot of data.

  For now, don't include much information about job successes/failures.
  We do want to keep track of these somehow, and this may be the place,
  just not quite yet.
  """
  def cache_manifest(%ManifestSpec{} = manifest, ets_table \\ :uuid) do
    Logger.info "Cache processed manifest information in mnesia DB `Manifest' table"
    
    with [{uuid, start_ts}] <- :ets.lookup(ets_table, manifest.uuid),
         true               <- :ets.delete(ets_table, manifest.uuid) do
      
      end_ts = DateTime.utc_now() |> DateTime.to_unix()

      annotated_manifest = %ManifestSpec{ manifest | start_time: start_ts, end_time: end_ts }
      Logger.info "Annotated manifest with start/end time: #{inspect annotated_manifest}"

      transformed_manifest = %{ annotated_manifest | __struct__: ManifestTable }
      Logger.info "Transformed annotated manifest into: #{inspect transformed_manifest}"
      Amnesia.transaction do
	transformed_manifest |> ManifestTable.write()
      end
     
      {:ok, annotated_manifest}
    else
      [] ->
	Logger.info "Could not find UUID in ETS!"
	{:error, :ets_no_uuid}
      [{_uuid, _start} | [_ | _]] = multiple_uuids ->
	# Note, this should never happen for our usage of ETS as
	Logger.info "Found multiple matching UUIDs in ETS!"
        {:error, :ets_multiple_uuids, multiple_uuids}
      false ->
	# Note, this should never happen if the lookup succeeded:
	Logger.info "UUID was found in ETS but could not be removed"
        {:error, :ets_no_uuid}
    end
  end

  def retrieve_manifest(uuid) do
    Logger.info "Attempt to retrieve prepared manifest information from mnesia DB `Manifest' table"
    Amnesia.transaction do
      ManifestTable.read(uuid) |> Map.new() |> inject_manifest()
    end
  end

  defp inject_manifest(nil), do: nil
  defp inject_manifest(pre), do: %{ pre | __struct__: ManifestSpec }

  def yield_manifests(invoked_after \\ -1, time_zone, owner \\ :any) do
    date_proper = invoked_after |> Misc.format_time(time_zone)
    Logger.info "Retrieve all prepared manifests after #{date_proper}"
    
    Amnesia.transaction do
      ManifestTable.where(start_time > invoked_after)
      |> Amnesia.Selection.values()
      |> Enum.map(&inject_manifest/1)
    end
  end
  
end
