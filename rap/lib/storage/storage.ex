defmodule RAP.Storage do
  @moduledoc """

  Original %Prepare module named struct:
      defstruct [ :uuid,
	      :title, :description,
	      :start_time, :end_time,
	      :manifest_signal,
	      :manifest_pre_base_ttl,
	      :manifest_pre_base_yaml,
	      :resource_bases,
	      :result_bases,
	      :results,
	      :staged_tables,
	      :staged_jobs          ]
  """
  use Amnesia

  defdatabase DB do
    deftable Manifest, [
      :uuid,
      :name, :title, :description,
      :start_time, :end_time,
      :signal,
      :manifest_pre_base_ttl,
      :manifest_pre_base_yaml,
      :resource_bases,
      :result_bases,
      :results,
      :staged_tables,
      :staged_jobs      ]
  end
  
end

defmodule RAP.Storage.PreRun do
  @moduledoc """
  This module largely serves to provide utility functions
  """
  require Amnesia
  require Amnesia.Helper
  #require RAP.Storage.DB.Job,      as: JobTable
  require RAP.Storage.DB.Manifest, as: ManifestTable

  require Logger

  defstruct [ :uuid, :index, :resources ]

  @doc """
  Simple wrapper around Erlang term storage table of UUIDs with 
  """
  def ets_feasible?(uuid) do
    case :ets.lookup(:uuid, uuid) do
      [] -> true
      _  ->
	Logger.info("Job UUID #{uuid} is already running, cannot add to ETS.")
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
  def dl_success?(purported_md5, body, opts: _) do
    dl_success?(purported_md5, body, opts: [input_md5: true])
  end
  
  def mnesia_feasible?(uuid) do
    Amnesia.transaction do
      case ManifestTable.read(uuid) do
	nil -> true
	_   ->
	  Logger.info("Found job UUID #{uuid} in job cache!")
	  false
      end
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
  defstruct [ :uuid, :signal, :data_source,
	      :manifest_name, :manifest_yaml, :manifest_ttl, :resources ]
end

defmodule RAP.Storage.PostRun do

  require Amnesia
  require Amnesia.Helper
  require Logger

  #require RAP.Storage.DB.Job,      as: JobTable
  require RAP.Storage.DB.Manifest, as: ManifestTable

  alias RAP.Job.{Result, Runner}
  alias RAP.Bakery.Prepare

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
  def cache_manifest(%Prepare{} = manifest) do
    Logger.info "Cache processed manifest information in mnesia DB `Manifest' table"
    
    with [{uuid, start_ts}] <- :ets.lookup(:uuid, manifest.uuid),
         true               <- :ets.delete(:uuid, manifest.uuid) do
      
      end_ts = DateTime.utc_now() |> DateTime.to_unix()

      annotated_manifest = %Prepare{ manifest | start_time: start_ts, end_time: end_ts }
      Logger.info "Annotated manifest with start/end time: #{inspect annotated_manifest}"

      transformed_manifest = %{ annotated_manifest | __struct__: ManifestTable }
      Logger.info "Transformed annotated manifest into: #{inspect transformed_manifest}"
      
      Amnesia.transaction do
	transformed_manifest |> ManifestTable.write()
      end
      {:ok, annotated_manifest}
    else
      [{_uuid, _start} | [_ | _]] = multiple_uuids ->
	# Note, this should never happen for our usage of ETS as
	Logger.info "Found multiple matching UUIDs in ETS!"
        {:error, "Found multiple matching UUIDs in ETS!", multiple_uuids}
      false ->
	# Note, this should never happen if the lookup succeeded:
	Logger.info "UUID was found in ETS but could not be removed"
        {:error, "UUID not found in ETS"}
    end
  end

end
