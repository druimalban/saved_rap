defmodule RAP.Bakery.Prepare do
  @moduledoc """
  The 'bakery' effectively delivers an RDF data-set (manifest + schema +
  maybe the data model proper), annotated with some extras.

  In addition to the input data, then, the files output by the Bakery
  are:

  - Raw results generated by jobs;
  - Post-results processing of input data and/or results
    (e.g. descriptive statistics, visualisations);
  - A file which links the input data and schemata, the RDF manifest,
    results, and post-processing. Effectively a manifest post-results.

  In addition to the post-processing of results, if applicable, it is
  possible that we would want to include any pre-processing performed on
  data files (and potentially include the input data prior to this pre-
  processing). This would need to be built into the pipeline much earlier
  than this stage.
  
  This stage of the 'bakery' just outputs results from each job to the
  correct directory, then moves the files associated with the UUID.
  However, note the naming of the `manifest_pre_base' attribute in the
  module named struct. Subsequent stages will generate a 'post'-running
  manifest which links results &c. (see above) with the extant data
  submitted.

  Calling the cache functions here, rather than at the end of the
  pipeline, makes good sense because it is feasible that we want to be
  able to generate new HTML documentation / descriptive statistics /
  visualisations at arbitrary times, without re-running or re-submitting
  any jobs. Further, we might want to do it for certain jobs, but not
  others.
  """
  use GenStage
  require Logger

  alias RAP.Application
  alias RAP.Miscellaneous, as: Misc
  alias RAP.Storage.{PreRun, PostRun}
  alias RAP.Job.{Result, Runner}

  # Note naming of manifest_pre_base
  # Manifest signal is simple "are all the tables valid"?
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
  
  def start_link(%Application{} = initial_state) do
    GenStage.start_link(__MODULE__, initial_state, name: __MODULE__)
  end

  def init(initial_state) do
    Logger.info "Initialised cache module `RAP.Bakery' with initial_state #{inspect initial_state}"
    subscription = [
      { Runner, min_demand: 0, max_demand: 1 }
    ]
    {:producer_consumer, initial_state, subscribe_to: subscription}
  end

  def handle_events(events, _from, %Application{} = state) do
    Logger.info "Testing storage consumer received #{inspect events}"
    processed_events = events
    |> Enum.map(&bake_data(&1, state.cache_directory, state.bakery_directory, state.linked_result_stem, :uuid))
    {:noreply, processed_events, state}
  end

  #  def handle_demand(demand, state) do
  #    Logger.info "Bakery.Prepare: Received demand for #{inspect demand} events"
  #    yielded   = state.staging_objects |> Enum.take(demand)
  #    remaining = state.staging_objects |> Enum.drop(demand)
  #    pretty    = yielded |> Enum.map(&Misc.pretty_print_object/1)
  #    new_state = state   |> Map.put(:staging_objects, staging)
  #    Logger.info "Yielded #{inspect(length yielded)} objects, with #{inspect(length remaining)} held in state: #{inspect pretty}"
  #    {:noreply, yielded, new_state}
  #  end

  # Very similar to Storage.Monitor :stage_objects cast
  def handle_cast({:trigger_rebuild, after_ts}, %Application{staging_objects: extant} = state) do
    with prepared  <- PostRun.yield_manifests(after_ts, state.time_zone),
         [qh | qt] <- extant ++ prepared do
      new_state = state |> Map.put(:staging_objects, qt)
      {:noreply, [qh], new_state}
    else
      [] -> {:noreply, [], state}
    end    
  end

  @doc """
  Special case for the manifest, rename to something like
  manifest_pre.ttl since we have a notion that we generate
  a post-results manifest which links the results and the data
  presented
  """
  defp move_manifest(fp, cache_dir, bakery_dir, uuid) do
    fp_pre    = fp |> String.replace(~r"\.([a-z]+)$", "_pre.\\1")
    fp_target = cond do
      fp_pre != fp -> fp_pre
      true         -> fp
    end
    fp |> move_wrapper(cache_dir, bakery_dir, uuid, fp_target)
    fp_target
  end
  
  @doc """
  0. Check target UUID directory doesn't already exist like when fetching
  1. If it does exist, check that the files are identical
  2. Output results into the directory and call something related to job
  3. Move data from cache directory/UUID
  4. Remove UUID from ETS
  5. Add results to mnesia DB
  6. ?!

  This function is structured a bit differently to the previous stages,
  i.e. not using the `with' macro.

  In terms of the signal, the %Runner{} signal `:job_errors' records *at
  least one* job error. This is carried over here, and although it is
  meaningful on the level of a single job (i.e. when caching the job bases),
  it's not especially meaningful here.

  Therefore, this stage doesn't record a signal.
  """
  #def bake_data(%Runner{} = processed, cache_dir, bakery_dir, linked_stem, ets_table \\ :) do
  #  Logger.info "Called Prepare.bake_data/5 with ets_table #{ets_table}"
  #  bake_data(processed, cache_dir, bakery_dir, linked_stem, ets_table)
  #end
  def bake_data(%Runner{} = processed, cache_dir, bakery_dir, _linked_stem, ets_table) when processed.signal in [:working, :job_errors] do
    #source_dir = "#{cache_dir}/#{processed.uuid}"
    #target_dir = "#{bakery_dir}/#{processed.uuid}"
    Logger.info "Called Prepare.bake_data/5 with signal `#{processed.signal}'"
    Logger.info "Preparing result of job(s) associated with UUID #{processed.uuid}`"
    Logger.info "Prepare (mkdir(1) -p) #{bakery_dir}/#{processed.uuid}"
    File.mkdir_p("#{bakery_dir}/#{processed.uuid}")

    cached_job_bases = processed.results
    |> Enum.map(&write_result(&1, bakery_dir, processed.uuid))
    
    moved_manifest_ttl = processed.manifest_base_ttl
    |> move_manifest(cache_dir, bakery_dir, processed.uuid)
    moved_manifest_yaml = processed.manifest_base_yaml
    |> move_manifest(cache_dir, bakery_dir, processed.uuid)

    moved_resources = processed.resource_bases
    |> Enum.map(&move_wrapper(&1, cache_dir, bakery_dir, processed.uuid))

    # Start time and end time are calculated when caching, albeit %Prepare{} struct has these fields
    #end_time = DateTime.utc_now() |> DateTime.to_unix()
    semi_final_data = %__MODULE__{
      uuid:                   processed.uuid,
      data_source:            processed.data_source,
      name:                   processed.name,
      title:                  processed.title,
      description:            processed.description,
      pre_signal:             processed.pre_signal,
      producer_signal:        processed.producer_signal,
      runner_signal:          processed.signal,
      manifest_pre_base_ttl:  moved_manifest_ttl,
      manifest_pre_base_yaml: moved_manifest_yaml,
      resource_bases:         moved_resources,
      result_bases:           cached_job_bases,
      results:                processed.results,
      staged_tables:          processed.staging_tables,
      staged_jobs:            processed.staging_jobs
    }
    {:ok, cached_manifest} = PostRun.cache_manifest(semi_final_data, ets_table)
    cached_manifest
  end

  # [:working | :job_errors] | :see_producer | :see_pre
  # :see_producer means that we're only able to move the data package over
  @doc """
  %Runner{ name:               spec.name,
	   uuid:               spec.uuid,
	   pre_signal:         spec.pre_signal,
	   producer_signal:    spec.signal,
	   signal:             :see_producer,
	   manifest_base_ttl:  spec.manifest_base_ttl,
	   manifest_base_yaml: spec.manifest_base_yaml,
	   resource_bases:     spec.resource_bases    }
  """
  def bake_data(%Runner{signal: :see_producer} = processed, cache_dir, bakery_dir, _linked_stem, ets_table) do
    Logger.info "Called Prepare.bake_data/5 with signal `see_producer'"
    File.mkdir_p("#{bakery_dir}/#{processed.uuid}")
    
    moved_manifest_ttl = processed.manifest_base_ttl
    |> move_manifest(cache_dir, bakery_dir, processed.uuid)
    moved_manifest_yaml = processed.manifest_base_yaml
    |> move_manifest(cache_dir, bakery_dir, processed.uuid)

    moved_resources = processed.resource_bases
    |> Enum.map(&move_wrapper(&1, cache_dir, bakery_dir, processed.uuid))

    # Start time and end time are calculated when caching, albeit %Prepare{} struct has these fields
    #end_time = DateTime.utc_now() |> DateTime.to_unix()
    semi_final_data = %__MODULE__{
      uuid:                   processed.uuid,
      data_source:            processed.data_source,
      name:                   processed.name,
      pre_signal:             processed.pre_signal,
      producer_signal:        processed.producer_signal,
      runner_signal:          processed.signal,
      manifest_pre_base_ttl:  moved_manifest_ttl,
      manifest_pre_base_yaml: moved_manifest_yaml,
      resource_bases:         moved_resources
    }
    {:ok, cached_manifest} = PostRun.cache_manifest(semi_final_data, ets_table)
    cached_manifest
  end

  # :see_pre means that we have very little to work with, effectively only UUID + 'runner', 'producer' and 'pre' stage signals (uniformly :see_pre)
  def bake_data(%Runner{signal: :see_pre} = processed, _cache, _bakery, _ln, ets_table) do
    Logger.info "Called Prepare.bake_data/5 with signal `see_pre'"
    File.mkdir_p("#{bakery_dir}/#{processed.uuid}")
    semi_final_data = %__MODULE__{
      uuid:            processed.uuid,
      data_source:     processed.data_source,
      pre_signal:      processed.pre_signal,
      producer_signal: :see_pre,
      runner_signal:   :see_pre      
    }
    {:ok, cached_manifest} = PostRun.cache_manifest(semi_final_data, ets_table)
    cached_manifest
  end

  def bake_data(%Runner{signal: signal}, _cache, _bakery, _ln, _ets) do
    Logger.info "Called Prepare.bake_data/5 with signal #{signal}, not doing anything"
    File.mkdir_p("#{bakery_dir}/#{processed.uuid}")
  end
  
  @doc """
  This is called `write_result' but it should be generalised to any file
  we need to write which isn't already on disc, e.g. RDF descriptions of
  the data output.

  Check that the file does not exist and the file on disk doesn't have a
  matching checksum.

  When logging, the name of the job is in the file name, so no problem
  just printing that to the log.
  """
  def write_result(%Result{type: "density", signal: :working} = result, bakery_directory, uuid) do
    target_base = "#{result.output_stem}_#{result.name}.#{result.output_format}"
    target_full = "#{bakery_directory}/#{uuid}/#{target_base}"
      
    Logger.info "Writing results file #{target_full}"
    
    with false <- File.exists?(target_full) && PreRun.dl_success?(
                    result.contents, File.read!(target_full), opts: [input_md5: false]),
         :ok   <- File.write(target_full, result.contents) do
      
      Logger.info "Wrote result of target #{inspect result.name} to fully-qualified path #{target_full}"
      target_base
    else
      true ->
	Logger.info "File #{target_full} already exists and matches checksum of result to be written"
	result.name
      {:error, error} ->
	Logger.info "Could not write to fully-qualified path #{target_full}: #{inspect error}"
        {:error, error}
    end
  end
  def write_result(%Result{signal: signal} = result, _bakery, _uuid) when signal in [:job_failure, :python_error] do
    Logger.info "Job exited with error: Not writing result to file"
  end
  def write_result(%Result{signal: :ignored}, _bakery, _uuid) do
    Logger.info "Ignored/fake job: Not writing result to file"
  end
  def write_result(%Result{}, _bakery, _uuid) do
    Logger.info "Invalid job: Not writing result to file"
  end

  @doc """
  Wrapper for `File.mv/2' similarly to above
  
  Extra field for overring the target file, useful when renaming manifest
  to have `_pre' before the extension, since we generate a post-results
  linking manifest as well.
  """
  def move_wrapper(orig_fp, source_dir, bakery_dir, uuid) do
    move_wrapper(orig_fp, source_dir, bakery_dir, uuid, orig_fp)
  end
  def move_wrapper(orig_fp, cache_dir, bakery_dir, uuid, target_fp) do
    source_full = "#{cache_dir}/#{uuid}/#{orig_fp}"
    target_full = "#{bakery_dir}/#{uuid}/#{target_fp}"

    with false <- File.exists?(target_full),
	 :ok   <- File.cp(source_full, target_full),
	 :ok   <- File.rm(source_full) do
      Logger.info "Move file #{inspect source_full} to #{inspect target_full}"
      target_fp # Previously `fp' but this is the original file name…
    else
      true ->
	Logger.info "Target file #{inspect target_full} already existed, forcibly removing"
	File.rm(target_full)
        move_wrapper(orig_fp, cache_dir, bakery_dir, uuid, target_fp)
      error ->
	#Logger.info "Couldn't move file #{inspect fp} from cache #{inspect cache_dir} to target directory #{target_dir}"
	Logger.info "Couldn't move file #{inspect target_full} to #{target_full}"
	error
    end
  end

end
