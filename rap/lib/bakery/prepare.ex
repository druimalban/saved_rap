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

  In addition to the post-processing oF results, if applicable, it is
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

  alias RAP.Miscellaneous, as: Misc
  alias RAP.Storage.{PreRun, PostRun}
  alias RAP.Job.ManifestSpec
  alias RAP.Job.{Producer, Runner, Result}
  alias RAP.Provenance.Work
  
  def start_link(initial_state) do
    Logger.info "Start link to Bakery.Prepare"
    GenStage.start_link(__MODULE__, initial_state, name: __MODULE__)
  end

  def init([rap_invoked_at: rap_invoked_at] = initial_state) do
    Logger.info "Initialise Bakery.Prepare"
    curr_ts = DateTime.utc_now() |> DateTime.to_unix()
    subscription = [{ Runner, min_demand: 0, max_demand: 1 }]
    invocation_state = %{ rap_invoked_at:      rap_invoked_at,
			  stage_invoked_at:    curr_ts,
			  stage_type:          :producer_consumer,
			  stage_subscriptions: subscription,
			  stage_dispatcher:    GenStage.DemandDispatcher }

    { invocation_state.stage_type, invocation_state,
      subscribe_to: invocation_state.stage_subscriptions,
      dispatcher:   invocation_state.stage_dispatcher }
  end

  def handle_events(events, _from, stage_state) do
    known_prefixes = %{
      rap:     RAP.Vocabulary.RAP,
      saved:   RAP.Vocabulary.SAVED,
      rdfs:    RDF.NS.RDFS,
      dcat:    RAP.Vocabulary.DCAT,
      dcterms: RAP.Vocabulary.DCTERMS,
      prov:    RAP.Vocabulary.PROV,
      pav:     RAP.Vocabulary.PAV
    }
    Logger.info "Testing storage consumer received #{inspect events}"
    input_work = events |> Enum.map(& &1.work)
    Logger.info "Bakery.Prepare received objects with the following work defined: #{inspect input_work}"
    
    processed_events = events
    |> Enum.map(&bake_data(&1, stage_state))
    |> Enum.map(&write_turtle(&1, known_prefixes))
    |> Enum.map(&PostRun.cache_manifest/1)
    {:noreply, processed_events, stage_state}
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
  def handle_cast({:trigger_rebuild, after_ts}, %{} = stage_state) do
    with {:ok, extant}    <- Map.fetch(stage_state, :staging_objects),
         {:ok, time_zone} <- Application.fetch(:rap, :time_zone),
	 prepared  <- PostRun.yield_manifests(after_ts, time_zone),
         [qh | qt] <- extant ++ prepared
      do
      new_state = stage_state |> Map.put(:staging_objects, qt)
      {:noreply, [qh], new_state}
    else
      [] -> {:noreply, [], stage_state}
    end
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
  def bake_data(%ManifestSpec{signal: sig} = processed, %{} = state) when sig in [:working, :job_errors, :see_producer] do
    Logger.info "Called Prepare.bake_data/5 with signal `#{processed.signal}'"
    Logger.info "Preparing result of job(s) associated with UUID #{processed.uuid}`"
    work_started_at = DateTime.utc_now() |> DateTime.to_unix()
    
    with {:ok, rap_invoked_at} <- Map.fetch(state, :rap_invoked_at),
	 {:ok, cache_dir}  <- Application.fetch_env(:rap, :cache_directory),
	 {:ok, bakery_dir} <- Application.fetch_env(:rap, :bakery_directory),
	 :ok <- File.mkdir_p("#{bakery_dir}/#{processed.uuid}") do

      # default value is []
      cached_results = processed.results
      |> Enum.map(&write_result(&1, processed.base_prefix, bakery_dir, processed.uuid))
    
      moved_manifest_ttl = processed.submitted_manifest_base_ttl
      |> move_wrapper(cache_dir, bakery_dir, processed.uuid)
      moved_manifest_yaml = processed.submitted_manifest_base_yaml
      |> move_wrapper(cache_dir, bakery_dir, processed.uuid)
      
      moved_resources = processed.resource_bases
      |> Enum.map(&move_wrapper(&1, cache_dir, bakery_dir, processed.uuid))

      #resource_iris = processed.resources |> Enum.map(& &1.__id__)
      result_iris   = processed.results   |> Enum.map(& &1.__id__)
      pipeline_generated_iris = [processed.__id__ | result_iris]
    
      new_work = processed.work
      |> Work.append_work(
           __MODULE__, sig, work_started_at,
           state,
           [], [processed.__id__])
      |> Work.traverse_work(
           processed.base_prefix,
           rap_invoked_at, pipeline_generated_iris)

      %{ processed |
	 results:         cached_results,
	 resource_bases:  moved_resources,
	 rap_app:         new_work.app_agent,
	 rap_app_init:    new_work.app_invocation,
	 rap_stages:      new_work.stages,
	 rap_stages_init: new_work.invocations,
	 rap_processing:  new_work.processing
      }
    else
      :error -> {:error, "Cannot fetch keywords or make new directory"}
    end
  end

  def bake_data(%ManifestSpec{signal: :see_pre} = processed, %{} = state) do

    Logger.info "Called Prepare.bake_data/5 with signal `see_pre', not doing anything"
    work_started_at = DateTime.utc_now() |> DateTime.to_unix()

    with {:ok, rap_invoked_at} <- Map.fetch(state, :rap_invoked_at),
	 {:ok, bakery_dir} <- Application.fetch_env(:rap, :bakery_directory),
	 :ok <- File.mkdir_p("#{bakery_dir}/#{processed.uuid}") do
        
       new_work = processed.work
       |> Work.append_work(
            __MODULE__, :see_pre, work_started_at,
            state,
            [], [processed.__id__])
       |> Work.traverse_work(
            processed.base_prefix,
            rap_invoked_at, [processed.__id__])
    
       %{ processed |
	  rap_app:         new_work.app_agent,
	  rap_app_init:    new_work.app_invocation,
	  rap_stages:      new_work.stages,
	  rap_stages_init: new_work.invocations,
	  rap_processing:  new_work.processing
       }
     end
  end

  def write_turtle(%ManifestSpec{} = processed, known_prefixes) do

    {:ok, linked_stem} = Application.fetch_env(:rap, :linked_result_stem)
    {:ok, bakery_dir}  = Application.fetch_env(:rap, :bakery_directory)
    
    all_prefixes = known_prefixes
    |> Map.put_new(:submission, RDF.IRI.new(processed.base_prefix))

    fp_processed =
	case processed.submitted_manifest_base_ttl do
	  nil -> "manifest_#{processed.uuid}.#{linked_stem}.ttl"
	  nom -> String.replace(nom, ~r"\.([a-z]+)$", ".#{linked_stem}.\\1")
	end
    fp_full = "#{bakery_dir}/#{processed.uuid}/#{fp_processed}"

    dl_url    = RDF.IRI.new(processed.base_prefix <> fp_processed)
    annotated = %{ processed |
		   processed_manifest_base: fp_processed,
		   download_url:            dl_url,
		   output_format:           "text/turtle" }
    
    with :ok              <- File.mkdir_p("#{bakery_dir}/#{processed.uuid}"),
	 {:ok, rdf_equiv} <- Grax.to_rdf(annotated),
         rdf_annotated    <- RDF.Graph.add_prefixes(rdf_equiv, all_prefixes),
         :ok              <- RDF.Turtle.write_file(rdf_annotated, fp_full, force: true) do
      Logger.info "Successfully wrote turtle manifest to #{fp_full}"
      annotated
    else
      {:error, %Grax.ValidationError{} = err} ->
	Logger.info(inspect err)
        {:error, %Grax.ValidationError{} = err}
      other_error ->
	other_error
    end
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
  def write_result(%Result{signal: :working} = result, base_prefix, bakery_dir, uuid) do
    
    target_ext  = result.output_format |> String.split("/") |> Enum.at(1) # move me
    target_name = Producer.extract_id(result.__id__)
    target_base = "#{result.output_stem}_#{target_name}.#{target_ext}"
    target_full = "#{bakery_dir}/#{uuid}/#{target_base}"
      
    Logger.info "Writing results file #{target_full}"
    
    with false <- File.exists?(target_full) && PreRun.dl_success?(
                    result.contents, File.read!(target_full), opts: [input_md5: false]),
         :ok   <- File.write(target_full, result.contents) do
      
      Logger.info "Wrote result of target #{inspect result.__id__} to fully-qualified path #{target_full}"

      %{ result | download_url: result.__id__, signal: :working, text_signal: "working" }
    else
      true ->
	Logger.info "File #{target_full} already exists and matches checksum of result to be written"
        %{ result | download_url: result.__id__, signal: :working, text_signal: "working" }
      {:error, error} ->
	Logger.info "Could not write to fully-qualified path #{target_full}: #{inspect error}"
        {:error, error}
    end
  end
  def write_result(%Result{signal: signal} = result, base_prefix, _bakery, _uuid) when signal in [:job_failure, :python_error] do
    Logger.info "Job exited with error: Not writing result to file"
    %{ result | text_signal: to_string(signal) }
  end
  def write_result(%Result{signal: :ignored} = result, base_prefix, _bakery, _uuid) do
    Logger.info "Attempting to write #{inspect result}, with 'base' prefix #{base_prefix}"
    Logger.info "Ignored/fake job: Not writing result to file"
    %{ result | text_signal: "ignored" }
  end
  def write_result(%Result{signal: signal} = result, base_prefix, _bakery, _uuid) do
    Logger.info "Attempting to write #{inspect result}, with 'base' prefix #{base_prefix}"
    Logger.info "Invalid job: Not writing result to file"
    %{ result | text_signal: to_string(signal) }
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
