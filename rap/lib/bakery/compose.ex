defmodule RAP.Bakery.Compose do
  @moduledoc """
  Given a struct from the previous stage, either directly passed on, or
  taken from the cache, generate an static HTML representation.
  """
  use GenStage
  require Logger

  import EEx
  
  alias RAP.Application
  alias RAP.Miscellaneous, as: Misc
  alias RAP.Storage.PreRun
  alias RAP.Job.{ScopeSpec, ResourceSpec, TableSpec, JobSpec, ManifestSpec}
  alias RAP.Job.Result
  alias RAP.Bakery.Prepare

  defstruct [ :uuid,          :contents,
	      :output_stem,   :output_format,
	      :runner_signal, :runner_signal_full ]

  def start_link(%Application{} = initial_state) do
    GenStage.start_link(__MODULE__, initial_state, name: __MODULE__)
  end

  def init(initial_state) do
    Logger.info "Initialised cache module `RAP.Bakery.Compose' with initial_state #{inspect initial_state}"
    curr_ts = DateTime.utc_now() |> DateTime.to_unix()
    invocation_state = %{ initial_state | stage_invoked_at: curr_ts }
    subscription = [
      { Prepare, min_demand: 0, max_demand: 1 }
    ]
    {:consumer, invocation_state, subscribe_to: subscription}
  end

  # target_contents, bakery_directory, uuid, stem, extension, target_name
  def handle_events(events, _from, %Application{} = state) do
    Logger.info "HTML document consumer received #{inspect events}"
    input_work = events |> Enum.map(& &1.work)
    Logger.info "Bakery.Compose received objects with the following work defined: #{inspect input_work}"
    #processed_events = events
    #|> Enum.map(&compose_document(state, &1))
    #|> Enum.map(&write_result(&1, state.bakery_directory))
    {:noreply, [], state}
  end

  # Result stem/extension should be configurable and in the ManifestOutput
  # struct, since there's no way to guarantee these are constant across
  # runs, i.e. we could start the program with different parameters,
  # and then past generated HTML pages may break
  #def compose_document(
  #  html_directory,
  #  rap_uri,
  #  style_sheet,
  #  time_zone,
  #  %ManifestOutput{} = prepared 
  #) do
  def compose_document(%Application{} = state, %ManifestSpec{} = prepared) do
    # %ManifestOutput{} is effectively an annotated manifest struct, pass in a map
    {html_contents, manifest_signal} =
      doc_lead_in()
      |> head_lead_in()
      |> preamble(state.html_directory, state.rap_style_sheet, prepared.uuid)
      |> head_lead_out()
      |> body_lead_in()
      |> manifest_info(state.html_directory, state.rap_uri_prefix, state.time_zone, prepared)
      |> tables_info(  state.html_directory, state.rap_uri_prefix, prepared.uuid, prepared.staged_tables)
      |> jobs_info(    state.html_directory, prepared.staged_jobs)
      |> results_info( state.html_directory, state.rap_uri_prefix, state.rap_js_lib_d3, state.rap_js_lib_plotly, state.time_zone, prepared.uuid, prepared.results)
      |> body_lead_out()
      |> doc_lead_out()

    %__MODULE__{
      uuid:                 prepared.uuid,
      contents:             html_contents,
      output_stem:          "index",
      output_format:        "html",
      runner_signal:        prepared.runner_signal,
      runner_signal_full:   manifest_signal
    }
  end
  
  def doc_lead_in, do: {"<!DOCTYPE html>\n<html>\n", nil}
  def head_lead_in( {curr, sig}), do: {curr <> "<head>\n",  sig}
  def head_lead_out({curr, sig}), do: {curr <> "</head>\n", sig}
  def body_lead_in( {curr, sig}), do: {curr <> "<body>\n",  sig}
  def body_lead_out({curr, sig}), do: {curr <> "</body>\n", sig}
  def doc_lead_out( {curr, sig}), do: {curr <> "</html>\n", sig}
  
  def preamble({curr, sig}, html_directory, style_sheet, uuid) do
    preamble_input = [uuid: uuid, style_sheet: style_sheet]
    preamble_fragment = EEx.eval_file("#{html_directory}/preamble.html", preamble_input)
    working_contents = curr <> preamble_fragment
    {working_contents, sig}
  end

  def manifest_info(
    {curr, _sig},
    html_directory,
    rap_uri,
    time_zone,
    %ManifestSpec{} = prepared
  ) do
    ttl_full  = "#{rap_uri}/#{prepared.uuid}/#{prepared.manifest_pre_base_ttl}"
    yaml_full = "#{rap_uri}/#{prepared.uuid}/#{prepared.manifest_pre_base_yaml}"

    signal_full =
      case prepared.runner_signal do
	:working      -> "All stages succeeded."
	:job_errors   -> "Some jobs have failed. See below."
	:see_producer ->
	  producer_signal_full =
	    case prepared.producer_signal do
	      :empty_manifest      -> "Name/IRI of manifest was malformed"
	      :bad_manifest_tables -> "RDF graph was valid, but referenced tables were malformed"
	      :bad_input_graph     -> "RDF graph was malformed and could not be load at all"
	      :working             -> "Passing loaded manifest to job runner stage failed"
	      nil                  -> "Other error loading manifest: unspecified signal"
	      error                -> "Other error loading manifest: #{error}"
	    end
	  "Reading the manifest file failed: #{producer_signal_full}"
	:see_pre ->
	  pre_full =
	    case prepared.pre_signal do
	      :empty_index -> "Index file was empty"
	      :bad_index   -> "Index file was malformed"
	      :working     -> "Passing loaded index to job producer stage failed"
	      nil          -> "Other error loading index: unspecified signal"
	      error        -> "Other error loading index: #{error}"
	    end
	  "Reading the index file failed: #{pre_full}"
	nil   -> "Other error running jobs: unspecified signal"
	error -> "Other error running jobs: #{error}"
      end

    info_extra = %{
      manifest_uri_ttl:    ttl_full,
      manifest_uri_yaml:   yaml_full,
      start_time_readable: Misc.format_time(prepared.start_time, time_zone),
      end_time_readable:   Misc.format_time(prepared.end_time,   time_zone),
      runner_signal_full:  signal_full
    }
    info_input = prepared |> Map.merge(info_extra) |> Map.to_list()

    info_fragment = EEx.eval_file("#{html_directory}/manifest.html", info_input)
    working_contents = curr <> info_fragment
    {working_contents, signal_full}
  end

  # uuid not included in object
  # for the maps, we don't weave in current state of document
  def stage_table(html_directory, rap_uri, uuid,
    %TableSpec{
      resource: %ResourceSpec{base: resource_path},
      schema_ttl:   %ResourceSpec{base: schema_path_ttl}
    } = table_spec) do
    table_extra = %{
      uuid:            uuid,
      resource_path:   resource_path,
      schema_path_ttl: schema_path_ttl,
      resource_uri:    "#{rap_uri}/#{uuid}/#{resource_path}",
      #schema_uri_yaml: "#{rap_uri}/#{uuid}/#{table_spec.schema_path_yaml}",
      schema_uri_ttl:  "#{rap_uri}/#{uuid}/#{schema_path_ttl}"
    }
    table_input = table_spec
    |> Map.merge(table_extra)
    |> Map.to_list()
    
    EEx.eval_file("#{html_directory}/table.html", table_input)
  end

  def tables_info({curr, sig}, _html_dir, _uri, _uuid, nil), do: {curr, sig}
  def tables_info({curr, sig}, html_directory, rap_uri, uuid, tables) do
    tables_lead     = "<h1>Specified tables</h1>\n"
    table_fragments = tables
    |> Enum.map(&stage_table(html_directory, rap_uri, uuid, &1))
    |> Enum.join("\n")
    working_contents = curr <> tables_lead <> table_fragments
    {working_contents, sig}
  end

  def stage_scope(html_directory, %ScopeSpec{} = scope_spec) do
    EEx.eval_file(
      "#{html_directory}/scope.html",
      Map.to_list(scope_spec)
    )
  end
  
  def stage_scope_list(_dir, nil, scope_type), do: nil
  def stage_scope_list(html_directory, scope_triples, scope_type) do
    scope_lead      = EEx.eval_string("<li>‘<%= type %>’ columns in scope:\n<ul>", type: scope_type)
    scope_fragments = scope_triples
    |> Enum.map(&stage_scope(html_directory, &1))
    |> Enum.join("\n")
    scope_lead <> scope_fragments <> "</ul>\n"
  end

  def stage_job(html_directory, %JobSpec{} = job_spec) do
    descriptive = stage_scope_list(html_directory, job_spec.scope_descriptive, "Descriptive")
    collected   = stage_scope_list(html_directory, job_spec.scope_collected,   "Collected")
    modelled    = stage_scope_list(html_directory, job_spec.scope_modelled,    "Modelled")
    
    job_input = [
      name:  job_spec.name,
      title: job_spec.title,
      type:  job_spec.type,
      description:       job_spec.description,
      scope_descriptive: descriptive,
      scope_collected:   collected,
      scope_modelled:    modelled,
    ]
    EEx.eval_file("#{html_directory}/job.html", job_input)
  end

  def jobs_info({curr, sig}, _html, nil), do: {curr, sig}
  def jobs_info({curr, sig}, html_directory, jobs) do
    jobs_lead = "<h1>Specified jobs</h1>\n"
    job_fragments = jobs
    |> Enum.map(&stage_job(html_directory, &1))
    |> Enum.join("\n")
    curr <> jobs_lead <> job_fragments
    {curr, sig}
  end

  def plot_result(
    html_directory,
    lib_d3,
    lib_plotly,
    target_uri,
    %Result{job_type: "density", signal: :working} = result) do
    
    plot_extra = %{
      contents_uri:   target_uri,
      lib_d3:         lib_d3,
      lib_plotly:     lib_plotly,
      plot_div_style: "width:600px;height:400px;"
    }
    plot_input = result
    |> Map.merge(plot_extra)
    |> Map.to_list()
    EEx.eval_file("#{html_directory}/plot_density.html", plot_input)
  end
  def plot_result(_fragments, _uri, _d3, _plotly, _res), do: ""
  
  # Assumption is that we have a notion of a completed job, (see named
  # RAP.Job.Result struct), annotated with the base name of the output file
  # As opposed to the final RAP.Bakery.ManifestOutput struct which chucks away a
  # bunch of information.
  # Call the base name of the output file contents_base since result text
  # contents are called `contents'  
  def stage_result(html_directory, rap_uri, lib_d3, lib_plotly, time_zone, uuid, %Result{} = result) do
    target_base = "#{result.output_stem}_#{result.name}.#{result.output_format}"
    target_uri  = "#{rap_uri}/#{uuid}/#{target_base}"
    plotted     = plot_result(html_directory, lib_d3, lib_plotly, target_uri, result)

    result_extra = %{
      start_time_readable: Misc.format_time(result.start_time, time_zone),
      end_time_readable:   Misc.format_time(result.end_time,   time_zone),
      contents_base:       target_base,
      contents_uri:        target_uri
    }
    result_input = result
    |> Map.merge(result_extra)
    |> Map.to_list()

    result_main =
      case result.signal do
	:bad_job_spec -> EEx.eval_file("#{html_directory}/nonresult.html", result_input)
	:ignored      -> EEx.eval_file("#{html_directory}/nonresult.html", result_input)
	_             -> EEx.eval_file("#{html_directory}/result.html", result_input)
      end
    result_main <> "\n" <> plotted
  end

  def results_info({curr, sig}, _html, _uri, _d3, _plotly, _tz, _uuid, nil), do: {curr, sig}
  def results_info({curr, sig}, html_directory, rap_uri, lib_d3, lib_plotly, time_zone, uuid, results) do
    results_lead = "<h1>Results</h1>\n"
    results_fragments = results
    |> Enum.map(&stage_result(html_directory, rap_uri, lib_d3, lib_plotly, time_zone, uuid, &1))
    |> Enum.join("\n")
    working_contents = curr <> results_lead <> results_fragments
    {working_contents, sig}
  end

  @doc """
  This is more or less identical to `ManifestOutput.write_result/4'…
  """
  def write_result(%__MODULE__{} = result, bakery_directory) do
    target_base = "#{result.output_stem}.#{result.output_format}"
    target_full = "#{bakery_directory}/#{result.uuid}/#{target_base}"
      
    Logger.info "Writing index file #{target_full}"
    
    with false <- File.exists?(target_full) && PreRun.dl_success?(
                    result.contents, File.read!(target_full), opts: [input_md5: false]),
         :ok   <- File.write(target_full, result.contents) do
      
      Logger.info "Wrote result to fully-qualified path #{target_full}"
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
end
