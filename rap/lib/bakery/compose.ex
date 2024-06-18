defmodule RAP.Bakery.Compose do
  @moduledoc """
  Given a struct from the previous stage, either directly passed on, or
  taken from the cache, generate an static HTML representation.
  """
  use GenStage
  require Logger

  import EEx
  
  alias RAP.Application
  alias RAP.Bakery.Prepare
  alias RAP.Job.{ScopeSpec, ResourceSpec, TableSpec, JobSpec, ManifestSpec}
  alias RAP.Job.Result

  def start_link(%Application{} = initial_state) do
    GenStage.start_link(__MODULE__, initial_state, name: __MODULE__)
  end

  def init(initial_state) do
    Logger.info "Initialised cache module `RAP.Bakery.Compose' with initial_state #{inspect initial_state}"
    subscription = [
      { Prepare, min_demand: 0, max_demand: 1 }
    ]
    {:consumer, initial_state, subscribe_to: subscription}
  end

  # target_contents, bakery_directory, uuid, stem, extension, target_name
  def handle_events(events, _from, %Application{} = state) do
    Logger.info "HTML document consumer received #{inspect events}"
    processed_events = events
    |> Enum.map(&compose_document(state.html_directory, state.rap_uri_prefix, state.rap_style_sheet, state.time_zone, &1))
    |> Enum.map(&Prepare.write_result(&1.contents, state.bakery_directory, &1.uuid, "index", "html"))
    {:noreply, [], state}
  end

  # Result stem/extension should be configurable and in the Prepare
  # struct, since there's no way to guarantee these are constant across
  # runs, i.e. we could start the program with different parameters,
  # and then past generated HTML pages may break
  def compose_document(
    html_directory,
    rap_uri,
    style_sheet,
    time_zone,
    %Prepare{} = prepared 
  ) when prepared.runner_signal in [:working, :job_errors] do
    # %Prepare{} is effectively an annotated manifest struct, pass in a map
    potted_manifest = %{
      title:             prepared.title,
      description:       prepared.description,
      uuid:              prepared.uuid,
      manifest_pre_ttl:  prepared.manifest_pre_base_ttl,
      manifest_pre_yaml: prepared.manifest_pre_base_yaml,
      start_time:        prepared.start_time,
      end_time:          prepared.end_time
    }
    html_contents = doc_lead_in()
    |> head_lead_in()
    |> preamble(html_directory, style_sheet, prepared.uuid)
    |> head_lead_out()
    |> body_lead_in()
    |> manifest_info(html_directory, rap_uri, time_zone, potted_manifest)
    |> tables_info(  html_directory, rap_uri, prepared.uuid, prepared.staged_tables)
    |> jobs_info(    html_directory, prepared.staged_jobs)
    |> results_info( html_directory, rap_uri, time_zone, prepared.uuid, prepared.results)
    |> body_lead_out()
    |> doc_lead_out()
    %{
      uuid:     prepared.uuid,
      contents: html_contents
    }
  end


  ## FIXME
  def compose_document(
    html_directory,
    rap_uri,
    style_sheet,
    time_zone,
    _result_stem,
    _result_extension,
    %Prepare{ runner_signal: :see_producer } = prepared
  ) do
    potted_manifest = %{
      manifest_pre_ttl:  prepared.manifest_pre_base_ttl,
      manifest_pre_yaml: prepared.manifest_pre_base_yaml,
      start_time:        prepared.start_time,
      end_time:          prepared.end_time
    }
    html_contents = doc_lead_in()
    |> head_lead_in()
    |> preamble(html_directory, style_sheet, prepared.uuid)
    |> head_lead_out()
    |> body_lead_in()
    |> manifest_info(html_directory, rap_uri, time_zone, potted_manifest)
    |> body_lead_out()
    |> doc_lead_out()
    %{
      uuid:     prepared.uuid,
      contents: html_contents
    }
  end
  
  def doc_lead_in,         do: "<!DOCTYPE html>\n<html>\n"
  def head_lead_in(curr),  do: curr <> "<head>\n"
  def body_lead_in(curr),  do: curr <> "<body>\n"
  def doc_lead_out(curr),  do: curr <> "</html>\n"
  def head_lead_out(curr), do: curr <> "</head>\n"
  def body_lead_out(curr), do: curr <> "</body>\n"

  def preamble(curr, html_directory, style_sheet, uuid) do
    preamble_input = [uuid: uuid, style_sheet: style_sheet]
    preamble_fragment = EEx.eval_file("#{html_directory}/preamble.html", preamble_input)
    curr <> preamble_fragment
  end

  def manifest_info(curr, html_directory, rap_uri, time_zone, %{manifest_pre_ttl: nil} = manifest_spec) do
    info_extra = %{
      manifest_uri_ttl:    nil,
      manifest_uri_yaml:   nil,
      start_time_readable: format_time(manifest_spec.start_time, time_zone),
      end_time_readable:   format_time(manifest_spec.end_time,   time_zone)
    }
    info_input = manifest_spec |> Map.merge(info_extra) |> Map.to_list()

    info_fragment = EEx.eval_file("#{html_directory}/manifest.html", info_input)
    curr <> info_fragment
  end
  
  def manifest_info(curr, html_directory, rap_uri, time_zone, %{} = manifest_spec) do
    ttl_full  = "#{rap_uri}/#{manifest_spec.uuid}/#{manifest_spec.manifest_pre_ttl}"
    yaml_full = "#{rap_uri}/#{manifest_spec.uuid}/#{manifest_spec.manifest_pre_yaml}"

    info_extra = %{ 
      manifest_uri_ttl:    ttl_full,
      manifest_uri_yaml:   yaml_full,
      start_time_readable: format_time(manifest_spec.start_time, time_zone),
      end_time_readable:   format_time(manifest_spec.end_time,   time_zone)
    }
    info_input = manifest_spec |> Map.merge(info_extra) |> Map.to_list()

    info_fragment = EEx.eval_file("#{html_directory}/manifest.html", info_input)
    curr <> info_fragment
  end
  
  # uuid not included in object
  # for the maps, we don't weave in current state of document
  def stage_table(html_directory, rap_uri, uuid,
    %TableSpec{
      resource: %ResourceSpec{base: resource_path},
      schema:   %ResourceSpec{base: schema_path_ttl}
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

  def tables_info(curr, html_directory, rap_uri, uuid, tables) do
    tables_lead     = "<h1>Specified tables</h1>\n"
    table_fragments = tables
    |> Enum.map(&stage_table(html_directory, rap_uri, uuid, &1))
    |> Enum.join("\n")
    curr <> tables_lead <> table_fragments
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
  
  def jobs_info(curr, html_directory, jobs) do
    jobs_lead = "<h1>Specified jobs</h1>\n"
    job_fragments = jobs
    |> Enum.map(&stage_job(html_directory, &1))
    |> Enum.join("\n")
    curr <> jobs_lead <> job_fragments
  end

  # Assumption is that we have a notion of a completed job, (see named
  # RAP.Job.Result struct), annotated with the base name of the output file
  # As opposed to the final RAP.Bakery.Prepare struct which chucks away a
  # bunch of information.
  # Call the base name of the output file contents_base since result text
  # contents are called `contents'
  def stage_result(html_directory, rap_uri, time_zone, uuid, %Result{} = result) do
    target_base = "#{result.stem}_#{result.name}.#{result.extension}"
    target_uri  = "#{rap_uri}/#{uuid}/#{target_base}"
    result_extra = %{
      start_time_readable: format_time(result.start_time, time_zone),
      end_time_readable:   format_time(result.end_time,   time_zone),
      contents_base:       target_base,
      contents_uri:        target_uri,
      generated_results:   nil
    }
    result_input = result
    |> Map.merge(result_extra)
    |> Map.to_list()
    EEx.eval_file("#{html_directory}/result.html", result_input)
  end
  
  def results_info(curr, html_directory, rap_uri, time_zone, uuid, results) do
    results_lead = "<h1>Results</h1>\n"
    results_fragments = results
    |> Enum.map(&stage_result(html_directory, rap_uri, time_zone, uuid, &1))
    |> Enum.join("\n")
    curr <> results_lead <> results_fragments
  end

  defp format_time(unix_ts, time_zone) do
    weekdays = [ "Monday",  "Tuesday",  "Wednesday", "Thursday",
		 "Friday",  "Saturday", "Sunday"   ]
    months =   [ "January", "February", "March",
		 "April",   "May",      "June",
		 "July",    "August",   "September",
		 "October", "November", "December" ]
    dt = unix_ts |> DateTime.from_unix!()  |> DateTime.shift_zone!(time_zone)

    # These range from 1-7, 1-12 but lists are zero-indexed
    day_name      = weekdays  |> Enum.fetch!(Date.day_of_week dt - 1)
    month_name    = months    |> Enum.fetch!(dt.month - 1)
    
    padded_hour   = dt.hour   |> to_string |> String.pad_leading(2, "0")
    padded_minute = dt.minute |> to_string |> String.pad_leading(2, "0") 
    
    "#{day_name}, #{dt.day} #{month_name} #{dt.year}, #{padded_hour}:#{padded_minute} (GMT)"
  end
end
