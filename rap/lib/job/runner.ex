defmodule RAP.Job.Result do
  @moduledoc """
  Same as for the `.Spec' module, this is a simple module declaration
  which is on the level of an individual job. Therefore, there is a name
  associated with the job, as well as a signal and/or result.
  """
  require Logger

  alias RAP.Manifest.TableDesc
  alias RAP.Job.{ScopeSpec, JobSpec, TableSpec, ManifestSpec}
  
  defstruct [ :name, :title, :description,
	      :source_job,    :type,
	      :output_format, :output_stem,
	      :signal,        :contents,
	      :start_time,    :end_time ]

  @doc """
  Normalises the two elements of failure:
    a) Command runs, but the exit code is non-zero;
    b) Command cannot be run at all (throw `ErlangError' with various codes,
       most commonly :enoent).
  """
  def cmd_wrapper(shell, command, args) do
    cmd_result =
      try do
	System.cmd shell, [ command | args ], parallelism: true
      rescue
        erlang_error -> IO.inspect(erlang_error)
      end
    case cmd_result do
      {result, 0} ->
	Logger.info "Result.cmd_wrapper/3: Exit status was zero"
	Logger.info(inspect result)
	{:run_success, 0, result}
      {result, signal} ->
	Logger.info "Result.cmd_wrapper/3: Exit status was non-zero"
	Logger.info(inspect result)
	{:run_error, signal, result}
      %ErlangError{original: signal, reason: reason} ->
	Logger.info "Result.cmd_wrapper/3: Call to executable failed with signal #{inspect signal} and reason #{inspect reason}}"
	{:call_error, signal, reason }
      error ->
	Logger.info "Result.cmd_wrapper/3: Call to executable failed with error #{inspect error}"
	{:call_error, error, nil}
    end
  end
  
  def run_job(_uuid, _cache_dir, _interpreter, %JobSpec{type: "ignore"} = spec) do
    %__MODULE__{
      name:        spec.name,
      title:       spec.title,
      description: spec.description,
      source_job:  spec.name,
      type:        "ignore",
      signal:      :ignored,
      contents:    "Dummy/ignored job"
    }
  end

  def run_job(
    uuid, cache_directory, python_call,
    
    %JobSpec{
      type:            "density",
      scope_collected: [ %ScopeSpec{
			   variable_curie: "saved:lice_af_total",
			   column:         label_count,
			   resource_base:  resource_count}
			 | _ ],
      scope_modelled:  [ %ScopeSpec{
			   variable_curie: "saved:density",
			   column:         label_density,
			   resource_base:  resource_density},
			 %ScopeSpec{
			   variable_curie: "saved:time",
			   column:         label_time,
			   resource_base:  resource_time}
			 | _ ]
    } = spec) do
    Logger.info "Running job #{spec.name} (associated with UUID #{uuid})"
    start_ts = DateTime.utc_now() |> DateTime.to_unix()

    if resource_density != resource_time do
      end_ts = DateTime.utc_now() |> DateTime.to_unix()
      res = "Density and time not derived from same data file"
      %__MODULE__{ title:  spec.title, description: spec.description,
	       type:   "density",  signal:      :failure_prereq,
	       contents: res, start_time: start_ts, end_time: end_ts }
    else
      file_path_count   = "#{cache_directory}/#{uuid}/#{resource_count}"
      file_path_density = "#{cache_directory}/#{uuid}/#{resource_density}"
      _file_path_time   = "#{cache_directory}/#{uuid}/#{resource_time}"

      Logger.info "Fully-qualified path for count data is #{file_path_count}"
      Logger.info "Fully-qualified path for density/time data is #{file_path_density}"

      end_ts = DateTime.utc_now() |> DateTime.to_unix()
      
      # This needs to be fixed so that it's less fragile, at least in terms of:
      # a) Python version
      # b) Guarantees about dependencies
      # We're after good reporting, and this information should certainly be part of that.
      py_result = 
 	cmd_wrapper(python_call, "contrib/density_count_ode.py", [
 	            file_path_count,   label_count,
 	            file_path_density, label_time,  label_density])
      case py_result do
	{:run_success, _sig, py_result} ->
	  Logger.info "Call to external command/executable density_count_ode succeeded:"
	  Logger.info(inspect py_result)
 	  %__MODULE__{
	    name: spec.name, title: spec.title,
	    description:   spec.description,
	    type:          "density",
	    output_format: spec.result_format,
	    output_stem:   spec.result_stem,
	    source_job:    spec.name,
	    start_time:    start_ts,  end_time: end_ts,
	    signal:        :working,
	    contents:      py_result
	  }
		   
	{:run_error, _sig, py_result} ->
	  Logger.info "Call to external command/executable density_count_ode: non-zero exit status:"
	  Logger.info(inspect py_result)
 	  %__MODULE__{
	    name: spec.name, title: spec.title,
	    description:   spec.description,
	    type:          "density",
	    output_format: spec.result_format,
	    output_stem:   spec.result_stem,
	    source_job:    spec.name,
	    start_time:    start_ts,      end_time: end_ts,
	    signal:        :job_failure,
	    contents:      py_result
	  }
	  
	{:call_error, py_error, py_result} ->
	  Logger.info "Call to Python interpreter failed or system is locked up"
 	  %__MODULE__{
	    name: spec.name, title: spec.title,
	    description:   spec.description,
	    type:          "density",
	    output_format: spec.result_format,
	    output_stem:   spec.result_stem,
	    source_job:    spec.name,
	    start_time:    start_ts,  end_time:   end_ts,
	    signal:        :python_error,
	    contents:      py_result
	  }
       end
     end
    
  end

  def run_job(_uuid, _cache_dir, _interpreter, %JobSpec{} = bad_spec) do
    %__MODULE__{
      name:          bad_spec.name,
      title:         bad_spec.title,
      description:   bad_spec.description,
      type:          bad_spec.type,
      source_job:    bad_spec.name,
      signal:        :bad_job_spec,
      contents:      "Unrecognised job spec"
    }
  end
  
end

defmodule RAP.Job.Runner do

  use GenStage
  require Logger

  alias RAP.Job.{Producer, Result}
  alias RAP.Job.ManifestSpec

  defstruct [
    :uuid,  :data_source, :local_version, :name, :title, :description,
    :manifest_base_ttl, :manifest_base_yaml, :resource_bases,
    :staging_tables, :staging_jobs,
    :pre_signal, :producer_signal,
    :signal, :results
  ]
  
  def start_link initial_state do
    Logger.info "Called Job.Runner.start_link (_)"
    GenStage.start_link __MODULE__, initial_state, name: __MODULE__
  end

  def init initial_state do
    Logger.info "Called Job.Runner.init (initial_state = #{inspect initial_state})"
    subscription = [
      { Producer, min_demand: 0, max_demand: 1 }
    ]
    { :producer_consumer, initial_state, subscribe_to: subscription }
  end
  
  def handle_events events, _from, state do
    ie = inspect events
    is = inspect state
    Logger.info "Called Job.Runner.handle_events (events = #{ie}, _, state = #{is})"
    
    target_events = events |> Enum.map(&process_jobs(&1, state.cache_directory, state.python_call))
    { :noreply, target_events, state }
  end
  
  def process_jobs(%ManifestSpec{signal: :working, staging_jobs: staging} = spec, cache_directory, python_call) do  
    Logger.info "Staging jobs: #{inspect staging}"
    
    result_contents = staging
    |> Enum.map(&Result.run_job(spec.uuid, cache_directory, python_call, &1))

    # Do need to have a notion of different signals
    overall_signal =
      if Enum.any?(result_contents, &(&1.signal == :working)) do
	:working
      else
	:job_errors
      end
      
    %__MODULE__{
      uuid:               spec.uuid,
      data_source:        spec.data_source,
      local_version:      spec.local_version,
      name:               spec.name,
      title:              spec.title,
      description:        spec.description,
      manifest_base_ttl:  spec.manifest_base_ttl,
      manifest_base_yaml: spec.manifest_base_yaml,
      resource_bases:     spec.resource_bases,
      staging_tables:     spec.staging_tables,
      staging_jobs:       spec.staging_jobs,
      pre_signal:         spec.pre_signal,
      producer_signal:    spec.signal,
      signal:             overall_signal,
      results:            result_contents
    }
  end

  def process_jobs(%ManifestSpec{signal: :see_pre} = spec, _cache, _interpreter) do
    %__MODULE__{
      uuid:            spec.uuid,
      data_source:     spec.data_source,
      pre_signal:      spec.pre_signal,
      producer_signal: :see_pre,
      signal:          :see_pre      
    }
  end

  @doc """
  # :empty_manifest | :bad_input_graph | :bad_manifest_tables
  # We thus have access to anything in the `minimal_manifest/2' function:

  def minimal_manifest(%MidRun{} = prev, curr_signal) do
    %ManifestSpec{ name:               prev.manifest_name,
		   uuid:               prev.uuid,
		   pre_signal:         prev.signal,
		   signal:             curr_signal,
		   manifest_base_ttl:  prev.manifest_ttl,
		   manifest_base_yaml: prev.manifest_yaml,
		   resource_bases:     prev.resources    }
  end
  """
  def process_jobs(%ManifestSpec{} = spec, _cache) do
    %__MODULE__{
      name:               spec.name,
      uuid:               spec.uuid,
      data_source:        spec.data_source,
      pre_signal:         spec.pre_signal,
      producer_signal:    spec.signal,
      signal:             :see_producer,
      manifest_base_ttl:  spec.manifest_base_ttl,
      manifest_base_yaml: spec.manifest_base_yaml,
      resource_bases:     spec.resource_bases
    }
  end
end
