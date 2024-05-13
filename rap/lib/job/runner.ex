defmodule RAP.Job.Result do
  @moduledoc """
  Same as for the `.Spec' module, this is a simple module declaration
  which is on the level of an individual job. Therefore, there is a name
  associated with the job, as well as a signal and/or result.
  """
  require Logger

  alias RAP.Manifest.TableDesc
  alias RAP.Job.Result
  alias RAP.Job.{ColumnSpec, JobSpec, TableSpec, ManifestSpec}
  
  defstruct [ :name,        :title,
	      :description, :type,
	      :signal,      :contents,
	      :start_time,  :end_time ]

  defp cmd_wrapper(shell, command, args) do
    System.cmd shell, [ command | args ]
  end
  
  def run_job(_uuid, _cache_dir, %JobSpec{type: "ignore"} = spec) do
    %Result{
      name:        spec.name,
      title:       spec.title,
      description: spec.description,
      type:        "ignore",
      signal:      :ok,
      contents:    "Dummy/ignored job"
    }
  end

  def run_job(
    uuid, cache_directory,
    
    %JobSpec{
      name:            job_name,
      type:            "density",
      scope_collected: [ %ColumnSpec{
			   variable:      "lice_count_total",
			   column:        label_count,
			   table:         table_count,
			   resource_base: resource_count}
			 | _ ],
      scope_modelled:  [ %ColumnSpec{
			   variable:      "density",
			   column:        label_density,
			   table:         table_density,
			   resource_base: resource_density},
			 %ColumnSpec{
			   variable:      "time",
			   column:        label_time,
			   table:         table_time,
			   resource_base: resource_time}
			 | _ ]
    } = spec) do
    Logger.info "Running job #{job_name} (associated with UUID #{uuid})"
    start_ts = DateTime.utc_now() |> DateTime.to_unix()

    if resource_density != resource_time do
      end_ts = DateTime.utc_now() |> DateTime.to_unix()
      res = "Density and time not derived from same data file"
      %Result{ title:  spec.title, description: spec.description,
	       type:   "density",  signal:      :failure_prereq,
	       contents: res, start_time: start_ts, end_time: end_ts }
    else
      file_path_count   = "#{cache_directory}/#{uuid}/#{resource_count}"
      file_path_density = "#{cache_directory}/#{uuid}/#{resource_density}"
      _file_path_time   = "#{cache_directory}/#{uuid}/#{resource_time}"

      Logger.info "Fully-qualified path for count data is #{file_path_count}"
      Logger.info "Fully-qualified path for density/time data is #{file_path_density}"
      
      { res, sig } =
 	cmd_wrapper("python3.9", "contrib/density_count_ode.py", [
 	            file_path_count,   label_count,
 	            file_path_density, label_time,  label_density])

      end_ts = DateTime.utc_now() |> DateTime.to_unix()
      if (sig == 0) do
 	Logger.info "Call to external command/executable density_count_ode succeeded:"
	Logger.info res
 	%Result{ title:      spec.title, description: spec.description,
		 type:       "density",  signal:      :ok,
		 start_time: start_ts,  end_time: end_ts,
		 contents:   res}
      else
 	Logger.info "Call to external command/executable density_count_ode failed"
 	%Result{ title:      spec.title, description: spec.description,
		 type:       "density",  signal:      :error,
		 start_time: start_ts,   end_time:    end_ts,
		 contents:   res }
       end
     end
    
  end

  defp mae col0, col1 do
    Logger.info "Called Job.Spec.mae (col0 = #{inspect col0} ,col1 = #{inspect col1})"
    Enum.zip(col0, col1)
    |> Enum.map( fn{y, x} -> abs(y - x) end)
    |> Math.Enum.mean()
  end

  defp rmsd col0, col1 do
    Logger.info "Called Job.Spec.rmsd (col0 = #{inspect col0} ,col1 = #{inspect col1})"
    Enum.zip(col0, col1)
    |> Enum.map( fn{x, y} -> Math.pow(x-y, 2) end)
    |> Math.Enum.mean()
    |> Math.sqrt()
  end
  
end

defmodule RAP.Job.Runner do

  use GenStage
  require Logger

  alias RAP.Job.{Producer, Result, Runner}
  alias RAP.Job.ManifestSpec

  defstruct [ :uuid,  :local_version,
	      :title, :description,
	      :manifest_base,  :resource_bases,
	      :staging_tables, :staging_jobs,
	      :results                        ]
  
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
    
    target_events = events |> Enum.map(&process_jobs(&1, state.cache_directory))
    { :noreply, target_events, state }
  end
  
  def process_jobs(%ManifestSpec{staging_jobs: staging} = spec, cache_directory) do
    
    Logger.info "Staging jobs: #{inspect staging}"
    
    result_contents = staging
    |> Enum.map(&Result.run_job(spec.uuid, cache_directory, &1))

    # Not clear whether to rename results attribute
    # these do contain %Result{} objects, whereas we don't want to repeat
    # 
    %Runner{ uuid:           spec.uuid,
	     local_version:  spec.local_version,
	     title:          spec.title,
	     description:    spec.description,
	     manifest_base:  spec.manifest_base,
	     resource_bases: spec.resource_bases,
	     staging_tables: spec.staging_tables,
	     staging_jobs:   spec.staging_jobs,
	     results:        result_contents    }
  end
  
end
    
