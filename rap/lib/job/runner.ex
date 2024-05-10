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
  
  defstruct [ :name, :title, :description, :type, :signal, :result ]

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
      result:      "Dummy/ignored job"
    }
  end

  def run_job(
    uuid, cache_directory,

    %JobSpec{
      type: "density",
      scope_collected: [ %ColumnSpec{
			   variable: "lice_count_total",
			   column:   label_count,
			   table:    table_count,
			   resource: resource_count}
			 | _ ],
      scope_modelled:  [ %ColumnSpec{
			   variable: "density",
			   column:   label_density,
			   table:    table_density,
			   resource: resource_density},
			 %ColumnSpec{
			   variable: "time",
			   column:   label_time,
			   table:    table_time,
			   resource: resource_time}
			 | _ ]
    } = spec) do

    if resource_density != resource_time do
      res = "Density and time not derived from same data file"
      %Result{ title:  spec.title, description: spec.description,
	       type:   "density",  signal:      :failure_prereq,
	       result: res }
    else
      file_path_count   = "#{cache_directory}/#{uuid}/#{resource_count}"
      file_path_density = "#{cache_directory}/#{uuid}/#{resource_density}"
      _file_path_time   = "#{cache_directory}/#{uuid}/#{resource_time}"
      
      { res, sig } =
 	cmd_wrapper("python3.9", "contrib/density_count_ode.py", [
 	            file_path_count,   label_count,
 	            file_path_density, label_time,  label_density])
      if (sig == 0) do
 	Logger.info "Call to external command/executable density_count_ode succeeded:"
	Logger.info res
 	%Result{ title:  spec.title, description: spec.description,
		 type:   "density",  signal:      :ok,
		 result: res }
      else
 	Logger.info "Call to external command/executable density_count_ode failed"
 	%Result{ title:  spec.title, description: spec.description,
		 type:   "density",  signal:      :error,
		 result: res }
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
	      :manifest_path,  :resources,
	      :staging_tables, :staging_jobs,	      :results ]
  
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
  
  def process_jobs(%ManifestSpec{} = spec, cache_directory) do
    
    results = spec.staging_jobs
    |> Enum.map(&Result.run_job(spec.uuid, cache_directory, &1))
    
    %Runner{ uuid:           spec.uuid,
	     local_version:  spec.local_version,
	     title:          spec.title,
	     description:    spec.description,
	     manifest_path:  spec.manifest_path,
	     resources:      spec.resources,
	     staging_tables: spec.staging_tables,
	     staging_jobs:   spec.staging_jobs,
	     results:        results            }
  end
  
end
    
