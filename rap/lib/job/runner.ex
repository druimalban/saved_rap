defmodule RAP.Job.Result do
  @defmodule """
  Same as for the `.Spec' module, this is a simple module declaration
  which is on the level of an individual job.
  """
  require Logger

  alias RAP.Manifest.TableDesc
  alias RAP.Job.{Result, Spec}
  
  defstruct [ :title, :description, :type, :signal, :result ]

  defp cmd_wrapper(shell, command, args) do
    System.cmd shell, [ command | args ]
  end

#  def run_job(%Spec{title: title, description: desc, type: "ignore"}) do
#     %Result{title:  title, description: desc, type: "ignore",
# 	    signal: :ok,   result:      "Dummy/ignored job"}
#   end
#   
#   def run_job(%Spec{title: title, description: desc, type: "density",
# 		    pairs_descriptive: [],
# 		    pairs_collected: [
# 		      { :valid_table, "saved:lice_count_total", lice_count_label, table_total }
# 		    ],
# 		    pairs_modelled: [
# 		      { :valid_table, "saved:density", density_label, table_dens },
# 		      { :valid_table, "saved:time",    time_label,    table_time }
# 		    ]}) do
#     
#     fp_counts = table_total.resource_path.path
#     fp_dens = table_dens.resource_path.path # Will need to restructure data so that it gathers these
#     fp_time = table_time.resource_path.path
#     
#     if (fp_dens != fp_time) do
#       res = "Density and time not derived from same data file"
#       %Result{ title: title, description: desc, type: "density", signal: :error, result: res}
#     else
#       { res, sig } =
# 	cmd_wrapper("python3.9", "contrib/density_count_ode.py", [
# 	      "data_cache/#{fp_counts}", lice_count_label,
# 	      "data_cache/#{fp_dens}", time_label, density_label])
#       if (sig == 0) do
# 	Logger.info "Call to external command/executable density_count_ode succeeded"
# 	%Result{ title: title, description: desc, type: "density", signal: :ok, result: res }
#       else
# 	Logger.info "Call to external command/executable density_count_ode failed"
# 	%Result{ title: title, description: desc, type: "density", signal: :error, result: res }
#       end
#     end
#   end

  # Placeholder
  def run_job(spec), do: spec
  

##### OLD OLD OLD OLD OLD #####
  #  def run_job(%Spec{
#	title: title,
#	type: "ext_density_count" = type,
#	state: [%{table: %RAP.Manifest.TableDesc{resource_path: fp_dens_t}, valid_columns: [density, time]},
#	        %{table: %RAP.Manifest.TableDesc{resource_path: fp_counts}, valid_columns: [total]}]}
#  ) do
#    Logger.info "Call to external command/executable #{type} requested"
#    
#    { res, sig } =
#      cmd_wrapper("python3.9", "contrib/density_count_ode.py", [
#	    "data_cache/#{fp_counts}", total,
#	    "data_cache/#{fp_dens_t}", time,  density        ])
#    if (sig == 0) do
#      Logger.info "Call to external command/executable #{type} succeeded"
#      %RAP.Job.Spec{ title: title, type: type, signal: :ok,    result: res }
#    else
#      Logger.info "Call to external command/executable #{type} failed"
#      %RAP.Job.Spec{ title: title, type: type, signal: :error, result: res }
#    end
#  end
  

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

  alias RAP.Job.{Producer, Result, Runner, Spec}

  defstruct [ :title, :description, :type, :results ]
  
  def start_link _args do
    Logger.info "Called Job.Runner.start_link (_)"
    initial_state = []
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
    
    ne = events |> Enum.map(&process_jobs/1)
    { :noreply, ne, state }
  end
  
  def process_jobs(%Producer{title: title, description: desc, staging_jobs: staging}) do
    results = staging |> Enum.map(&Result.run_job/1)
    %Runner{ title: title, description: desc, results: results }
  end  
  
  #def run_job(%RAP.Job.Producer{title: title, type: "rmsd",
  #				pairs_collected: [collected],
  #				pairs_modelled:  [modelled]}) do
  #  Logger.info "Mean absolute error job requested"
  #  res = nil # rmsd(collected, modelled)
  #  %Runner{ title: title, type: "mae", signal: :ok, result: res }
  #end
  
end
    
