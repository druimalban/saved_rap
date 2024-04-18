defmodule RAP.Job.Spec do

  require Math
  require Logger

  defstruct [ :title, :type, :signal, :result ]

  defp cmd_wrapper(shell, command, args) do
    System.cmd shell, [ command | args ]
  end

  def run_job(%RAP.Job.Producer{title: title, type: "ignore"}) do
    Logger.info "Dummy job requested"
    %RAP.Job.Spec{ title: title, type: "ignore", signal: :ok, result: "Dummy/ignored job" }
  end

  def run_job(%RAP.Job.Producer{
	title: title,
	type: "ext_density_count" = type,
	state: [%{table: %RAP.Manifest.TableDesc{resource_path: fp_dens_t}, valid_columns: [density, time]},
	        %{table: %RAP.Manifest.TableDesc{resource_path: fp_counts}, valid_columns: [total]}]}
  ) do
    Logger.info "Call to external command/executable #{type} requested"
    
    { res, sig } =
      cmd_wrapper("python3.9", "contrib/density_count_ode.py", [
	    "data_cache/#{fp_counts}", total,
	    "data_cache/#{fp_dens_t}", time,  density        ])
    if (sig == 0) do
      Logger.info "Call to external command/executable #{type} succeeded"
      %RAP.Job.Spec{ title: title, type: type, signal: :ok,    result: res }
    else
      Logger.info "Call to external command/executable #{type} failed"
      %RAP.Job.Spec{ title: title, type: type, signal: :error, result: res }
    end
  end
  
  defp mae col0, col1 do
    Logger.info "Called Job.Spec.mae (col0 = #{inspect col0} ,col1 = #{inspect col1})"
    Enum.zip( col0, col1)
    |> Enum.map( fn{y, x} -> abs(y - x) end)
    |> Math.Enum.mean()
  end

  defp rmsd col0, col1 do
    Logger.info "Called Job.Spec.rmsd (col0 = #{inspect col0} ,col1 = #{inspect col1})"
    Enum.zip( col0, col1)
    |> Enum.map( fn{x, y} -> Math.pow(x-y, 2) end)
    |> Math.Enum.mean()
    |> Math.sqrt()
  end
  
end
    
    
