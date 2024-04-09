defmodule RAP.Job.Spec do

  require Math
  require Logger
  
  def run_job({ :job_dummy, col0, col1 }), do: dummy(col0, col1)
  def run_job({ :job_mae, col0, col1 }), do: mae(col0, col1)
  def run_job({ :job_rmsd, col0, col1 }), do: rmsd(col0, col1)
  def run_job({ :job_emd, col0, col1 }), do: emd(col0, col1)
  
  defp dummy col0, col1 do
    Logger.info "Called Job.Spec.dummy (col0 = #{inspect col0} ,col1 = #{inspect col1})"
    res = Math.Enum.mean(col0 ++ col1)
    Process.sleep(500)
    Logger.info "Result of Job.Spec.dummy: #{inspect res}"
    res
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
    
  defp emd(_col0, _col1), do: :ok
  
end
    
    
