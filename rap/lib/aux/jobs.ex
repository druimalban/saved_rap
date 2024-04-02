defmodule RAP.Job.Spec do

  require Math
  require Logger
  
  def run_job({ :job_dummy, col0, col1 }), do: dummy(col0, col1)
  def run_job({ :job_mse, col0, col1 }), do: mse(col0, col1)
  def run_job({ :job_emd, col0, col1 }), do: emd(col0, col1)
  
  defp dummy col0, col1 do
    Logger.info "Called Job.Spec.dummy (col0 = #{inspect col0} ,col1 = #{inspect col1})"
    res = Math.Enum.mean(col0 ++ col1)
    Process.sleep(500)
    Logger.info "Result of Job.Spec.dummy: #{inspect res}"
    res
  end
  
  defp mse(_col0, _col1), do: :ok
  defp emd(_col0, _col1), do: :ok
  
end
    
    
