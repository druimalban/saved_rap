defmodule RAP.Job.Spec do

  require Math
  require Logger

  defstruct [ :title, :type, :result ]
  
  #def run_job({ "ignore", col0, col1 }), do: dummy(col0, col1)
  #def run_job({ :job_mae, col0, col1 }), do: mae(col0, col1)
  #def run_job({ :job_rmsd, col0, col1 }), do: rmsd(col0, col1)
  #def run_job({ :job_emd, col0, col1 }), do: emd(col0, col1)

  def run_job(%RAP.Job.Producer{title: title, type: "ignore"}) do
    Logger.info "Dummy job requested"
    %RAP.Job.Spec{ title: title, type: "ignore", result: "Dummy/ignored job"}
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
    
    
