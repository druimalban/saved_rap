defmodule RAP.Job.Spec do

  require Logger
  
  def dummy col0, col1 do
    Logger.info "Called RAP.Job.Spec.dummy (col0 = #{inspect col0} ,col1 = #{inspect col1})"
    k = 1..5 |> Enum.random() |> :timer.seconds() |> Process.sleep()
    Logger.info "Result of RAP.Job.Spec.dummy: #{inspect k}"
  end

end
    
    
