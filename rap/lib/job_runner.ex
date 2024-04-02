defmodule RAP.Job.Runner do

  use GenStage
  require Logger

  def start_link event do
    Logger.info "Called Job.Runner.start_link (event = #{inspect event})"

    Task.start_link(fn ->
      RAP.Job.Spec.dummy([], [])
    end)
  end

end
