defmodule RAP.Job.Cache do

  require Logger

  def start_link event do
    Logger.info "Called Job.Cache.start_link (event = #{inspect event})"
    
    Task.start_link fn ->
      RAP.Cache.Spec.dummy event
    end
  end

end
