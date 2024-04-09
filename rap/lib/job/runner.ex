defmodule RAP.Job.Runner do

  use GenStage
  require Logger

  def start_link _args do
    Logger.info "Called Job.Runner.start_link (_)"
    initial_state = []
    GenStage.start_link __MODULE__, initial_state, name: __MODULE__
  end

  def init initial_state do
    Logger.info "Called Job.Runner.init (initial_state = #{inspect initial_state})"
    subscription = [
      { RAP.Job.Producer, min_demand: 0, max_demand: 1 }
    ]
    { :producer_consumer, initial_state, subscribe_to: subscription }
  end
  
  def handle_events events, _from, state do
    ie = inspect events
    is = inspect state
    Logger.info "Called Job.Runner.handle_events (event = #{ie}, _, state = #{is})"
    
    ne = events |> Enum.map(&RAP.Job.Spec.run_job/1)
    Logger.info "Result of Job.Runner.handle_events: #{inspect ne}"
    { :noreply, ne, state }
  end
  
end
    
