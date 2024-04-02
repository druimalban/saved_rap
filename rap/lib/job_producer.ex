defmodule RAP.Job.Producer do

  use GenStage
  require Logger

  def start_link _args do
    statinit = []
    GenStage.start_link __MODULE__, statinit, name: __MODULE__
  end

  def init statinit do
    Logger.info "Called Job.Producer.init (#{inspect statinit})"
    { :producer, statinit }
  end

  def handle_demand demand, state do
    insd = inspect demand
    inss = inspect state
    events = []
    Logger.info "Called Job.Producer.handle_demand (demand = #{insd}, state = #{inss})"
    { :noreply, events, state }
  end

  def trigger({job_code, col0, col1} = job_spec) do
    Logger.info "Called Job.Producer.trigger ({ #{inspect job_code} ,#{inspect col0} ,#{inspect col1} })"
    GenStage.cast __MODULE__, {:try_job, job_spec}
  end

  # Second element is a list of events to despatch
  # At the moment, though, we just despatch one event.
  def handle_cast {:try_job, job_spec }, state do
    { :noreply, [job_spec], state }
  end
  
end
