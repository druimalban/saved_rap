defmodule RAP.Job.Runner.Supervisor do

  use ConsumerSupervisor
  require Logger

  def start_link _args do
    initial_signal = :ok
    Logger.info "Called Job.Runner.Supervisor.start_link(_)"
    ConsumerSupervisor.start_link __MODULE__, initial_signal
  end

  def init(:ok = initial_signal) do
    Logger.info "Called Job.Runner.Supervisor.init (#{inspect initial_signal})"

    children = [
      %{
	id: RAP.Job.Runner,
	start: { RAP.Job.Runner, :start_link, [] },
	restart: :transient
      }
    ]
    opts = [
      strategy: :one_for_one,
      subscribe_to: [{ RAP.Job.Producer, min_demand: 0, max_demand: 1 }]
    ]

    ConsumerSupervisor.init children, opts
  end
  
end
