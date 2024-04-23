defmodule RAP.Job.Cache do
  
  def start_link(%RAP.Job.Runner{results: res}) do
    Task.start_link(fn ->
      res |> Enum.map(&IO.puts(&1.result))
    end)
  end

end

defmodule RAP.Job.Cache.Supervisor do

  use ConsumerSupervisor
  require Logger

  alias RAP.Job.Cache

  def start_link(initial_signal) do
    ConsumerSupervisor.start_link(__MODULE__, :ok)
  end
  
  def init(:ok) do
    children = [
      worker(Cache, [], restart: :temporary)
    ]

    {
      :ok, children, strategy: :one_for_one,
      subscribe_to: [{ RAP.Job.Runner, min_demand: 0, max_demand: 1 }]
    }
  end

end
