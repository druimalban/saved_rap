defmodule RAP.Storage.TestConsumer do

  use GenStage
  require Logger

  alias RAP.Storage.Monitor
  
  def start_link(_args) do
    initial_state = []
    GenStage.start_link(__MODULE__, initial_state)
  end

  def init(initial_state) do
    Logger.info "Initialised testing storage consumer"
    subscription = [
      { Monitor, min_demand: 0, max_demand: 1 }
    ]
    {:consumer, initial_state, subscribe_to: subscription}
  end

  def handle_events(events, _from, state) do
    Logger.info "Testing storage consumer received #{inspect events}"

    pretty_events = events |> Enum.map(&Monitor.pretty_print_object/1)

    {:noreply, [], state}
  end
end
