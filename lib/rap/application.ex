defmodule RAP.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false
  
  use Application
  
  @impl true
  def start(_type, _args) do
    current_ts = DateTime.utc_now() |> DateTime.to_unix()
    prep_state = [ rap_invoked_at: current_ts ]
    
    children = [
      {RAP.Storage.Monitor, []},      
      {RAP.Storage.GCP,     []},
      {RAP.Job.Producer,    []},
      {RAP.Job.Runner,      []},
      {RAP.Bakery.Prepare,  prep_state},
      {RAP.Bakery.Compose,  []}
      #RAP.Job.Cache.Supervisor,
      #RAP.Storage.TestConsumer
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: RAP.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
