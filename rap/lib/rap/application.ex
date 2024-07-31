defmodule RAP.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false
  
  use Application
  
  @doc
  """
  Initial / global pipeline state

  Define a number of hard-coded attributes here, this makes it very easy
  to be able to read these later as CLI arguments. There are quite a few
  of these and the named struct makes pattern matching work well, and can
  also accept nil values - e.g. we don't define a connection here, but in
  the `RAP.Storage.Monitor' producer stage.

  Use a single GCP bucket for now, because in practice, if we want to
  monitor more than one, this would imply a distinct stages, since it is
  presumed that the reason behind using multiple buckets is to treat the
  data contained within differently.
  """
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
