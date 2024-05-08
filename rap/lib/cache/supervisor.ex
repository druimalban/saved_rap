defmodule RAP.Job.Cache do

  alias RAP.Storage.Monitor
  
  def start_link(%RAP.Job.Runner{} = result) do
    # Need to get the actual UUID later…
    fake_uuid = "f7a7e1da-4653-4f77-8896-8b72af4e7233"
    fake_resource = "/tmp/resource_file"
    fake_schema   = "/tmp/resource_schema"
    fake_manifest = "/tmp/manifest"
    
    Monitor.prep_rdf_data_set(fake_uuid, fake_resource, fake_schema, fake_manifest, result.results)
  end

end

defmodule RAP.Job.Cache.Supervisor do

  use ConsumerSupervisor
  require Logger

  alias RAP.Job.Cache

  def start_link(_args) do
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
