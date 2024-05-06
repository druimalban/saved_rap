defmodule RAP.Storage do
  @moduledoc """
  This module really serves as 'plumbing' to name state and to store
  results. The named struct is purposefully near-identical to the table
  in the mnesia cache row because it should hold state and let us call
  this immediately prior to injecting them into the cache.

  There is a slight complication of different data formats for some of
  these fields (e.g. LinkML YAML schemata vs turtle equivalent). Make it
  work for one and then extend it.
  """
  use Amnesia

  alias RAP.Job.Runner

  defstruct [ :uuid, :manifest, :resources, :results ]

  defdatabase DB do
    deftable Job, [ :uuid, :manifest, :resources, :results ], type: :bag
  end
end

defmodule RAP.Storage.Staging do

  require Amnesia
  require Amnesia.Helper
  require RAP.Storage.DB.Job, as: DB

  defstruct [ :uuid, :index, :resources ]

  def feasible?(uuid) do
    Amnesia.transaction do
      case DB.read(uuid) do
	nil -> true
	_   ->
	  Logger.info("Found job UUID #{uuid} in job cache!")
	  false
      end
    end
  end
  
  def prep_data_set(uuid, manifest, resources, results) do
    Amnesia.transaction do
      %DB{
	uuid:      uuid,
	manifest:  manifest,
	resources: resources,
	results:   results  }
      |> DB.write()
    end
  end
  
end
