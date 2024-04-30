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

  defstruct [ :uuid, :resource_file, :resource_schema, :job_manifest ]

  defdatabase DB do
    deftable Job, [:uuid, :resource_file, :resource_schema, :job_manifest, :job_results], type: :bag
  end 
end

defmodule RAP.Storage.Transactions do

  require Amnesia
  require Amnesia.Helper
  require RAP.Storage.DB.Job
  
  def prep_data_set(uuid, fp, schema, manifest, results) do
    Amnesia.transaction do
      %RAP.Storage.DB.Job{
	uuid:            uuid,
	resource_file:   fp,
	resource_schema: schema,
	job_manifest:    manifest,
	job_results:     results }
      |> RAP.Storage.DB.Job.write()
    end
  end

  
  def feasible?(uuid) do
    Amnesia.transaction do
      case RAP.Storage.DB.Job.read(uuid) do
	nil -> true
	_   ->
	  Logger.info("Found job UUID #{uuid} in job cache!")
	  false
      end
    end
  end
  
end
