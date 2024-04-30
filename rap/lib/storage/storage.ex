defmodule RAP.Storage do
  
  use Amnesia

  alias RAP.Job.Runner
  
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
	resource_file:   "#{uuid}_#{fp}",
	resource_schema: "#{uuid}_#{schema}",
	job_manifest:    "#{uuid}_#{manifest}",
	job_results:     results}
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
