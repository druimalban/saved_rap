defmodule RAP.Job.Spec do
  @moduledoc """
  At the moment, this module just includes a single struct definition.
  The aim here is to separate the declaration of the structure of the
  job from the structure of the producer, which holds its own state.

  This is better than having the job producer functions return a list
  of its named struct.
  """
  defstruct [ :title, :description, :type, :pairs_descriptive, :pairs_collected, :pairs_modelled ]
  
end

defmodule RAP.Job.Staging do

  defstruct [ :signal, :variable, :label, :target ]
  
end

defmodule RAP.Job.Producer do
  @moduledoc """
  This is the producer stage of the RAP, which, given a turtle manifest
  path, attempts to read this into an RDF graph, and then load it into a
  struct.

  The producer further generates a list of jobs which are to be run,
  based on the manifest contents.

  It is possible that there will be no jobs possible, probably because
  the manifest is ill-formed.
  """

  alias RAP.Manifest.{TableDesc, ColumnDesc, JobDesc, ManifestDesc}
  alias RAP.Storage.{GCP, Monitor}
  alias RAP.Job.{Producer, Spec, Staging}
  
  use GenStage
  require Logger

  import :timer, only: [ sleep: 1 ]

  defstruct [ :title, :description, :staging_jobs ]

  def start_link initial_state do
    Logger.info "Called Job.Producer.start_link (_)"
    GenStage.start_link __MODULE__, initial_state, name: __MODULE__
  end

  def init initial_state do
    Logger.info "Called Job.Producer.init (initial_state = #{inspect initial_state})"
    subscription = [
      # Fix this using Storage.Monitor -> Storage.GCP/Storage.Local stages
      { GCP, min_demand: 0, max_demand: 1 }
    ]
    { :producer_consumer, initial_state, subscribe_to: subscription }
  end
  
  def handle_events events, _from, state do
    pretty_events = events |> Enum.map(& &1.uuid) |> inspect()
    Logger.info "Called Job.Producer.handle_events on #{pretty_events}"

    # Fix once we've got the GCP stuff nailed down
    processed = events
    |> Enum.map(&invoke_manifest(&1, state.cache_directory))
    
    { :noreply, processed, state }
  end
  
  #def handle_demand demand, ts do
  #  insd = inspect demand
  #  inss = inspect ts
  #  events = []
  #  Logger.info "Called Job.Producer.handle_demand (demand = #{insd}, state = #{inss})"
  #  { :noreply, events, ts }
  #end
  
  #  @doc """
  #   Generic helper function which sends the :try_jobs signal. This is
  #   a place-holder which lets us manually trigger a job, since the aim is
  #   to monitor GCP / other storage backend for new manifests to run. This
  #   will probably be performed by an earlier stage.
  #   """
  #   def trigger manifest_path do
  #     Logger.info "Called Job.Producer.trigger ()"
  #     #({ #{inspect job_code} ,#{inspect col0} ,#{inspect col1} })"
  #     GenStage.cast __MODULE__, {:try_jobs, manifest_path}
  #   end
  defp pretty_print_table(%TableDesc{resource_path: file, schema_path: schema}), do: "#{file} (#{schema})"
  defp paths_extant?(%TableDesc{resource_path: file, schema_path: schema}, target_dir, resources) do
    "#{target_dir}/#{file}" in resources and "#{target_dir}/#{schema}" in resources
  end
  defp check_resources(%ManifestDesc{tables: ts} = manifest, target_dir, resources) do
    with [_ | _] = proc <- Enum.filter(ts, &paths_extant?(&1, target_dir, resources)),
	 {true, proc}   <- {length(proc) == length(ts), proc} do
      #Logger.info "Of referenced files #{inspect resources}, all were valid"
      Logger.info "All files in #{target_dir} were valid"
      {:ok, manifest, proc}
    else
      [] ->
	Logger.info "No files in #{target_dir} were valid"
	{:error, :none, manifest}
      {false, proc} ->
	pretty_valid = proc |> Enum.map(&pretty_print_table/1) |> inspect()
	Logger.info "Some files in #{target_dir} were valid: #{pretty_valid}"
        {:error, :some, manifest, proc}
    end
  end

  @doc """
  Given the :try_jobs signal, attempt to devise a set of jobs which are
  to be run.
  """
  def invoke_manifest(%GCP{uuid: uuid, manifest: manifest_path, resources: resources_paths}, cache_dir) do
    target_dir = "#{cache_dir}/#{uuid}"
    Logger.info "Building RDF graph from turtle manifest using data in #{target_dir}"
    with {:ok, rdf_graph} <- RDF.Turtle.read_file(manifest_path),
         {:ok, ex_struct} <- Grax.load(rdf_graph, RAP.Vocabulary.RAP.RootManifest, ManifestDesc),
         {:ok, _struct}        <- check_skeleton(ex_struct),
         {:ok, _extant, proc} <- check_resources(ex_struct, target_dir, resources_paths) do
      Logger.info "Detecting feasible jobs"
      base_iri = RDF.IRI.to_string(rdf_graph.base_iri)
    else
      {:error, err} ->
	Logger.info "Could not read RDF graph #{manifest_path}"
	Logger.info "Error was #{inspect err}"
      {:error, "manifest empty", _manifest} ->
	Logger.info "Generated manifest was empty!"
      error -> error
    end
  end
  defp check_skeleton(%ManifestDesc{description: nil, title:         nil,
				     tables:      [],  jobs:          [],
				     gcp_source:  nil, local_version: nil
				    } = manifest), do: {:error, "manifest_empty", manifest}
  defp check_skeleton(%ManifestDesc{}, manifest), do: {:ok, manifest}
  
  @doc """
  Given a manifest, check the following:

  1. Each job has an ID (`atomic_name'), a title (not important for a job),
     a flag to say whether it is auto-generated (important), and a list of
     sources.
     
  2. For the list of sources, while there is an ID (the `atomic_name') field,
     for each element, there is an additional field which defines which table the
     source is derived from. This is distinct because it is not possible to have
     duplicate fields in the RDF, so this field cannot be an identifier.

     When auto-generating these sources, all I do is prepend 'source_example_'
     to the table field. This is probably reasonably self-explanatory, so it is
     good style to suggest.

  In terms of naming:

  These are a bunch of nested maps which preserve the structure of the job
  manifest. I.e., for a given job, and some number of tables against which to
  check there exist the table and/or columns, return the same job source
  structure annotated with 1. the table and 2. valid/extrenuous columns.

  The mapping functions over the *job* should be called something like
  <operation>_over_job and the mapping functions over the *source* component of
  the job should be called something like <operation>_over_job_source.
  """
  defp compare_table_call(base_iri_text, tables, %ColumnDesc{table: table, column: label, variable: var}) do
    target_iri = RDF.iri(base_iri_text <> table)
    target_table = tables
    |> Enum.find(fn (tab) ->
      target_iri == tab.__id__
    end)
    case target_table do
      nil -> %Staging{ signal: :invalid_table, variable: var, label: label, target: target_iri   }
      tab -> %Staging{ signal: :valid_table,   variable: var, label: label, target: target_table }
    end
  end

  def sort_scope(%Staging{signal: :valid_table},   %Staging{signal: :invalid_table}), do: true
  def sort_scope(%Staging{signal: :invalid_table}, %Staging{signal: :valid_table}), do: false
  def sort_scope(%Staging{variable: var0},         %Staging{variable: var1}), do: var0 < var1

  defp job_scope_against_tables(base_iri_text, tables, job) do
    run_scope = fn (src) -> src
      |> Enum.map(&compare_table_call(base_iri_text, tables, &1))
      |> Enum.sort(&sort_scope/2)
    end   
    %Spec{
      title:             job.title,
      description:       job.description,
      type:              job.job_type,
      pairs_descriptive: run_scope.(job.job_scope_descriptive),
      pairs_collected:   run_scope.(job.job_scope_collected),
      pairs_modelled:    run_scope.(job.job_scope_modelled)
    }
  end

  def generate_jobs(%ManifestDesc{} = manifest, base_iri_text) do
    processed_jobs = manifest.jobs
    |> Enum.map(&job_scope_against_tables(base_iri_text, manifest.tables, &1))
    %Producer{
      title:        manifest.title,
      description:  manifest.description,
      staging_jobs: processed_jobs
    }    
  end
end
