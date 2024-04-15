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
  defstruct [ :title, :type, :auto_generate, :state ]

  alias RAP.Vocabulary.SAVED
  alias RAP.Manifest.{TableDesc,SourceDesc,JobDesc,ManifestDesc}
  
  use GenStage
  require Logger

  def start_link initial_state do
    Logger.info "Called Job.Producer.start_link (initial_state = #{inspect initial_state})"
    GenStage.start_link __MODULE__, initial_state, name: __MODULE__
  end

  def init initial_state do
    Logger.info "Called Job.Producer.init (initial_state = #{inspect initial_state})"
    { :producer, initial_state }
  end
  
  def handle_demand demand, state do
    insd = inspect demand
    inss = inspect state
    events = []
    Logger.info "Called Job.Producer.handle_demand (demand = #{insd}, state = #{inss})"
    { :noreply, events, state }
  end

  @doc """
  Generic helper function which sends the :try_jobs signal. This is
  a place-holder which lets us manually trigger a job, since the aim is
  to monitor GCP / other storage backend for new manifests to run. This
  will probably be performed by an earlier stage.
  """
  def trigger manifest_path do
    Logger.info "Called Job.Producer.trigger ()"
    #({ #{inspect job_code} ,#{inspect col0} ,#{inspect col1} })"
    GenStage.cast __MODULE__, {:try_jobs, manifest_path}
  end

  @doc """
  Given the :try_jobs signal, attempt to devise a set of jobs which are
  to be run.
  """
  def handle_cast {:try_jobs, manifest_path}, state do
    Logger.info "Received :try_jobs signal"
    Logger.info "Building RDF graph from turtle manifest #{manifest_path}"
    {:ok, graph}  = RDF.Turtle.read_file manifest_path

    Logger.info "Loading RDF graph into Elixir/Grax structs"
    {:ok, struct} = Grax.load graph, SAVED.RootManifest, ManifestDesc

    Logger.info "Detecting feasible jobs"
    base_iri  = RDF.IRI.to_string graph.base_iri
    feasible_jobs = generate_jobs base_iri, struct
    {:noreply, feasible_jobs, state}
  end

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
  def generate_jobs(base_iri, %RAP.Manifest.ManifestDesc{jobs: jobs, tables: tables} = manifest) do
    jobs |> Enum.map(&job_sources_against_tables(base_iri, tables, &1))
         |> Enum.map(&job_scope_against_tables/1)
  end

  defp compare_table_call(base_iri, tables, source) do
    target_iri   = RDF.iri(base_iri <> source.table)
    target_table = tables |> Enum.find(fn (tab) ->
      target_iri == tab.__id__
    end)
    case target_iri do
      nil -> { :invalid_table, source, target_iri }
      tab -> { :valid_table,   source, target_table }
    end
  end
  
  defp job_sources_against_tables(base_iri, tables, job) do
    paired_iris = job.job_sources |> Enum.map(&compare_table_call(base_iri, tables, &1))
    %RAP.Job.Producer{
      title: job.title, type: job.job_type, auto_generate: job.job_auto_generate,
      state: paired_iris
    }
  end

  defp compare_scope_pair({:invalid_table, _src, _iri} = source), do: source
  defp compare_scope_pair({:valid_table, src, table}) do
    scope_table     = MapSet.new(table.scope)
    scope_source    = MapSet.new(src.scope)
    
    valid_columns   = MapSet.intersection(scope_source, scope_table) |> MapSet.to_list()
    invalid_columns = MapSet.difference(scope_source, scope_table)   |> MapSet.to_list()
    
    %{ source: src, table: table, valid_columns: valid_columns, invalid_columns: invalid_columns }
  end

  defp job_scope_against_tables(%RAP.Job.Producer{} = job_state) do
    compared_scopes = job_state.state |> Enum.map(&compare_scope_pair/1)
    Map.put job_state, :state, compared_scopes
  end
  
end
