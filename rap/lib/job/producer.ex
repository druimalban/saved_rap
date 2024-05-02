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

  alias RAP.Vocabulary.SAVED
  alias RAP.Manifest.{TableDesc, ColumnDesc, JobDesc, ManifestDesc}
  alias RAP.Storage.{GCP, Monitor}
  alias RAP.Job.{Producer, Spec, Staging}
  
  use GenStage
  require Logger

  import :timer, only: [ sleep: 1 ]

  defstruct [ :title, :description, :staging_jobs ]

  def start_link _args do
    Logger.info "Called Job.Producer.start_link (_)"
    initial_state = []
    GenStage.start_link __MODULE__, initial_state, name: __MODULE__
  end

  def init initial_state do
    Logger.info "Called Job.Producer.init (initial_state = #{inspect initial_state})"
    subscription = [
      # Fix this using Storage.Monitor -> Storage.GCP/Storage.Local stages
      { Monitor, min_demand: 0, max_demand: 1 }
    ]
    { :producer_consumer, initial_state, subscribe_to: subscription }
  end
  
  def handle_events events, _from, state do
    ie = inspect events
    is = inspect state
    Logger.info "Called Job.Producer.handle_events (events = #{ie}, _, state = #{is})"

    # Fix once we've got the GCP stuff nailed down
    gcp_events = []
    #ne = events |> Enum.map(&process_jobs/1)
    { :noreply, gcp_events, state }
  end


  
  def start_link initial_ts do
    Logger.info "Called Job.Producer.start_link (initial_ts = #{inspect initial_ts})"
    GenStage.start_link __MODULE__, initial_ts, name: __MODULE__
  end

  def init initial_ts do
    Logger.info "Called Job.Producer.init (initial_ts = #{inspect initial_ts})"
    :ets.new(:uuid, [:set, :public, :named_table])
    { :producer_consumer, initial_ts }
  end
  
  def handle_demand demand, ts do
    insd = inspect demand
    inss = inspect ts
    events = []
    Logger.info "Called Job.Producer.handle_demand (demand = #{insd}, state = #{inss})"
    { :noreply, events, ts }
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
    {:noreply, [feasible_jobs], state}
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

  def sort_scope(%Staging{signal: :valid_table}, %Staging{signal: :invalid_table}), do: true
  def sort_scope(%Staging{signal: :invalid_table}, %Staging{signal: :valid_table}), do: false
  def sort_scope(%Staging{variable: var0}, %Staging{variable: var1}), do: var0 < var1

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

  def generate_jobs(base_iri_text, %ManifestDesc{} = manifest) do
    processed_jobs = manifest.jobs
    |> Enum.map(&job_scope_against_tables(base_iri_text, manifest.tables, &1))
    %Producer{
      title:        manifest.title,
      description:  manifest.description,
      staging_jobs: processed_jobs
    }    
  end
end
