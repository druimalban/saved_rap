defmodule RAP.Job.Producer do
  @moduledoc """
  This is the job producer stage of the RAP. Strictly speaking, the
  producer stage of the RAP proper is the monitoring of the storage
  backend. Given a turtle manifest path, attempt to read this into an RDF
  graph, and then load it into a struct.

  There are a few checks that we need to do before we can decide to run a
  job. These are as follows:

  1. Read the manifest, inject it into the ManifestDesc struct and check
     it's non-empty. There are circumstances in which Grax.load generates
     an empty struct, e.g. when the name is valid in the vocabulary and
     certain fields are not required.     

  2. Check that the tables section references real tables, or at least
     what we've been able to find in the cache directory under the
     manifest UUID.

  3. Check that the tables referenced in the columns section are real
     tables. The columns also have a notion of an underlying variable
     which we may want to check later, but it's better to let this be
     overridden for now because we only care about pattern matching and
     don't peer into the structure, although warning that these don't
     exist may be productive later.

  4. Good error messages and return behaviour help us to correct issues
     with our job manifests so it is important to have a way to compare
     good and bad table / column scope at the end. This isn't especially
     elegant as the errors need to be included in the function return
     values, so that it's clear at the end of the stage what, if anything
     went wrong.
  """

  alias RAP.Manifest.{TableDesc, ScopeDesc, JobDesc, ManifestDesc}
  alias RAP.Storage.GCP
  alias RAP.Job.{ScopeSpec, ResourceSpec, TableSpec, JobSpec, ManifestSpec}
    
  use GenStage
  require Logger

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
  
  @doc """
  This is somewhat problematic semantically.

  The assumption is that we trying to match local, cached, files, but
  the field is actually a URI and it's feasible that these will point to
  network resources.

  So, we just want the table name, here!

  There are basically two forms. The first one is just a path, this
  points to a local file and will likely be the correct usage. The second
  form has the `scheme', `host' and `path' fields filled out, with `http'
  or `https' as scheme, the host being something like `marine.gov.scot',
  and the path being the actual URI on their webroot like
  `/metadata/saved/rap/job_table_sampling'.

  If we were doing this properly, what we would do instead of this is to
  check that the reassembled URI portion matches the prefix before the
  file name in question. If not, error or warn because this does not
  actually match the local declaration, and there's (very likely) no such
  resource available, given it's highly specific to the given manifest
  file.
  """
  defp extract_uri(%URI{path: path, scheme: nil, userinfo: nil,
			host: nil,  port:   nil, query:    nil,
			fragment: nil}), do: path
  defp extract_uri(%URI{path: path}) do
    path |> String.split("/") |> Enum.at(-1)
  end
  defp extract_id(id) do
    id
    |> RDF.IRI.to_string()
    |> String.trim("/")
    |> String.split("/")
    |> Enum.at(-1)
  end

  defp check_table(%TableDesc{} = desc, resources) do

    table_name  = extract_id(desc.__id__)
    table_title = desc.title
    Logger.info "Checking table #{inspect table_name}"
    
    inject = fn fp -> 
      %ResourceSpec{ base: fp, extant: fp in resources }
    end
    data_validity   = desc.resource_path   |> extract_uri() |> then(inject)
    schema_validity = desc.schema_path_ttl |> extract_uri() |> then(inject) ## TO-DO: ADD YAML
    
    %TableSpec{ name:     table_name,    title:  table_title,
		resource: data_validity, schema: schema_validity }
  end

  @doc """
  With the new data structure, the table is implictly valid, so we just
  need to extract the information of interest (path to table).
  Nonetheless, may consider a possible error case where this is nil. 
  """
  defp check_column(%ScopeDesc{column:   column,
                               variable: underlying,
                               table: %RAP.Manifest.TableDesc{
				        __id__:        table_id,
					resource_path: resource_uri
                               }}) do
    %ScopeSpec{
      column:         column,
      variable_uri:   RDF.IRI.to_string(underlying.__id__),
      variable_curie: underlying.compact_uri,
      resource_name:  extract_id(table_id),
      resource_base:  extract_uri(resource_uri)
    }
  end

 @doc """
  This is largely the same as the column-level errors. The main thing
  which we want to do is to preserve both the error columns and the non-
  error columns, and to be able to warn, or issue errors which highlight
  any errors *which are applicable*.
  """
  defp check_job(%JobDesc{} = desc, _tables) do
    sub = fn(%ScopeSpec{variable_curie: var0}, %ScopeSpec{variable_curie: var1}) ->
     var0 < var1
    end

    job_name = extract_id(desc.__id__)
    scope_descriptive = desc.job_scope_descriptive |> Enum.map(&check_column/1) |> Enum.sort(sub)
    scope_collected   = desc.job_scope_collected   |> Enum.map(&check_column/1) |> Enum.sort(sub)
    scope_modelled    = desc.job_scope_modelled    |> Enum.map(&check_column/1) |> Enum.sort(sub)

    generated_job = %JobSpec{
      name:              job_name,
      type:              desc.job_type,
      title:             desc.title,
      description:       desc.description,
      scope_descriptive: Enum.sort(scope_descriptive, sub),
      scope_collected:   Enum.sort(scope_collected, sub),
      scope_modelled:    Enum.sort(scope_modelled, sub)
    }
    Logger.info "Generated job: #{inspect generated_job}"
    generated_job
  end

  defp check_manifest(%ManifestDesc{description: nil, title: nil,
				    tables:      [],  jobs:  [],
				    gcp_source:  nil, local_version: nil},
                      _uuid, _manifest, _resources) do
    {:error, :empty}
  end
  defp check_manifest(%ManifestDesc{} = desc, uuid, manifest_base, resources) do
    Logger.info "Check manifest (title #{inspect desc.title})"
    Logger.info "Working on tables: #{inspect desc.tables}"
    Logger.info "Working on jobs: #{inspect desc.jobs}"

    test_extant = fn tab ->
      res = tab.resource
      sch = tab.schema
      res.extant and sch.extant
    end

    processed_tables  = desc.tables |> Enum.map(&check_table(&1, resources))
    extant_tables     = processed_tables |> Enum.filter(test_extant)
    non_extant_tables = processed_tables |> Enum.reject(test_extant)

    if length(processed_tables) != length(extant_tables) do
	{:error, :bad_tables, non_extant_tables}
    else
      processed_jobs = desc.jobs |> Enum.map(&check_job(&1, extant_tables))
      
      manifest_obj = %ManifestSpec{
	title:         desc.title,
	description:   desc.description,
	local_version: desc.local_version,
	uuid:          uuid,
	manifest_base: manifest_base,
	resource_bases: resources,
	staging_tables: processed_tables,
	staging_jobs:   processed_jobs
      }
      {:ok, manifest_obj}
    end
  end

  def invoke_manifest(%GCP{uuid: uuid, manifest: manifest_base, resources: resources}, cache_dir) do
    Logger.info "Building RDF graph from turtle manifest using data in #{cache_dir}/#{uuid}"
    manifest_full_path = "#{cache_dir}/#{uuid}/#{manifest_base}"
    with {:ok, rdf_graph}    <- RDF.Turtle.read_file(manifest_full_path),
         {:ok, ex_struct}    <- Grax.load(rdf_graph, RAP.Vocabulary.RAP.RootManifest, ManifestDesc),
         {:ok, manifest_obj} <- check_manifest(ex_struct, uuid, manifest_base, resources)
      do
        Logger.info "Detecting feasible jobs"
	Logger.info "Found RDF graph:"
	Logger.info "#{inspect rdf_graph}"
	Logger.info "Corresponding struct to RDF graph:"
	Logger.info "#{inspect ex_struct}"
	Logger.info "Processed/annotated manifest:"
	Logger.info "#{inspect manifest_obj}"
	manifest_obj
    else
      {:error,   err} ->
	Logger.info "Could not read RDF graph #{manifest_full_path}"
        Logger.info "Error was #{inspect err}"
	{:error, :input_graph}
      {:error,   :empty} ->
	Logger.info "Corresponding struct to RDF graph was empty!"
	{:error, :output_struct}
      message ->
	message
    end
  end 
  
end
