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

  alias RAP.Manifest.{TableDesc, ColumnDesc, JobDesc, ManifestDesc}
  alias RAP.Storage.{GCP, Monitor}
  alias RAP.Job.{Producer, Staging}
  alias RAP.Job.Spec.{Column, Resource, Table, Job, Manifest}
    
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
  
  defp pretty_print_table(%TableDesc{resource_path: file, schema_path: schema}), do: "#{file.path} (#{schema.path})"
  defp check_table(%TableDesc{} = table, target_dir, resources) do
    table_name    = extract_id(table.__id__)
    target_file   = "#{target_dir}/#{extract_uri table.resource_path}"
    target_schema = "#{target_dir}/#{extract_uri table.schema_path}"
    
    inject = fn (fp, res) ->
      %Resource{path: fp, extant: fp in res}
    end
    resource_validity = target_file   |> inject.(resources)
    schema_validity   = target_schema |> inject.(resources)
    
    %Table{name: table_name, title: table.title, resource: resource_validity, schema: schema_validity}
  end
      
#  defp check_resources(%ManifestDesc{tables: ts} = manifest, target_dir, resources) do     
#     with [_ | _] = proc <- Enum.filter(ts, &paths_extant?(&1, target_dir, resources)),
#  	 {proc, true}   <- {proc, length(proc) == length(ts)} do
#       Logger.info "All files in #{target_dir} were valid"
#       {:ok, :valid_tables, manifest}
#     else
#       [] ->
#  	Logger.info "No files in #{target_dir} were valid"
#         {:error, :invalid_tables}
#       {proc, false} ->
#  	pretty_valid = proc |> Enum.map(&pretty_print_table/1) |> inspect()
#         Logger.info "Some files in #{target_dir} were valid: #{pretty_valid}"
#  	processed_manifest = manifest |> Map.put(:tables, proc)
# 	{:error, :invalid_tables, processed_manifest}
#     end
#    end

  @doc """
  This is somewhat problematic semantically.

  The assumption is that we trying to match local, cached, files, but
  the field is actually a URI and it's feasible that these will point to
  network resources.

  So, we just want the table name, here!

  There are basically two forms. The first one is just a path, this
  points to a local file and will likely be the correct usage. The second
  form has the `scheme', `host' and `path' fields filled out, with `http'
  or `https as scheme, the host being something like `marine.gov.scot',
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
  
  defp check_column(%ColumnDesc{table: tab, column: col, variable: var}, table_names) do
    target_table = extract_uri(tab)
    underlying   = extract_uri(var)
    column       = extract_uri(col)
    staging      = %Column{column: column, variable: underlying, table: target_table}
    if target_table in table_names do
      { :valid,  staging }
    else
      Logger.info "Column description referenced table #{target_table} does not exist in tables (#{inspect table_names})"
      { :invalid, staging }
    end
  end

  defp group_columns(annotated_labels, table_names) do
    grouped = annotated_labels
    |> Enum.map(&check_column(&1, table_names))
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Map.put_new(:valid, [])
    |> Map.put_new(:invalid, [])
  end

  @doc """
  This is largely the same as the column-level errors. The main thing
  which we want to do is to preserve both the error columns and the non-
  error columns, and to be able to warn, or issue errors which highlight
  any errors *which are applicable*.
  """
  defp check_job(%JobDesc{} = job, table_names) do
    Logger.info "Checking job"
    res_descriptive = job.job_scope_descriptive |> group_columns(table_names)
    res_collected   = job.job_scope_collected   |> group_columns(table_names)
    res_modelled    = job.job_scope_modelled    |> group_columns(table_names)

    Logger.info "Found descriptive results: #{inspect res_descriptive}"
    Logger.info "Found collected results: #{inspect res_collected}"
    Logger.info "Found modelled results: #{inspect res_modelled}"
    
    %{valid: scope_descriptive, invalid: errors_descriptive} = res_descriptive
    %{valid: scope_collected,   invalid: errors_collected}   = res_collected
    %{valid: scope_modelled,    invalid: errors_modelled}    = res_modelled
    generated_job = %Job{
      title:             job.title,
      description:       job.description,
      type:              job.job_type,
      scope_descriptive: scope_descriptive, errors_descriptive: errors_descriptive,
      scope_collected:   scope_collected,   errors_collected:   errors_collected,
      scope_modelled:    scope_modelled,    errors_modelled:    errors_modelled,
    }
    Logger.info "Generated job: #{inspect generated_job}"
    generated_job
  end
  
#   defp check_job(%JobDesc{} = job, tables) do
#     Logger.info "Check job #{inspect job.title} (type #{extract_uri job.job_type})"
#     good_col? = fn
#       {:valid,   _col} -> true
#       {:invalid, _col} -> false
#     end
#     get_col = fn
#       k, :valid   -> k |> elem(0) |> Enum.map(&elem(&1, 1))
#       k, :invalid -> k |> elem(1) |> Enum.map(&elem(&1, 1))
#     end
#     
#     process_columns = fn (cols) ->
#       cols
#       |> Enum.map(&check_column(&1, tables))
#       |> Enum.split_with(good_col?)
#     end
#     
#     pairs_desc = process_columns.(job.job_scope_descriptive)
#     pairs_coll = process_columns.(job.job_scope_collected)
#     pairs_mod  = process_columns.(job.job_scope_modelled)
#     Logger.info "Descriptive pairs: #{inspect pairs_desc}"
#     Logger.info "Collected pairs: #{inspect pairs_coll}"
#     Logger.info "Modelled pairs: #{inspect pairs_mod}"
#     
#     #revised_job = job
#     #|> Map.put(:job_scope_descriptive, get_col.(pairs_desc, :ok))
#     #|> Map.put(:job_scope_collected,   get_col.(pairs_coll, :ok))
#     #|> Map.put(:job_scope_modelled,    get_col.(pairs_mod,  :ok))
#     
#     #%{job: revised_job,
#     #  errors_descriptive: get_col.(pairs_desc, :error),
#     #  errors_collected:   get_col.(pairs_coll, :error),
#     #  errors_modelled:    get_col.(pairs_mod,  :error)}
  #   end


  defp check_manifest(%ManifestDesc{} = desc, target_dir, manifest_path, resources) do
    Logger.info "Check manifest #{desc.title}"

    Logger.info "Working on tables: #{inspect desc.tables}"
    Logger.info "Working on jobs: #{inspect desc.jobs}"
    
    processed_tables = desc.tables |> Enum.map(&check_table(&1, target_dir, resources))
    table_names = processed_tables |> Enum.map(& &1.name)
    processed_jobs   = desc.jobs   |> Enum.map(&check_job(&1, table_names))
    
    %Manifest{
      title:          desc.title,
      description:    desc.description,
      local_version:  desc.local_version,
      manifest_path:  manifest_path,
      resources:      resources,
      staging_tables: processed_tables,
      staging_jobs:   processed_jobs
    }
  end
  

#   defp check_manifest(%ManifestDesc{tables: tables, jobs: jobs} = manifest) do
#     Logger.info "Check manifest #{manifest.title}"
#     processed_jobs = jobs
#     |> Enum.map(&check_job(&1, tables))
# 
#     revised_jobs = processed_jobs
#     |> Enum.map(& &1.job)
# 
#     is_error = fn (k) ->
#       length(k.errors_descriptive) > 0 or
#       length(k.errors_collected)   > 0 or
#       length(k.errors_modelled)    > 0 end
# 
#     job_errors = processed_jobs
#     |> Enum.filter(is_error)
#     
#     revised_manifest = manifest |> Map.put(:jobs, revised_jobs)
# 
#     %{manifest: revised_manifest, job_errors: job_errors}
#   end

  def invoke_manifest(%GCP{uuid: uuid, manifest: manifest_path, resources: resources}, cache_dir) do
    target_dir = "#{cache_dir}/#{uuid}"
    Logger.info "Building RDF graph from turtle manifest using data in #{target_dir}"
    with {:ok, rdf_graph} <- RDF.Turtle.read_file(manifest_path),
         {:ok, ex_struct} <- Grax.load(rdf_graph, RAP.Vocabulary.RAP.RootManifest, ManifestDesc),
         {:ok, non_empty} <- check_skeleton(ex_struct),
         manifest <- check_manifest(non_empty, target_dir, manifest_path, resources)
      do
      #Logger.info "Detecting feasible jobs"
      #Logger.info "I found a manifest: #{inspect processed}"
      #Logger.info "I found job errors: #{inspect job_errors}"
      manifest
    else
      {:error, err} ->
	Logger.info "Could not read RDF graph #{manifest_path}"
        Logger.info "Error was #{inspect err}"
	{:error, :input_graph}
      {:error, :empty, _manifest} ->
	Logger.info "Generated manifest was empty!"
	{:error, :output_struct}
      error -> error
    end
  end
  defp check_skeleton(%ManifestDesc{description: nil, title:         nil,
				     tables:      [],  jobs:          [],
				     gcp_source:  nil, local_version: nil
				    } = manifest), do: {:error, :empty, manifest}
  defp check_skeleton(%ManifestDesc{} = manifest), do: {:ok, manifest}
  
  

end
