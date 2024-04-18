defmodule RAP.Manifest.TableDesc do
  
  use Grax.Schema, depth: +5
  import RDF.Sigils
  alias RAP.Vocabulary.{DCTERMS, SAVED}
  
  schema SAVED.TableDesc do
    property :title,         DCTERMS.title,       type: :string
    property :description,   DCTERMS.description, type: :string
    property :resource_path, SAVED.resource_path, type: :string
    property :schema_path,   SAVED.schema_path,   type: :string
    property :resource_hash, SAVED.resource_hash, type: :string
    property :scope,         SAVED.scope,         type: :any
  end
end

defmodule RAP.Manifest.SourceDesc do

  require Logger

  use RDF
  use Grax.Schema, depth: +5
  import RDF.Sigils
  alias RAP.Vocabulary.SAVED
  alias RAP.Manifest.Plumbing

  schema SAVED.SourceDesc do
    property :table, SAVED.table, type: :string, depth: +5
    property :scope, SAVED.scope, type: list_of(:string), from_rdf: :scope_from_rdf
  end
  
  def scope_from_rdf(types, desc, graph) do
    values = types
    |> Enum.flat_map(&Plumbing.follow_subject(&1, graph))
    {:ok, values}
  end

end

defmodule RAP.Manifest.JobDesc do

  use Grax.Schema, depth: +5
  import RDF.Sigils
  alias RAP.Vocabulary.{DCTERMS, SAVED}
  alias RAP.Manifest.SourceDesc

  schema SAVED.JobDesc do
    property :title,             DCTERMS.title,           type: :string
    property :job_type,          SAVED.job_type,          type: :string
    property :job_auto_generate, SAVED.job_auto_generate, type: :boolean

    link job_sources: SAVED.job_sources, type: list_of(SourceDesc), depth: +5
  end
end

defmodule RAP.Manifest.ManifestDesc do

  use Grax.Schema, depth: +2
  import RDF.Sigils
  alias RAP.Vocabulary.SAVED
  alias RAP.Manifest.{TableDesc, JobDesc}

  schema SAVED.ManifestDesc do
    property :local_version, SAVED.local_version

    link tables: SAVED.tables, type: list_of(TableDesc), depth: +5
    link jobs:   SAVED.jobs,   type: list_of(JobDesc), depth: +5
  end
end

defmodule RAP.Manifest.Plumbing do

  use RDF
  import RDF.Sigils
  alias RAP.Vocabulary.SAVED
  alias RAP.Manifest.{TableDesc, JobDesc, SourceDesc, ManifestDesc}

  def update_manifest(%ManifestDesc{tables: tabs, jobs: jobs} = manifest, source_scope) do
    updated_jobs = jobs |> Enum.map(&update_job_desc(&1, source_scope))
    manifest |> Map.put(:jobs, updated_jobs)
  end
  
  defp update_job_desc(%JobDesc{job_sources: sources} = job_desc, source_scope) do
    updated_sources = sources |> Enum.map(&update_source_desc(&1, source_scope))
    job_desc |> Map.put(:job_sources, updated_sources)
  end
  
  defp update_source_desc(%SourceDesc{} = source_desc, source_scope) do
    source_desc |> Map.put(:scope, source_scope)
  end
  
  # The basic idea is resolve the first; rest triple into an actual list
  # sub0: An RDF description. Get its predications pred0.
  # pred0: %{~I<rdf:first> => %{~L<literal> => nil}, ~I<rdf:rest> => %{next_iri}
  # sub1: Graph.fetch(next_iri)
  # Very annoying as pattern-matching was too difficult
  defp get_only_match(struct) do
    struct
    |> Enum.find(fn ({key, _}) -> RDF.Literal.valid?(key) or RDF.IRI.valid?(key) end)
    |> elem(0)
  end
  
  defp kludge_predication(%{term_to_iri(RDF.first) => struct_current_literal,
			   term_to_iri(RDF.rest)  => remainder_struct},
    graph) do
    
    actual_literal = struct_current_literal
    |> get_only_match()
    |> RDF.Literal.value()
    
    case remainder_struct do
      %{term_to_iri(RDF.nil)  => nil} -> [actual_literal]
      other_struct ->
	next_iri = get_only_match other_struct
	[actual_literal] ++ follow_subject(next_iri, graph)
    end
  end

  def follow_subject(nil, _graph), do: nil
  def follow_subject(iri, graph) do
    {:ok, description} = graph |> RDF.Graph.fetch(iri)
    description.predications |> kludge_predication(graph)
  end
end


  #
  
