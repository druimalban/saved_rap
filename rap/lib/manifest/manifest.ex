defmodule RAP.Manifest.TableDesc do
  
  use Grax.Schema, depth: +5
  import RDF.Sigils
  alias RAP.Vocabulary.{DCTERMS, SAVED}
  
  schema SAVED.TableDesc do
    property :title,            DCTERMS.title,          type: :string,  required: false
    property :description,      DCTERMS.description,    type: :string,  required: false
    property :resource_path,    SAVED.resource_path,    type: :any_uri, required: true
    property :schema_path_yaml, SAVED.schema_path_yaml, type: :any_uri, required: true
    property :schema_path_ttl,  SAVED.schema_path_ttl,  type: :any_uri, required: false
    property :resource_hash,    SAVED.resource_hash,    type: :string,  required: true
  end
end

defmodule RAP.Manifest.ExtColumnDesc do
  @moduledoc """
  This is a placeholder for IRIs which need to be annotated, which
  avoids having to pass the loaded RDF graph along the pipeline.

  The Grax RDF tooling already has an :iri type, which should be used
  instead of this data structure. When we change/revamp the RDF generated
  by fisdat/fisup, so that it has a distinct notion of a resource with a
  file type, this ought to be removed and treated just as a simple IRI
  corresponding to that resource description.
  """
  use Grax.Schema, depth: +5
  import RDF
  alias RAP.Manifest.ExtColumnDesc
  alias RAP.Vocabulary.{DCTERMS, SAVED}

  schema SAVED.ExtColumnDesc do
    field :compact_uri
  end

  def on_load(column_desc, graph, _opts) do
    curie = RDF.PrefixMap.prefixed_name(graph.prefixes, column_desc.__id__)
    new_desc = column_desc |> Map.put(:compact_uri, curie)
    {:ok, new_desc}
  end
  
end

defmodule RAP.Manifest.ScopeDesc do

  require Logger
  
  use Grax.Schema, depth: +5
  import RDF.Sigils
  alias RAP.Vocabulary.SAVED
  alias RAP.Manifest.{ExtColumnDesc, TableDesc}

  schema SAVED.ScopeDesc do
    
    property :column,   SAVED.column,   type: :string, required: true

    link variable: SAVED.variable, type: ExtColumnDesc, depth: +5
    link table:    SAVED.table,    type: TableDesc, depth: +5
  end
  
end

defmodule RAP.Manifest.JobDesc do

  use Grax.Schema, depth: +5
  import RDF.Sigils
  alias RAP.Vocabulary.{DCTERMS, SAVED}
  alias RAP.Manifest.{JobDesc, ScopeDesc}

  schema SAVED.JobDesc do
    property :title,       DCTERMS.title,       type: :string,  required: false
    property :description, DCTERMS.description, type: :string,  required: false
    property :job_type,    SAVED.job_type,      type: :string,  required: true
    
    link job_scope_descriptive: SAVED.job_scope_descriptive, type: list_of(ScopeDesc), depth: +5
    link job_scope_collected:   SAVED.job_scope_collected,   type: list_of(ScopeDesc), depth: +5
    link job_scope_modelled:    SAVED.job_scope_modelled,    type: list_of(ScopeDesc), depth: +5

    field :job_result_format
    field :job_result_extension
    field :job_result_stem
  end

  def on_load(%JobDesc{job_type: job_type} = job_desc, _graph, _opts) do
    {fmt, ext, stem} =
      case job_type do
	"density" -> {"text/json", "json", "result_density"}
	_         -> {"text/plain", "txt", "result_misc"}
      end
    new_desc = %{ job_desc |
		  job_result_format:    fmt,
		  job_result_extension: ext,
		  job_result_stem:      stem}
    {:ok, new_desc}
  end
end

defmodule RAP.Manifest.ManifestDesc do

  use Grax.Schema, depth: +2
  import RDF.Sigils
  alias RAP.Vocabulary.{DCTERMS, SAVED}
  alias RAP.Manifest.{TableDesc, JobDesc}

  schema SAVED.ManifestDesc do
    property :title,         DCTERMS.title,       type: :string, required: false, required: false
    property :description,   DCTERMS.description, type: :string, required: false, required: false
    property :local_version, SAVED.local_version, type: :string, required: false, required: true
    # property :gcp_source,    SAVED.gcp_source,    type: :string, required: false, required: false

    link tables: SAVED.tables, type: list_of(TableDesc), depth: +5, required: true
    link jobs:   SAVED.jobs,   type: list_of(JobDesc),   depth: +5, required: true
  end
end

####################
# BEGIN OLD FORMAT #
####################
#
# This is the old format, keep it around for reference since we may
# depend on collections and/or containers later.
# 
#defmodule RAP.Manifest.SourceDesc do
#
#  require Logger
#
#  use RDF
#  use Grax.Schema, depth: +5
#  import RDF.Sigils
#  
#  alias RAP.Manifest.{Plumbing, SourceDesc}
#  alias RAP.Vocabulary.SAVED
#
#  schema SAVED.SourceDesc do
#    property :table, SAVED.table, type: :string, depth: +5
#    property :scope, SAVED.scope, type: list_of(:string), from_rdf: :scope_from_rdf
#  end
#
#  def scope_from_rdf(types, _desc, graph) do
#    values = types
#    |> Enum.flat_map(&Plumbing.follow_subject(&1, graph))
#    {:ok, values}
#  end
#
#  def on_to_rdf(%SourceDesc{} = source_desc, _graph, _opts) do
#    Plumbing.expand_subject(source_desc.__id__.value, source_desc.scope, source_desc.table)
#  end
#end
#defmodule RAP.Manifest.Plumbing do
#
#  use RDF
#  import RDF.Sigils
#  alias RDF.{Graph,NS}
#  
#  alias RAP.Vocabulary.SAVED
#  alias RAP.Manifest.{SourceDesc, JobDesc, ManifestDesc}
#  
#  # The basic idea is resolve the first; rest triple into an actual list
#  # sub0: An RDF description. Get its predications pred0.
#  # pred0: %{~I<rdf:first> => %{~L<literal> => nil}, ~I<rdf:rest> => %{next_iri}
#  # sub1: Graph.fetch(next_iri)
#  # Very annoying as pattern-matching on the key is not possible
#  defp get_only_match(struct) do
#    struct
#    |> Map.to_list()
#    |> List.first()
#    |> elem(0)
#  end
#  
#  defp kludge_predication(%{term_to_iri(RDF.first) => struct_current_literal,
#			    term_to_iri(RDF.rest)  => remainder_struct},
#    graph) do
#    
#    actual_literal = struct_current_literal
#    |> get_only_match()
#    |> RDF.Literal.value()
#    
#    case remainder_struct do
#      %{term_to_iri(RDF.nil)  => nil} -> [actual_literal]
#      other_struct ->
#	next_iri = get_only_match other_struct
#	[actual_literal] ++ follow_subject(next_iri, graph)
#    end
#  end
#
#  def follow_subject(nil, _graph), do: nil
#  def follow_subject(iri, graph) do
#    {:ok, description} = graph |> RDF.Graph.fetch(iri)
#    description.predications   |> kludge_predication(graph)
#  end
#  
#  defp expand_source(resource, columns, table) do
#    srv = "http://localhost/saved/"
#    len = inspect(length(columns))
#    
#    source_iri    = RDF.IRI.new(srv <> "source_" <> resource)
#    source_triple = RDF.triple(source_iri, RDF.type(), SAVED.SourceDescDesc)
#
#    table_triple  = RDF.triple(source_iri, SAVED.table, table)
#    
#    scope_iri     = RDF.IRI.new(srv <> "scope_" <> resource <> "_0")
#    scope_triple  = RDF.triple(source_iri, SAVED.scope, scope_iri)
#
#    Graph.new(source_triple)
#    |> Graph.add(table_triple)
#    |> Graph.add(scope_triple)
#    |> expand_scope(resource, columns, 0)
#  end
#  
#  defp expand_scope(graph, resource, [column], incr) do
#    iri = RDF.IRI.new("http://localhost/saved/scope_" <> resource <> "_" <> inspect(incr))
#    graph
#    |> Graph.add(
#      iri
#      |> RDF.first(column)
#      |> RDF.rest(RDF.nil)
#    )
#  end
#  defp expand_scope(graph, resource, [head | tail] = columns, incr) do
#    srv = "http://localhost/saved/scope_" <> resource <> "_"
#    iri      = RDF.IRI.new(srv <> inspect(incr))
#    next_iri = RDF.IRI.new(srv <> inspect(incr+1))
#    graph
#    |> Graph.add(
#      iri
#      |> RDF.first(head)
#      |> RDF.rest(next_iri)
#    )
#    |> Graph.add(
#      expand_scope(graph, resource, tail, incr+1)
#    )
#  end
#
#  def expand_subject("http://localhost/saved/source_" <> resource, scope, table) do
#    {:ok, expand_source(resource, scope, table)}
#  end
#  def expand_subject("http://localhost/saved/" <> resource, scope, table) do
#    {:ok, expand_source(resource, scope, table)}
#  end
#  
#end
#
##################
# END OLD FORMAT #
##################
