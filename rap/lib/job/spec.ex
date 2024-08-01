defmodule RAP.Job.ScopeSpec do

  @moduledoc """
  Simple struct for recording whether a given column is valid.
  There exists a mapping from this representation to an original
  `%ScopeDesc{}' struct.

  In the original usage of this (I had called this `Staging'), I had a
  struct attribute called `target'. This had two uses: the IRI of the
  target table, if that table did not exist; or the description of the
  table proper if it did exist. This dual use is really confusing, so 
  I've renamed it `table'.

  I further had a struct attribute called `signal' which I used to record
  whether the table exists or not. This is actually implicit, and is
  recorded at the above level by the job in the `errors_descriptive'
  (&c.) attribute, so don't duplicate this.

  In the generated manifests, column descriptions are blank nodes.

  This struct does duplicate the purported path (base name, to be exact)
  of the referenced table. This is necessary for pattern matching over
  processed job descriptions, and does not occur in our RDF manifests.

  There are both a URI
  (e.g., `https://marine.gov.scot/metadata/saved/column_descriptive')
  and CURIE (e.g. `saved:column_descriptive') fields. Resolving the URI
  into a CURIE is useful for reporting, i.e. use this in generated HTML
  documents instead of the full URI. Doing this robustly involves
  querying an RDF graph's prefix map, so we add a private field to the
  Grax struct which is filled out on loading the RDF graph using Grax.

  In addition to the base name of the resource file, further record th
  atomic name of the table, which is useful for generating reporting.
  """

  use Grax.Schema, depth: +5
  import RDF.Sigils
  alias RAP.Vocabulary.SAVED
  alias RAP.Job.TableSpec
  
  schema SAVED.ScopeOutput do
    property :column,      SAVED.column,   type: :string
    property :variable_id, SAVED.variable, type: :iri

    link table: SAVED.table, type: TableSpec

    field :variable_curie
    field :resource_name
    field :resource_base
  end
  
end

defmodule RAP.Job.ResourceSpec do
  @moduledoc """
  Slightly different to the above in that the validity should be defined
  on the file level. There's only one resource and schema per table,
  unlike columns where there's a bunch of columns per job from many
  places.

  In the generated manifests, this level of nesting does not exist
  because the fields are arbitrary, it's only in the Elixir RAP that we
  actually validate these. These are just named pairs of the resource
  path and whether the resource exists.
  """
  
  use Grax.Schema, depth: +5
  import RDF.Sigils
  alias RAP.Vocabulary.{DCTERMS, DCAT, PROV, PAV, SAVED}

  schema SAVED.ResourceOutput do
    property :created_with, PAV.createdWith, type: :iri
    property :created_on,   PAV.createdOn,   type: :integer
    property :resource_title,       DCTERMS.title,       type: :string
    property :resource_description, DCTERMS.description, type: :string
    property :license,              DCTERMS.license,     type: :iri
    property :download_url,         DCAT.downloadURL, type: :iri
    #property :media_type,           DCAT.mediaType,   type: :iri
    property :output_format,        DCAT.mediaType, type: :string

    field :base
    field :extant
  end
end


defmodule RAP.Job.TableSpec do
  @defmodule """
  The `resource' and `schema' attributes are named resource structs as
  above, analoguous to columns. 

  However, unlike columns, there must be at most one resource file and
  one schema file (ignoring multiple formats of schemata, LinkML YAML
  and generated RDF equivalent), so in terms of pattern matching, it is
  not desirable to have multiple attributes like `invalid_resource',
  `valid_resource', `invalid_schema', `valid_schema'.

  Unlike the columns, which are blank nodes, tables are named and so can
  be associated with a name (the struct's generated `__id__' attribute).

`  The `resource' and `schema' attributes are not fully qualified file
  names or base file names, but instances of the above `%ResourceSpec{}'
  struct.
  """

  use Grax.Schema, depth: +5
  import RDF.Sigils
  alias RAP.Vocabulary.{DCTERMS, DCAT, PROV, PAV, SAVED}
  alias RAP.Job.Producer
  alias RAP.Job.ResourceSpec

  schema SAVED.TableOutput do
    property :title,           DCTERMS.title,       type: :string
    property :description,     DCTERMS.description, type: :string
    property :submitted_table, PROV.wasDerivedFrom, type: :iri # :source_id above
    link resource:    SAVED.resource,     type: ResourceSpec, depth: +5
    link schema_ttl:  SAVED.schema_ttl,   type: ResourceSpec, depth: +5
    link schema_yaml: SAVED.schema_yaml,  type: ResourceSpec, depth: +5
  end
  
end

defmodule RAP.Job.JobSpec do

  use Grax.Schema, depth: +5
  import RDF.Sigils
  alias RAP.Vocabulary.{DCTERMS, PAV, PROV, SAVED}
  alias RAP.Job.ScopeSpec

  schema SAVED.JobOutput do
    property :title,         DCTERMS.title,       type: :string
    property :description,   DCTERMS.description, type: :string
    property :created_with,  PAV.createdWith,     type: :iri
    property :type,          SAVED.job_type,      type: :string
    property :submitted_job, PROV.wasDerivedFrom, type: :iri # :source_id above
    link scope_descriptive: SAVED.job_scope_descriptive, type: list_of(ScopeSpec), depth: +5
    link scope_collected:   SAVED.job_scope_collected,   type: list_of(ScopeSpec), depth: +5
    link scope_modelled:    SAVED.job_scope_modelled,    type: list_of(ScopeSpec), depth: +5
    field :result_format
    field :result_extension
    field :result_stem
  end
end

defmodule RAP.Job.ManifestSpec do

  use Grax.Schema, depth: +5
  import RDF.Sigils
  alias RAP.Vocabulary.{DCTERMS, DCAT, PROV, PAV, SAVED}
  alias RAP.Manifest.{TableDesc, JobDesc}
  alias RAP.Job.{TableSpec, JobSpec, Result}
  alias RAP.Provenance.{RAPProcess, RAPInvocation, RAPStageProcessing}
  #alias RAP.Manifest.{TableDesc, JobDesc}

  schema SAVED.ManifestOutput do
    property :uuid,               SAVED.uuid,           type: :string
    property :data_source,        SAVED.data_source,    type: :string
    property :title,              DCTERMS.title,        type: :string
    property :description,        DCTERMS.description,  type: :string
    property :output_format,      DCAT.mediaType,       type: :string
    property :download_url,       DCAT.downloadURL,     type: :iri
    property :submitted_manifest, PROV.wasDerivedFrom,  type: :iri
    property :ended_at,           PROV.generatedAtTime, type: :date_time
    link tables:  SAVED.tables,  type: list_of(TableSpec),  depth: +5
    link jobs:    SAVED.jobs,    type: list_of(JobSpec),    depth: +5
    link results: SAVED.results, type: list_of(Result),     depth: +5
    link rap_app:         SAVED.rap_application,      type: RAPProcess,    depth: +5
    link rap_app_init:    SAVED.rap_application_init, type: RAPInvocation, depth: +5
    link rap_stages:      SAVED.rap_stages,      type: list_of(RAPProcess),         depth: +5
    link rap_stages_init: SAVED.rap_stages_init, type: list_of(RAPInvocation),      depth: +5
    link rap_processing:  SAVED.rap_processing,  type: list_of(RAPStageProcessing), depth: +5
    
    # semantically, start/end times largely don't make sense for this, an entity,
    # but they do in the potted summary in HTML
    # likewise, signal is derived from the LAST stage signal of relevance,
    # which is hard to model
    field :started_at
    field :start_time_unix
    field :end_time_unix
    field :signal
    field :submitted_manifest_base_ttl
    field :submitted_manifest_base_yaml
    field :resource_bases
    field :result_bases
    field :processed_manifest_base
    field :work
    field :local_version
    field :staging_tables
    field :base_prefix
  end

  def on_to_rdf(%__MODULE__{__id__: id} = agent, graph, _opts) do
    {:ok, RDF.Graph.add(graph, RDF.type(id, PROV.Entity))}
  end
end
