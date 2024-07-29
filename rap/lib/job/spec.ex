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
  #defstruct [ :variable_uri,  :variable_curie, :column,
  # :resource_name, :resource_base,  :table_id ]

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

#  def from_spec(%ScopeSpec{} = spec, base_prefix, output_suffix \\ "_annotated") do
#    output_table_iri =
#      TableOutput.iri_from_source(
#	spec.table_iri,
#	base_prefix,
#	output_suffix
#      )
#    %__MODULE__{
#      column:   spec.column,
#      table:    output_table_iri,
#      variable: spec.variable_uri
#    }
#  end
  
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
  #defstruct [ :base, :extant ]

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
  #defstruct [ :name, :title, :description, :resource, :schema, :source_id ]

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

  #def iri_from_source(_src = nil, _base, _suffix), do: nil
  #def iri_from_source(source_table, base_prefix, output_suffix \\ "_annotated") do
  #  id = source_table
  #  |> Producer.extract_id()
  #  |> String.append()
  #  |> then(fn k -> k <> output_suffix end)
  #  iri = base_prefix
  #  |> RDF.IRI.new()
  #  |> RDF.IRI.append(id)
  #  iri
  #end
  
end

defmodule RAP.Job.JobSpec do
  @moduledoc """
  Simple struct for recording both errors and correct paired columns
  These are not blank nodes and so are associated with a name like a
  table description.
  """
  #  defstruct [ :name, :title, :description,
  #	      :type, :result_format, :result_stem,
  #	      :scope_descriptive,   :scope_collected,  :scope_modelled,
  #	      :errors_descriptive,  :errors_collected, :errors_modelled,
  #	      :source_id ]

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
    field :result_format # I don't think these ought to go here, they're a property of the pipeline
    field :result_stem   #
  end
end

defmodule RAP.Job.ManifestSpec do
  @moduledoc """
  While the manifest is associated with a name (it's not a blank node),
  this is by default `RootManifest'. Nonetheless, it's feasible that we
  submit manifests with a different name.

  This module reincoprorates the UUID, path of the manifest proper, and
  paths of the various resources associated with the job.
  """
  #  defstruct [ :name, :title, :description, :local_version, :uuid, :data_source,
  #	      :pre_signal, :signal, :manifest_base_ttl, :manifest_base_yaml,
  #	      :resource_bases,
  #	      :staging_tables, :staging_jobs,
  #	      :source_id, :base_prefix ]

  use Grax.Schema, depth: +5
  import RDF.Sigils
  alias RAP.Vocabulary.{DCTERMS, DCAT, PROV, PAV, SAVED}
  alias RAP.Manifest.{TableDesc, JobDesc}
  alias RAP.Job.{TableSpec, JobSpec, Result}
  alias RAP.Provenance.{RAPProcess, RAPInvocation, RAPStageProcessing}
  #alias RAP.Manifest.{TableDesc, JobDesc}

  schema SAVED.ManifestOutput do
    property :uuid,               SAVED.uuid,          type: :string
    property :data_source,        SAVED.data_source,   type: :string
    property :title,              DCTERMS.title,       type: :string
    property :description,        DCTERMS.description, type: :string
    property :output_format,      DCAT.mediaType,      type: :string
    property :download_url,       DCAT.downloadURL,    type: :iri
    property :submitted_manifest, PROV.wasDerivedFrom, type: :iri
    property :signal,             SAVED.stage_signal,  type: :string
    link tables:  SAVED.tables,  type: list_of(TableSpec),  depth: +5
    link jobs:    SAVED.jobs,    type: list_of(JobSpec),    depth: +5
    link results: SAVED.results, type: list_of(Result), depth: +5
    link rap_app:         SAVED.rap_application,      type: RAPProcess,    depth: +5
    link rap_app_init:    SAVED.rap_application_init, type: RAPInvocation, depth: +5
    link rap_stages:      SAVED.rap_stages,      type: list_of(RAPProcess),    depth: +5
    link rap_stages_init: SAVED.rap_stages_init, type: list_of(RAPInvocation), depth: +5
    link rap_processing:  SAVED.rap_processing,  type: list_of(RAPStageProcessing), depth: +5
    
    # link stages: SAVED.stages, type: list_of(RAPStage), depth: +5
    # FIXME: Extras just to get the thing to compile
    field :start_time
    field :end_time
    field :manifest_base_ttl
    field :manifest_base_yaml
    field :resource_bases
    field :result_bases
    field :processed_manifest_base
    field :work
    field :local_version
    field :staging_tables
    field :base_prefix
  end
  
  #def expand_id(%__MODULE__{} = spec, source_name, base_prefix, output_suffix \\ "_output") do
  #  iri =
  #    case source_name do
  #nil -> RDF.IRI.new(base_prefix <> "RootManifest" <> output_suffix)
  #nom -> RDF.IRI.new(base_prefix <> source_name    <> output_suffix)
  #end
  # %{ spec | __id__: iri }
  #end
end
