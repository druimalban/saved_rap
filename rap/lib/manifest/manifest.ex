defmodule RAP.Manifest.TableDesc do
  
  use Grax.Schema, depth: +2
  import RDF.Sigils
  alias RAP.Vocabularies.{DCTERMS,SAVED}
  
  schema SAVED.TableDesc do
    property :title,         DCTERMS.title,       type: :string
    property :description,   DCTERMS.description, type: :string
    property :resource_path, SAVED.resource_path, type: :string
    property :schema_path,   SAVED.schema_path,   type: :string
    property :resource_hash, SAVED.resource_hash, type: :string
    property :scope,         SAVED.scope,         type: list_of(:string)
  end
end

defmodule RAP.Manifest.SourceDesc do

  use Grax.Schema, depth: +2
  import RDF.Sigils
  alias RAP.Vocabularies.SAVED

  schema SAVED.SourceDesc do
    property :scope, SAVED.scope, type: list_of(:string)
  end
end

defmodule RAP.Manifest.JobDesc do

  use Grax.Schema, depth: +2
  import RDF.Sigils
  alias RAP.Vocabularies.{DCTERMS,SAVED}
  alias RAP.Manifest.{SourceDesc}

  schema SAVED.JobDesc do
    property :title,             DCTERMS.title,           type: :string
    property :job_type,          SAVED.job_type,          type: :string
    property :job_auto_generate, SAVED.job_auto_generate, type: :boolean

    link job_sources: SAVED.job_sources, type: list_of(RAP.Manifest.SourceDesc)
  end
end

defmodule RAP.Manifest.ManifestDesc do

  use Grax.Schema, depth: +2
  import RDF.Sigils
  alias RAP.Vocabularies.SAVED

  schema SAVED.ManifestDesc do
    property :local_version, SAVED.local_version

    link tables: SAVED.tables, type: list_of(RAP.Manifest.TableDesc)
    link jobs:   SAVED.jobs,   type: list_of(RAP.Manifest.JobDesc)
  end
end
