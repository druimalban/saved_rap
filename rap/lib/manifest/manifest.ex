defmodule RAP.Job.Manifest.TableDesc do
  
  use Grax.Schema
  import RDF.Sigils

  alias RAP.Vocabularies.{DCAT,DCTERMS,DCE,Schema,LinkML,SAVED}
  
  schema SAVED.TableDesc do
    property :title, DCTERMS.title
    property :atomic_name, SAVED.atomic_name
    property :job_auto_generate, SAVED.job_auto_generate
  end
end
