defmodule Job.Manifest do
  
  use Grax.Schema
  import RDF.Sigils

  alias RAP.Vocabularies.{DCAT,DCTERMS,DCE,Schema,LinkML,SAVED}
  
  schema do
    property :title, DCTERMS.title
  end
end
