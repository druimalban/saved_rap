defmodule RAP.TermHandler do
  
  def sub_special(_type, term, _variant) do
    {:ok, String.replace(term, ~r"[-|%20]", "_")}
  end

end

defmodule RAP.Vocabularies do
  @moduledoc """
  A collection of RDF namespaces

  This is based on https://github.com/marcelotto/rdf_vocab,
  with a few additions.

  License for this file is thus "MIT".
  """
  use RDF.Vocabulary.Namespace

  @vocabdoc """
  The data catalog (DCAT) vocabulary.

  See <http://www.w3.org/TR/vocab-dcat/>
  """
  defvocab DCAT,
    base_iri: "http://www.w3.org/ns/dcat#",
    file: "dcat.nt",
    case_violations: :fail
  
  @vocabdoc """
  The Dublin Core Metadata Terms vocabulary.

  See <http://purl.org/dc/terms/>
  """
  defvocab DCTERMS,
    base_iri: "http://purl.org/dc/terms/",
    file: "dcterms.nt",
    alias: [
      ISO639_2: "ISO639-2",
      ISO639_3: "ISO639-3"
    ],
    case_violations: :fail

  @vocabdoc """
  The Dublin Core Metadata Element Set, Version 1.1 vocabulary.

  See <http://purl.org/dc/elements/1.1/>
  """
  defvocab DCE,
    base_iri: "http://purl.org/dc/elements/1.1/",
    file: "dce.nt",
    case_violations: :fail
  
  @vocabdoc """
  The Schema.org vocabulary.

  See <http://schema.org>
  """
  defvocab Schema,
    base_iri: "http://schema.org/",
    file: "schema.nt",
    case_violations: :ignore

  @vocabdoc """
  The LinkML meta-model

  See <https://w3id.org/linkml/meta>
  """
  defvocab LinkML,
    base_iri: "https://w3id.org/linkml/",
    file: "linkml.ttl",
    case_violations: :ignore,
    alias: [
      linkml_alias: "alias"
    ]

  #@vocabdoc """
  #Addendum to the LinkML meta-model: data-sets
  #
  #See <https://w3id.org/linkml/datasets>
  #"""
  #defvocab LinkMLDataSets,
  #  base_iri: "https://w3id.org/linkml/datasets/",
  #  file: "linkml_datasets.ttl",
  #  case_violations: :ignore

  @vocabdoc """
  SAVED data-model
  
  Temporarily living on localhost.
  
  See <https://github.com/saved-models/data-model>

  Note that the meta-model imports *most* of schemata in its repository,
  but not all, e.g. it doesn't import `datasets.yaml'. The two
  identifiers we're actually interested here are `path' and `hash'.

  It's fair to say that the utility in importing the main LinkML
  meta-model as above is in being able to link both to LinkML proper and
  the links to other schemata and data models which it defines / imports,
  so given that it apparently does not import that component, just refer
  to these in our own schema.

  A further element of this is that it isn't actually clear why/how
  LinkML does this, nor how they generated their initial schemata/data
  model. Answering this is probably pertinent to deciding how to define
  the equivalent Grax schema/struct into which our RDF job manifests are
  loaded/injected.
  """
  defvocab SAVED,
    base_iri: "http://localhost/saved/",
    file: "saved.nt",
    case_violations: :ignore,
    alias: [
      JSON_LD: "JSON-LD",
      LD_Patch: "LD%20Patch",
      N_Quads: "N-Quads",
      N_Triples: "N-Triples",
      OWL_Functional_Syntax: "OWL%20Functional%20Syntax",
      OWL_Manchester_Syntax: "OWL%20Manchester%20Syntax",
      OWL_XML_Serialisation: "OWL%20XML%20Serialization",
      POWDER_S: "POWDER-S",
      PROV_N: "PROV-N", "PROV_XML": "PROV-XML",
      RIF_XML_Syntax: "RIF%20XML%20Syntax",
      SPARQL_Results_in_CSV: "SPARQL%20Results%20in%20CSV",
      SPARQL_Results_in_JSON: "SPARQL%20Results%20in%20JSON",
      SPARQL_Results_in_TSV: "SPARQL%20Results%20in%20TSV",
      SPARQL_Results_in_XML: "SPARQL%20Results%20in%20XML",
      RDF_XML: "rdf-xml"    
    ],
    strict: false  
    #terms: {RAP.TermHandler, :sub_special, [:variant1]}

end
