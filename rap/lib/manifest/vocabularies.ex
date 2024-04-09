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
  The LinkML specification

  See <https://w3id.org/linkml/>
  """
  defvocab LinkML,
    base_iri: "https://w3id.org/linkml/",
    file: "linkml.ttl",
    case_violations: :ignore,
    alias: [
      linkml_alias: "alias"
    ]

  @vocabdoc """
  SAVED data-model

  Temporarily living on localhost.

  See <https://github.com/saved-models/data-model>
  """
  defvocab SAVED,
    base_iri: "http://localhost/saved/",
    file: "saved.ttl",
    case_violations: :ignore,
    terms: {RAP.TermHandler, :sub_special, [:variant1]}

end
