defmodule RAP.TermHandler do
  
  def sub_special(_type, term, _variant) do
    {:ok, String.replace(term, ~r"[-|%20]", "_")}
  end

end


# @prefix dcterms:   <http://purl.org/dc/terms/> .
# @prefix pav:       <http://purl.org/pav/> .
# @prefix dcat:      <http://www.w3.org/ns/dcat#> .
# @prefix rdr:       <http://www.re3data.org/schema/3-0> .
# @prefix prov:      <http://www.w3.org/ns/prov#> .
# @prefix rdfs:      <https://www.w3.org/2000/01/rdf-schema#> .


defmodule RAP.Vocabulary do
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
  The W3C PROVenance vocabulary.

  See <http://www.w3.org/TR/prov-o/>
  """
  defvocab PROV,
    base_iri: "http://www.w3.org/ns/prov#",
    file: "prov.nt",
    case_violations: :fail

  @vocabdoc """
  The Dublin Core Metadata Element Set, Version 1.1 vocabulary.

  See <http://purl.org/dc/elements/1.1/>

  Named `dc:' in the LinkML prefixes section, so name it as such here.
  """
  defvocab DC,
    base_iri: "http://purl.org/dc/elements/1.1/",
    file: "dc.nt",
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
  The Provenance, Authoring and Versioning (PAV) Version 2.3.1 vocabulary

  See <http://purl.org/pav/2.3/>
  """
  defvocab PAV,
    base_iri: "http://purl.org/pav/",
    file: "pav.nt",
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
    base_iri: "https://marine.gov.scot/metadata/saved/schema/",
    file: "saved.nt",
    case_violations: :ignore,
    terms: {RAP.TermHandler, :sub_special, [:variant1]},
    strict: true

  @vocabdoc """
  SAVED data-model: RAP-specific namespace

  Unsetting the `strict' element of the vocabulary avoids having to
  define arbitary instances of the manifest description, and the tables
  and jobs descriptions, in the vocabulary. This would make sense for the
  manifest itself (it's always called 'RootManifest'), but the manifest
  can include a number of instances of tables or jobs, none of which are
  more meaningful than the other. This is warned against in production
  code by the RDF.ex documentation, but there is no other way to do this.
  I suspect our use-case is fairly unusual, so ignore this for now, while
  keeping it in mind, as this is really at the stage proof-of-concept.
  """
  defvocab RAP,
    base_iri: "https://marine.gov.scot/metadata/saved/rap/",
    terms: [:RootManifest],
    strict: false

end
