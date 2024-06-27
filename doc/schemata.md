# YAML schema writing
## Outline

Each data file has a schema, which we use both to validate and describe data. The [SAVED data model/ontology](https://marine.gov.scot/metadata/saved/schema/) is based on [LinkML](https://linkml.io/linkml/), with schema files authored in the YAML format. It is useful to think about these files as being made up of two main elements:

1. Metadata about the schema file. These declare certain fields essential for validation of the schema (e.g. a unique identifier and name for the schema), as well as extra, optional data such as its license, or when it was last updated.
2. Fields which describe the structure of data files to be validated against the schema. These associate the names columns with a description, a data type (e.g. text field, or number), a provenance (e.g. modelled, or simulated), and with a variable described in the data model (e.g. [`saved:lice_af_total`](https://marine.gov.scot/metadata/saved/schema/lice_af_total/)).

Top-level fields of a typical schema are summarised as follows:

| Field             | Description                                                                            | Do I need this? | Example  |
| ----------------- | -------------------------------------------------------------------------------------- | --------------- |-------------------------------------------------------------------------------------- |
| `id`              | Fully unique resource identifier: a URI                                                | Yes             | `id: https://marine.gov.scot/metadata/saved/rap/sentinel_cages_sampling`              |
| `name`            | Name of the resource, an 'atom' (text field with no spaces, only underscores)          | Yes             | `name: sentinel_cages_sampling`                                                       |
| `title`           | Longer title of the schema: this may be free text                                      | Yes             | `title: Sentinel cages sampling information schema`                                   |
| `description`     | Long-form free text description of the schema                                          | No              | `description: This is the example schema for sentinel cages sampling…`                |
| `prefixes`        | List of mappings between an 'atom' and URI prefix which may be prefixed to identifiers | Yes             | See note                                                                              |
| `imports`         | Import of slots/types/classes from other LinkML schema resources                       | Yes             | See note                                                                              |
| `default_prefix`  | Default prefix declaring URI to map to identifiers in the schema                       | Yes             | `default_prefix: mssamp`                                                              |
| `license`         | URI/identifier of license associated with schema file                                  | No              | `license: https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/` |
| `keywords`        | List of keywords associated with schema, each list item may be free text               | No              | See note                                                                              |
| `created_by`      | 'Atom' indicating original creator of schema                                           | No              | `created_by: marinescot`                                                              |
| `modified_by`     | 'Atom' indicating who last modified schema                                             | No              | `modified_by: saved`                                                                  |
| `last_updated_on` | Time-stamp (ISO8601 `YYYY-MM-DD` format) indicating last update                        | No              | `last_updated_on: "2024-04-26"`                                                       |

## YAML syntax

YAML documents are made up of blocks, with indentation indicating membership of elements, and elements as mappings between 'atoms' and values. For example, in the LinkML schema, the slot for the `Sampling.Note` column is declared as follows:

```yaml
slots:
  Sampling.Note:
    description: "Notes on issues with sampling"
	range:       string
	required:    false
	exact_mappings:
	  -  saved:date
```

Key things to keep in mind:
1. YAML is a machine-readable language, and indentation matters. In the above example, if `range` was indented two spaces fewer, it would not be considered to be part of the `Sampling.Note` block.
2. It is not necessary to quote the majority of text fields (even free text), except if the text includes control characters used by YAML, such as colons (which mark key/value pairs), dashes (which designate lists), or hashes (which designate comments).
3. If a text field is multi-line (sometimes useful for descriptions), you need to use `>-` and indent the text in question below. The end of the indentation, here, rather than quotes, marks the end of the text field.

## Metadata about the schema file

### The `id`, `name`, `title` and `description` fields

The exact distinction between these is not at first glance very clear. It can be summarised as follows:

1. The identifier (`id`) is a unique resource where the schema file may (perhaps should) live in the future. The examples use the "rap" directory under the SAVED prefix hosted on the Marine Scotland namespace. If you are not Marine Scotland, it is preferable to choose a URI which you control and could host something there. In any case, this field is necessary for a schema to be valid and to validate data files.
2. The name field is the a unique name for the schema. This is an "atom" with no spaces, only underscores. It is likely easiest just to use the end of the identifier above, e.g. for the ID `https://marine.gov.scot/metadata/saved/rap/sentinel_cages_sampling`, one might choose the name `sentinel_cages_sampling`.
3. The title of the resource is a free text field. Keep it relatively short as this will make the generated output (web pages) clearer.
4. The description is a longer free text field. Whereas the identifier, name and title fields are mandatory, this description is not. This is the place to put longer descriptions of the data, rather than the title field, although they both allow free text.

### Prefixes and imports
#### Prefixes

**Prefixes** are a collection of between an atom (text field with no spaces, only underscores) and a URI prefix. These mappings are prefixes because they are used to create unique identifiers from names, i.e. they prefix the name of a thing. Rather than type out the name of the URI every time, we use the short atom to refer to it.

Consider the following set of prefixes:

```yaml
prefixes:
  linkml: https://w3id.org/linkml/
  saved:  https://marine.gov.scot/metadata/saved/schema/
  rap:    https://marine.gov.scot/metadata/saved/rap/
  mssamp: https://marine.gov.scot/metadata/saved/rap/sentinel_cages_sampling/
  xsd:    http://www.w3.org/2001/XMLSchema#
```

Identifiers are referenced using the form `<atom>:<resource name>`, and are expanded to `<prefix URI><resource name>`. The last character of the prefix URI is important because the expansion does not assume anything about it, e.g. both the [`xsd`](http://www.w3.org/2001/XMLSchema#) (which ends in a hash) and the other prefixes are valid. The expansion is very simple. The identifier [`saved:lice_af_total`](https://marine.gov.scot/metadata/saved/schema/lice_af_total/) would expand to `https://marine.gov.scot/metadata/saved/schema/lice_af_total`.

#### The default prefix

The default prefix is a single reference to a prefix listed as above:

```yaml
default_prefix: mssamp
```

The function of the default prefix is to create unique identifiers for any element declared in the current schema file. For example, in the `Sampling.Note` example above, other documents could refer to this identifier using the URI `https://marine.gov.scot/metadata/saved/rap/sentinel_cages_sampling/Sampling.Note` (expanded from `mssamp:Sampling.Note`). Indeed, if they declared also some prefix (it could be `mssamp`, or `mssamp2`, as long as it maps to the same URI as `mssamp` does in this document), they could also refer to it using that prefix.

The basic idea here, then, is that we need to declare a default prefix which is unique to this document, to identify elements in this document, both internally and externally.

#### Imports

**Imports** are a link to an external LinkML schema, from which items declared in the schema are imported. 

Consider the following set of imports:

```yaml
imports:
  linkml:types
  linkml:datasets
  saved:core
  saved:job
```

There is an important distinction between referencing an identifier and importing it. For example, if [`saved:core`](https://marine.gov.scot/metadata/saved/schema/core.yaml) in the example above declared the type [`saved:LatLonType`](https://marine.gov.scot/metadata/saved/schema/LatLonType/), to use this as a range proper, we would need to import [`saved:core`](https://marine.gov.scot/metadata/saved/schema/core.yaml) in the `imports` directive above. That is, [`saved:LatLonType`](https://marine.gov.scot/metadata/saved/schema/LatLonType/) is effectively just a URI (it expands to `https://marine.gov.scot/metadata/saved/schema/LatLonType`), and isn't usable in a different LinkML schema file just by referencing it. Note that importing it would bring it into the top level, i.e. to use it as a range, we would simply use it as `LatLonRange`.

#### The relation between prefixes and imports, or, what do I need to include?

This hints at the importance of the prefixes and their relation to imports. 

1. Any import must use both a valid prefix, and the resource in question must exist and be a valid LinkML schema file.
2. To be a valid LinkML schema file, declare both the [`linkml`](https://w3id.org/linkml/) prefix and import [`linkml:types`](https://w3id.org/linkml/types)
3. Our schema files are based on [our own data model/ontology](https://marine.gov.scot/metadata/saved/schema/), so, always declare the `saved` prefix and import [`saved:core`](https://marine.gov.scot/metadata/saved/schema/core.yaml)
4. The schema file must declare a prefix unique to this document, to be set as the default prefix in the document.
4. It is not necessary to import a schema file to reference anything related to the prefix, and in fact, this may cause problems. However, it is always necessary to add a prefix to the `prefixes` directive before referencing any identifier underneath it, as it will be impossible to expand the identifier into a URI.

#### What is the difference between our data model/ontology, and schema files for data?

You have probably noticed that both [SAVED data model/ontology](https://marine.gov.scot/metadata/saved/schema/) and the schema files which we are writing are based on LinkML. This partly derives [from the design of LinkML itself](https://linkml.io/linkml/faq/general.html#is-linkml-just-for-metadata), which "doesn't draw any hard and fast distinction between data and metadata, recognising that “metadata” is often defined in relative terms."

There are several elements to the fish data utilities workflow:
1. We write **schema** files for data, which are based on LinkML
2. The `fisdat(1)` utility validates and appends metadata to the description manifest, which is a **data** file in YAML or RDF/TTL, the schema of which is the [`job.yaml` component of the data model](`https://marine.gov.scot/metadata/saved/schema/job.yaml`)
3. The data description manifest can be edited to describe jobs to be run on the data
4. Upon upload, the `upload(1)` utility:
   1. Checks that the referenced data exists, and that the checksum matches
   2. Converts the schema files from YAML to machine-readable RDF/TTL (which can be used independently of the LinkML Python libraries)
   3. Converts the manifest files from YAML to machine-readable RDF/TTL (or vice versa, since we also support creating/editing the manifest in RDF/TTL)
   4. Uploads the set of data files + schema files + manifest to an external storage hosting provider (such as, at the moment Google Cloud)
   
Here, it can be said that the schema files which we are writing describe data files, whereas the manifest files which the fish data utilities create are data files, which (usually) happen to be in the YAML format.

A further point which is relevant to this section is that there are circumstances in which YAML schema files will be valid, despite prefixes being used but not declared, whereas they may fail conversion due to this issue. Make sure that all identifiers referenced use a prefix which has been declared.
