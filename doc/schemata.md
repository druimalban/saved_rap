# YAML schema writing
## Outline

Each data file has a schema, which we use both to validate and describe data. The [SAVED data model/ontology](https://marine.gov.scot/metadata/saved/schema/) is based on [LinkML](https://linkml.io/linkml/), with schema files authored in the YAML format. It is useful to think about these files as being made up of two main elements:

1. Metadata about the schema file. These declare certain fields essential for validation of the schema (e.g. a unique identifier and name for the schema), as well as extra, optional data such as its license, or when it was last updated.
2. Fields which describe the structure of data files to be validated against the schema. These associate the names columns with a description, a data type (e.g. text field, or number), a provenance (e.g. modelled, or simulated), and with a variable described in the data model (e.g. [`saved:lice_af_total`](https://marine.gov.scot/metadata/saved/schema/lice_af_total/)).

Top-level fields of a typical schema are summarised as follows, including what is strictly necessary for a schema file to be valid a LinkML schema file, what is necessary for validation, as well as a few other fields which you may want to include. This summary is not exhaustive, in fact, any fields in the LinkML model should be valid. The example schema files included with the fish data utilities were originally devised based on the JSON/CSVW schema files which we were writing before, as well as from following [the LinkML tutorial](https://linkml.io/linkml/intro/tutorial.html). You can choose to enrich your schema file with additional fields beyond what is needed just for validation, then.

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
4. The order of fields does not strictly matter, but at least in terms of writing schema files for LinkML, it makes good sense to follow the split described above (metadata describing the schema; fields describing structure of data) and the order effectively described in this document, and indicated in the examples.

## Metadata about the schema file

### The `id`, `name`, `title` and `description` fields

The exact distinction between these is not at first glance very clear. It can be summarised as follows:

1. The identifier (`id`) is a unique resource where the schema file may (perhaps should) live in the future. The examples use the "rap" directory under the SAVED prefix hosted on the Marine Scotland namespace. If you are not Marine Scotland, it is preferable to choose a URI which you control and could host something there. In any case, this field is necessary for a schema to be valid and to validate data files.
2. The name field is the a unique name for the schema. This is an "atom" with no spaces, only underscores. It is likely easiest just to use the end of the identifier above, e.g. for the ID `https://marine.gov.scot/metadata/saved/rap/sentinel_cages_site`, one might choose the name `sentinel_cages_site`.
3. The title of the resource is a free text field. Keep it relatively short as this will make the generated output (web pages) clearer.
4. The description is a longer free text field. Whereas the identifier, name and title fields are mandatory, this description is not. This is the place to put longer descriptions of the data, rather than the title field, although they both allow free text.

```yaml
id: http://marine.gov.scot/metadata/saved/rap/sentinel_cages_site/
name:  sentinel_cages_site
title: Sentinel cages station information schema
description: >-
  This is an example for the purposes of testing fisdat(1) and fisup(1)
  This is a multi-line text field, the start of which is the `>-' section,
  with the end indicated by change in indentation

```

### Prefixes and imports
#### Prefixes

Prefixes are a collection of between an atom (text field with no spaces, only underscores) and a URI prefix. These mappings are prefixes because they are used to create unique identifiers from names, i.e. they prefix the name of a thing. Rather than type out the name of the URI every time, we use the short atom to refer to it.

Consider the following set of prefixes:

```yaml
prefixes:
  linkml: https://w3id.org/linkml/
  saved:  https://marine.gov.scot/metadata/saved/schema/
  rap:    https://marine.gov.scot/metadata/saved/rap/
  mssite: https://marine.gov.scot/metadata/saved/rap/sentinel_cages_site/
  mssamp: https://marine.gov.scot/metadata/saved/rap/sentinel_cages_sampling/
  xsd:    http://www.w3.org/2001/XMLSchema#
```

Identifiers are referenced using the form `<atom>:<resource name>`, and are expanded to `<prefix URI><resource name>`. The last character of the prefix URI is important because the expansion does not assume anything about it, e.g. both the [`xsd`](http://www.w3.org/2001/XMLSchema#) (which ends in a hash) and the other prefixes are valid. The expansion is very simple. The identifier [`saved:lice_af_total`](https://marine.gov.scot/metadata/saved/schema/lice_af_total/) would expand to `https://marine.gov.scot/metadata/saved/schema/lice_af_total`.

#### The default prefix

The default prefix is a single reference to a prefix listed as above:

```yaml
default_prefix: mssamp
```

Some things to keep in mind:
- The purpose of the default prefix is to create unique identifiers for any element declared in the current schema file. However, it is possible that this prefix could be used across different schema files. Indeed, this is the approach taken for producing data models using LinkML schema files: they share a single prefix, and can import each other.
- For example, in the `Sampling.Note` example above, other documents could refer to this identifier using the URI `https://marine.gov.scot/metadata/saved/rap/sentinel_cages_sampling/Sampling.Note` (expanded from `mssamp:Sampling.Note`).
- Indeed, external schema files may also declare some prefix (it could be the same, `mssamp`; or e.g. `mssamp_alt`, as long as it maps to the same URI as `mssamp` does in this document), they could also refer to it using that prefix.
- When declaring prefixes, although they are arbitrary, there tends to be conventions for certain well-known prefixes, a sample list of which is [summarised here](https://bioregistry.io/registry/).

The basic idea here, then, is that we need to declare a default prefix which is unique to this document, to identify elements in this document, both internally and externally. 

#### Compact URIs (CURIEs)

LinkML has a type called [`UrioOrCurie`](https://linkml.io/linkml-model/1.7.x/docs/Uriorcurie/). This is either a full URI, or the compact equivalent. That is, the compact URI, or CURIE, is a succint way to refer to resources under the prefix in question, and it also allows us to name the prefix to some short-hand which describes its role.

Consider the `saved` prefix above (which has a trailing slash). Given a URI, [`https://marine.gov.scot/metadata/saved/schema/lice_density_collected`](https://marine.gov.scot/metadata/saved/schema/lice_density_collected), then, the compact URI equivalent is anything after the prefix, i.e. `saved:lice_density_collected`. A space on either side of the colon (e.g. `saved: lice_density_collected`) is not a valid CURIE, and would, at least in the YAML files, be considered to be part of the YAML syntax, which uses colons extensively.

See the following page for a bit more information about the role of URIs and CURIEs: [https://cthoyt.com/2021/09/14/curies.html
](https://cthoyt.com/2021/09/14/curies.html)

#### Imports

Imports are a link to an external LinkML schema, from which items declared in the schema are imported. 

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
   
Here, it can be said that the **schema** files which we are writing describe data files, whereas the manifest files which the fish data utilities create are **data** files, which are (usually) in the YAML format.

A further point which is relevant to this section is that there are circumstances in which YAML schema files will be valid, despite prefixes being used but not declared, whereas they may fail conversion due to this issue. Make sure that all identifiers referenced use a prefix which has been declared.

## Describing data files

### Classes, slots, and types

The LinkML has three related concepts:
1. Classes, which are effectively a way to describe *things* with attributes
2. Slots, which are attributes, and correspond closely to the columns in our data files
3. Types, which extend basic types (such as numbers or text strings)

For validation, at minimum, `fisdat(1)` expects a couple of things:
1. Declaration of a simple class `TableSchema`, which is used by the programs to actually load the schema files
2. A set of slots referenced in the schema, corresponding to the columns of the data file in question, which are included as attributes of the `TableSchema` class

The following is a shortened example `TableSchema` object from the sentinel cages (sampling information) example. We can think of `TableSchema` as a class which has column attributes.

```yaml
slots:
  Cage.Number:
    description:    A unique identifier for each cage which links back to station information. Not necessarily a number.
    any_of:
      - range:      integer
      - range:      string
    is_a:           column_descriptive
    exact_mappings: saved:cage_id
    implements:     linkml:elements
    required:       true

  Deployment.date:
    description:    Date cage was stocked with fish
    range:          string
    is_a:           column_descriptive
    exact_mappings:
      - saved:date
      - saved:deployment_date
    implements:     linkml:elements
    required:       true

  Fish.Weight.g:
    description:    Weight of fish expressed in grams
    range:          float
    is_a:           column_collected
    exact_mappings: saved:fish_mass
    implements:     linkml:elements
    required:       false

  TOTAL:
    description:    Total number of lice on the fish
    range:          integer
    is_a:           column_collected
    exact_mappings: saved:lice_af_total
    implements:     linkml:elements
    required:       false

classes:
  TableSchema:
    implements:
      - linkml:TwoDimensionalArray
      - linkml:ColumnOrderedArray
    slots:
      - Deployment.date
      - Sampling.Note
      - Fish.Weight.g
      - TOTAL
```

Slots are declared in their own section at the top level of the YAML schema file, and then referenced in the table schema class object. This is because the LinkML schema files support a notion of slot *reuse*: by declaring them in this slots section, they are available to multiple classes in the same LinkML schema file (for instance, if we had another class, called `TableSchemaAlt`), as well as to other schema files (either a link to the URI of the slot object, or by importing the schema file to access its objects directly).

For example, in this example schema, you will note that there is a slot not used in the `TableSchema` object (`Cage.Number`), and there is a slot referenced in the `TableSchema` object which is not declared in the `slots` section (`Sampling.Note`). The former is valid (the `Cage.Number` slot just won't be included as an attribute of the `TableSchema` object), whereas the latter is invalid, because this slot does not (yet) exist.

A typical slot declaration has the following properties:
- The top-level *name* of the slot, for example `TOTAL`. For the purposes of validation, this should be the exact label used in the data file which is to be validated against this schema file.
- [`description`](https://linkml.io/linkml-model/1.7.x/docs/description/): Free text description of the column, while not required, strongly advisable to be included
- [`range`](https://linkml.io/linkml-model/1.7.x/docs/range/): The underlying data type of the column. Valid base types are `boolean`, `integer`, `float` (or `double`), or `string`. This can be inadequate when the column is a mixture of text and numeric data, e.g. consider the `Cage.Number` example. In these cases, specify [`any_of`](https://linkml.io/linkml-model/1.7.x/docs/any_of/) with a list of `range` directives, as in the `Cage.Number` example above.
- [`exact_mappings`](https://linkml.io/linkml-model/1.7.x/docs/exact_mappings/): Link/map columns to equivalents in the ontology/data model, or in other schema files. For example, the `TOTAL` column/slot above directly maps to values like [`saved:lice_af_total`](https://marine.gov.scot/metadata/saved/schema/lice_af_total/). There are further notions of mappings, since there are a number of situations in which there isn't an exact mapping: you can use [`broad_mappings`](https://linkml.io/linkml-model/1.7.x/docs/broad_mappings/), [`narrow_mappings`](https://linkml.io/linkml-model/1.7.x/docs/narrow_mappings/), [`close_mappings`](https://linkml.io/linkml-model/1.7.x/docs/close_mappings/) and [`related_mappings`](https://linkml.io/linkml-model/1.7.x/docs/related_mappings/)
- [`implements`](https://linkml.io/linkml-model/1.7.x/docs/implements/): External slot definition which the column slot implements. We are validating these with LinkML, so at least semantically, it is important to include [`linkml:elements`](https://linkml.io/linkml-model/1.7.x/docs/elements/) at minimum. If the slot implements any other slot definition, turn it into a list with `linkml:elements` and additional definitions as separate list elements.
- [`required`](https://linkml.io/linkml-model/1.7.x/docs/required/): Set as true/false depending on whether the data included have missing values. See the section "Dealing with missing data" in the troubleshooting/debugging section for more information on this and validation.

Further note the inclusion of the [`implements`](https://linkml.io/linkml-model/1.7.x/docs/implements/) directive in the `TableSchema` class definition. Including this was based on LinkML's [documentation prior to version 1.7.0](https://linkml.io/linkml/howtos/multidimensional-arrays.html). They have since added support for arrays, so including this is not strictly necessary, especially given the suggested class URIs do not actually resolve.
