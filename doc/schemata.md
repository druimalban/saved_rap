# YAML schema writing
## Outline

Each data file has a schema, which we use both to validate and describe data. The [SAVED data model/ontology](https://marine.gov.scot/metadata/saved/schema/) is based on [LinkML](https://linkml.io/linkml/), with schema files authored in the YAML format. It is useful to think about these files as being made up of two main elements:

1. Metadata about the schema file. These declare certain fields essential for validation of the schema (e.g. a unique identifier and name for the schema), as well as extra, optional data such as its license, or when it was last updated.
2. Fields which describe the structure of data files to be validated against the schema. These associate the names columns with a description, a data type (e.g. text field, or number), a provenance (e.g. modelled, or simulated), and with a variable described in the data model (e.g. [`saved:lice_af_total`](https://marine.gov.scot/metadata/saved/schema/lice_af_total/)).

Fields of a typical schema are summarised as follows:

| Field             | Description | Do I need this? | Example  |
| ----------------- | ----------- | --------------- |--------- |
| `id`              | Fully unique resource identifier: a URI | Yes | `id: https://marine.gov.scot/metadata/saved/rap/sentinel_cages_sampling` |
| `name`            | Name of the resource, this is an 'atom' (no spaces, only underscores) | Yes | `name: sentinel_cages_sampling` |
| `title`           | Longer title of the schema, this may be free text | Yes | `title: "Sentinel cages sampling information schema"` |
| `description`     | Long-form free text description of the schema | No | `description: "This is the example schema for the sentinel cages sampling example…"` |
| `prefixes`        | Prefixes used in the schema. These are mappings between an atom and a URI which is prefixed to an identifier like the above `name` field. | Yes (see note) | See note |
| `imports`         | Import of slots/types/classes from other LinkML schema resources | Yes (see note) | See note |
| `default_prefix`  | Prefix to map URIs to identifiers | Yes | `default_prefix: mssamp` |
| `license`         | URI/identifier of license of the schema file | No | `license: https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/` |
| `keywords`        | List of keywords associated with schema, each list item may be free text | No | See note |
| `created_by`      | 'Atom' (text field with no spaces, only underscores) indicating the original creator of the schema file | No | `created_by: marinescot` |
| `modified_by`     | 'Atom (text field with no spaces, only underscores) | No | `modified_by: saved` |
| `last_updated_on` | Time-stamp (ISO8601 `YYYY-MM-DD` format) indicating last update | No | `last_updated_on: "2024-04-26"` |

## Metadata about the schema file

### The `id`, `name`, `title` and `description` fields

The exact distinction between these is not at first glance very clear. It can be summarised as follows:

1. The identifier (`id`) is a unique resource where the schema file may (perhaps should) live in the future. The examples use the "rap" directory under the SAVED prefix hosted on the Marine Scotland namespace. If you are not Marine Scotland, it is preferable to choose a URI which you control and could host something there. In any case, this field is necessary for a schema to be valid and to validate data files.
2. The name field is the a unique name for the schema. This is an "atom" with no spaces, only underscores. It is likely easiest just to use the end of the identifier above, e.g. for the ID `https://marine.gov.scot/metadata/saved/rap/sentinel_cages_sampling`, one might choose the name `sentinel_cages_sampling`.
3. The title of the resource is a free text field. Keep it relatively short as this will make the generated output (web pages) clearer.
4. The description is a longer free text field. Whereas the identifier, name and title fields are mandatory, this description is not. This is the place to put longer descriptions of the data, rather than the title field, although they both allow free text.

### Prefixes and imports

