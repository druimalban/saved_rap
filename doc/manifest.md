# The manifest

## Outline

As it stands, the fish data utilities work-flow can be summarised as follows:

1. We write **schema** files for data, which are based on [LinkML](https://linkml.io/linkml/)
2. The `fisdat(1)` utility validates and appends metadata to the description manifest, which is a **data** file in YAML or RDF/TTL, the schema of which is the [`job.yaml` component of the data model](`https://marine.gov.scot/metadata/saved/schema/job.yaml`)
3. The data description manifest can be edited to describe jobs to be run on the data
4. Upon upload, the `fisup(1)` utility:
   1. Checks that the referenced data exists, and that the checksum matches
   2. Converts the schema files from YAML to machine-readable RDF/TTL (which can be used independently of the LinkML Python libraries)
   3. Converts the manifest files from YAML to machine-readable RDF/TTL (or vice versa, since we also support creating/editing the manifest in RDF/TTL)
   4. Uploads the set of data files + schema files + manifest to an external storage hosting provider (such as, at the moment Google Cloud)

One might generate a schema in the `examples/sentinel_cages` directory as follows:
```sh
% fisdat sentinel_cages_site.yaml Sentinel_cage_station_info_6.csv  my_manifest.yaml
% fisdat sentinel_cages_sampling.yaml sentinel_cages_cleaned.csv  my_manifest.yaml
% fisup my_manifest.yaml
```

## Declaring jobs
### The generated example job
If you've had a look at the generated manifest files, you may have noticed that it generates an example/empty job when the utility first creates the manifest file (additional example jobs aren't appended to the file when appending more data). In YAML, the example/empty job has the following form:

```yaml
jobs:
- atomic_name: job_example_sentinel_cages_cleaned
  job_type:    ignore
  title:       Empty job template for sentinel_cages_cleaned
```

The design of this section is not specific to any job. The data model does not know anything about the structure of a job, or what it runs. All it knows about is the following attributes:

1. `atomic_name`: This is an identifier for the job description. Recall that an 'atom' is a text string with no spaces, underscores are the only valid control characters. It must be unique, indeed, it gets transformed into the identifier for the job (a URI) in RDF/TTL.
2. `job_type`: This is the "type" of the job and the data model has a notion of valid jobs. At the moment, these are "ignore" and "density".
3. `title`: A free-text title of the job. Keep it relatively short like the `title` field in the YAML schemata. Longer descriptions should go in the `description` field.

### Additional attributes

There are several other fields supported here:

1. `description`: Longer free-text description of the job. Both this and title are a key part of the feedback at the end of the pipeline, and will be included in the generated results (web pages).
2. `job_scope_descriptive`: A list of column mappings to bring into scope, the provenance of which is notionally that they describe data about the world (e.g. latitude, longitude, data sampling notes).
3. `job_scope_collected`: A list of column mappings to bring into scope, the provenance of which is notionally that they describe data which has been collected, or sampled, from the environment.
4. `job_scope_modelled`: A list of column mappings to bring into scope, the provenance of which is notionally that they describe data which has been mod, or simulated.

Column mappings to bring into scope for the job are specified in the same way for each type, with the following fields necessary:
1. `column`: The verbatim column name in the table/data file in question, e.g. `TOTAL`
2. `table`: The name of the table object (specifically, the `atomic_name` field) in the manifest file which contains the column, e.g. `sampling`. It is likely that when comparing data, the source columns are included in different files.
3. `variable`: The **underlying** variable in the SAVED data model, e.g. [`saved:lice_af_total`](https://marine.gov.scot/metadata/saved/schema/lice_af_total/). Making sure that this is the variable which the job in question is able to process is important, as it is how subsequent processing of the job proper is able to identify the variable to which the column actually refers.

In effect, what we are doing here is columns to data files, and to an underlying variable in the data model, which we have ostensibly agreed describes something across models. This lets us run jobs on generic data with arbitrary column names, which reflects quite well what we encounter in practice, particularly when sharing data. The neat thing about this approach is it really emerges naturally from the notion that we should link variables in data files to variables in the data model.

#### Density example

```yaml
atomic_name: RootManifest
tables:
- atomic_name: time_density_simple
  resource_path: density.csv
  resource_hash: 1974c2dbefaeaaa425a789142e405f7b8074bb96348b24003fe36bf4098e6b58e2227680bcf72634c4553b214f33acb4
  schema_path_yaml: density.yaml
  title: placeholder time/density description
  description: ''
- atomic_name: sampling
  resource_path: cagedata-10.csv
  resource_hash: 338279e44840d693ce184ef672c430c8cf0d26bc4ca4ca968429f0b3b472685f5410d78ab808b102f1f37148020b4d0c
  schema_path_yaml: sentinel_cages_sampling.yaml
  title: Sentinel cages sampling information schema
  description: ''
jobs:
- atomic_name: job_example_time_density_simple
  job_type: density
  title: Example job time_density_simple
  job_scope_collected:
    - column: TOTAL
      table:  sampling
      variable: saved:lice_af_total
  job_scope_modelled:
    - column: time
      table: time_density_simple
      variable: saved:time
    - column: density
      table: time_density_simple
      variable: saved:density
local_version: 0.5
```
The manifest itself has an `atomic_name` identifier. This is by default `RootManifest`, and you can change this. What does this mean in practice?

- Recall that when writing schema files for our data, we had to declare a prefix to be used for the schema.
- When serialising manifests as RDF/TTL (with the `--manifest-format ttl`  option in `fisdat(1)`, and/or during the conversion upon upload), there is a so-called 'base' prefix which uses these identifiers. This is by default `https://marine.gov.scot/metadata/saved/rap/`.
- Currently, there isn't a check on whether the expanded identifier (the default would thus be `https://marine.gov.scot/metadata/saved/rap/RootManifest`) is already in use, but there could be in the future. 
- Making the name of the serialised manifest, unique then, involves either changing the base prefix to something else (e.g. `https://marine.gov.scot/metadata/saved/rap/job_20240627/`, using the `--base-prefix <some_prefix>` CLI option), varying the name of the manifest in this file (e.g. to `Manifest20240627`), or some combination of the two. 
- Since the aim is to link data together, including results, it's worth thinking about this carefully. Varying the base prefix is desirable in the sense that not everyone is Marine Scotland, so would have a different place to put results.

Other things to consider:

- The `tables` and `jobs` sections are lists. Note the dash before the start of a new element in the list, where indentation indicates that these list items are part of the same block.
- In general, do not edit the `tables` section, since these are created by the `fisdat(1)` tool. It is easy to make mistakes, and then the upload with `fisup(1)` may fail. In both these lists, what makes elements unique is the `atomic_name` identifier.
- There is a single job declared here, but more than one job could be requested in a single manifest file, just like there . Whether to create multiple manifests for multiple jobs may depend on the cost of uploading data, which may be large.


