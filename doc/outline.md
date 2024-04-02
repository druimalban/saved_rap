# Reproducible analytical pipeline for SAVED

## About RAP

"RAP" stands for [reproducible analytical pipeline](https://ukgovdatascience.github.io/rap_companion/).
This is a term commonly used in the civil service, and it is useful as it
is largely self-explanatory and succint. The term is typically used to
refer to the process of producing statistical reports and producing
website documentation, whereas our use here is somewhat more expansive.

## Components of the pipeline

The first component of the pipeline is a data model based on LinkML,
which defines variables which we agree should be used by those of us
sharing data between each other. Not every variable in data provided
will be in this data model. These data models are sometimes known as
ontologies, and are part of the wider semantic web. The advantage of
LinkML is that the schemata (YAML) are relatively easy to write compared
to JSON, and that it allows us to define links between data consistently
and in a machine-readable fashion. Linking data and agreeing on how to
share it is a key element of the project overall, and is a pre-requisite
to automating some of the analysis.

The second component of the pipeline is a local Python program which
verifies data against schemas. The schema defines the structure of a
data file, as well as how the data file's variables relate to variables
in the data model. It is possible to link to variables in other schemas,
some of which may not be in the parent data model. The Python program
creates a manifest in RDF/TTL format, which, at minimum, defines which a
schema which relates to a given data file. (A given schema may define
more than one data file, but there should not be more than one instance
of the data file in the manifest.) The manifest is actually intended to
define a set of permissable/possible jobs to run on the pipeline.

The third component of the pipeline is the "RAP" itself. This runs on a
remote server and monitors some storage somewhere (currently, this is
Google 'cloud' storage) for job/data submissions. The RAP is written in
Elixir/Erlang due to the extensive library support for 'stages' (GenStage
/ Broadway) as well as its concurrency and fault-tolerant properties.
While something like Python may be more widely used, the tooling for
writing these pipelines is somewhat limited, as is Python's concurrency
support and support for functional programming.

## The pipeline step-by-step

| *Step* | *Operation*                                        | *Location*                                                                                                             |
|--------|----------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| 0.1    | Modelling strategy                                 | Local machine                                                                                                          |
| 0.2    | Preparation/clean-up                               | Local machine                                                                                                          |
| 1      | Verify schema, create job manifest (incl. UUID)    | Local machine: existing Python program OR web app served by RAP frontend (dependent on level of control over versions) |
| 2      | Upload job data-set to Google cloud bucket         | Local machine: albeit backs onto GCP storage                                                                           |
| 3      | Append job metadata to queue and local DB          | RAP service: monitor GCP API for changes                                                                               |
| 4      | Fetch data required by job(s)                      | RAP service: pull via GCP API                                                                                          |
| 5      | Run job(s)                                         | RAP service                                                                                                            |
| 6      | Cache result(s) in local DB and/or data-set        | RAP service                                                                                                            |
| 7      | Create canonical URL of result and/or data-set     | RAP service                                                                                                            |

## Design of the RAP

The RAP uses the Elixir GenStage framework. This has three concepts of a
stage: a producer, a consumer, and a hybrid consumer-producer. It also
has a concept of "back-pressure" as data passes through the pipeline,
to manage demand.

The stage where concurrency will be of most benefit is in the stage in
which we run a given job. So, only this stage is to be supervised, the
others are much simpler.

```
 _____________          ______________________          ____________________          _______________
|  Producer:  | ___|\  |  Consumer-producer:  | ___|\  | Consumer-producer: | ___|\  |   Consumer:   |
|             |      \ |                      |      \ |                    |      \ |               |
| Monitor GCP | ___  / | Process job manifest | ___  / |     Run job(s)     | ___  / | Cache results |
|_____________|    |/  |______________________|    |/  |____________________|    |/  |_______________|

```
