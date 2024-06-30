# SAVED reproducible analytical pipeline (RAP) overview

## About RAP

"RAP" stands for [reproducible analytical pipeline](https://ukgovdatascience.github.io/rap_companion/). This is a term commonly used in the civil service, and it is useful as it is largely self-explanatory and succint. The term is typically used to refer to the process of producing statistical reports and producing website documentation, whereas our use here is somewhat more expansive.

## Components of the pipeline

The first component of the pipeline is a data model based on LinkML, which defines variables which we agree should be used by those of us sharing data between each other. Not every variable in data provided will be in this data model. These data models are sometimes known as ontologies, and are part of the wider semantic web. The advantage of LinkML is that the schemata (YAML) are relatively easy to write compared to JSON or RDF/TTL, and that it allows us to define links between data consistently and in a machine-readable fashion. Linking data and agreeing on how to share it is a key element of the project overall, and is a pre-requisite to automating some of the analysis.

The second component of the pipeline is a collection of local Python programs (`fisdat(1)` and `fisup(1)`) which verify data against schema files, producing a job 'manifest' file, which may then be edited to describe jobs to be run on the data. This description is then used to upload the package of data, metadata, and job description, for subsequent processing. Schema files define the structure of any associated data files (a single schema may describe more than one data file), as well as how variables in data-sets relate to an *underlying* variable (or variables) in our own data model or in other data models/ontologies. Indeed, it is further possible to link variables across schemas, some of which may not be in the parent data model or commonly-used ontologies, such as specific column names/aliases. 

The third component of the pipeline is subsequent processing of our data. This is a computer program which monitors some location for data uploaded, runs jobs described by the data, then 'bakes' the data into a package describing input, jobs specified, and output. A job is typically associated with some computer program run by the pipeline, and the package produced by the pipeline includes a static web page describing results, including any descriptive statistics or visualisations which can be generated. This is pushed to a well-known location, allowing results to be retrieved using a web browser.


## The pipeline step-by-step

| *Step* | *Operation*                                        | *Location*                                                                                                             |
|--------|----------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| 0.1    | Modelling strategy                                 | Local machine                                                                                                          |
| 0.2    | Preparation/clean-up                               | Local machine                                                                                                          |
| 1      | Verify schema, create job manifest (incl. UUID)    | Local machine: existing Python program OR web app served by RAP frontend (dependent on level of control over versions) |
| 2      | Upload job data-set to storage service             | Local machine: albeit currently backs onto GCP storage                                                                 |
| 3      | Append job metadata to queue and local DB          | RAP service: monitor storage for changes                                                                               |
| 4      | Fetch data required by job(s)                      | RAP service: pull any changes from storage                                                                             |
| 5      | Run job(s)                                         | RAP service                                                                                                            |
| 6      | Cache result(s) and/or data-set                    | RAP service                                                                                                            |
| 7      | Generate HTML results, including any errors and any descriptive statistics/visualisations applicable | RAP service and/or external web server |

## Technical aspects of the pipeline program

Notably, this processing of data is distinctly decoupled from the place to which data are submitted, in the sense that this is a computer program monitoring multiple locations on the net to which data may be submitted. This may be 'cloud' storage services like Amazon Web Services or Google Cloud Platform (GCP), another computer program implementing an object store not unlike these services, or some directory local to the computer program. The current implementation monitors a 'bucket' on the Google Cloud Storage service.

The computer program in question is written in the Elixir programming language (which runs on the Erlang/OTP platform) due to the extensive library support for 'stages' (GenStage) as well as Elixir and Erlang/OTP's common concurrency and fault-tolerant properties. This should allow the RAP program to scale (including across multiple computers), which is especially important if we use the thing extensively, or which components we run on different machines. 

While something like Python may be more widely used, the tooling for writing a data ingestion pipeline like ours is somewhat limited, as is Python's concurrency support and support for functional programming. Therefore, even if Python had been selected, there would have been a number of technical issues to solve or be re-implemented.
