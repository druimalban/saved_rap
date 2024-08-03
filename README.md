# SAVED RAP service: data ingestion and model validation pipeline

## SAVED: Sustainable Aquaculture: Validating Sea Lice Dispersal [models]

[SAVED](https://www.sustainableaquaculture.com/news-events/new-tool-being-developed-to-validate-sea-lice-dispersion-models/) is a [SAIC](https://www.sustainableaquaculture.com/)-funded effort led by the Scottish Government Marine Directorate with academic and industrial partners. The aim is to develop a standardised way to validate sea lice dispersal models.

## RAP

"RAP" stands for reproducible analytical pipeline. This is a term commonly used in the civil service and public sector, and it is useful as it is largely self-explanatory and succint. The term is typically used to refer to the process of producing statistical reports and producing website documentation, whereas our use here is somewhat more expansive.

This repository hosts the pipeline component of the RAP, which we're using to validate dispersal models. The civil service's conception of RAP is a set of working practices emphasising use of open source tools and collaboration. For SAVED, we consider our RAP to be the entire set of tooling we developed from start to finish: 

1. Our [data model/ontology](https://marine.gov.scot/metadata/saved/schema/), which we developed to describe data in an agreed, common way;
2. Our [local Python upload utilities](https://github.com/saved-models/data-utilities), (`fisdat(1)` and `fisup(1)`), which let us validate data against schemata written in YAML (using [LinkML](https://w3id.org/linkml/));
3. This model validation pipeline program (the "RAP service"), implemented using Elixir and Erlang/OTP, which we use to validate dispersal model output against observations, such as the [2011-2013 sentinel cages sampling exercise](https://data.marine.gov.scot/dataset/loch-linnhe-biological-sampling-data-products-2011-2013-0).

## Pipeline technical design

Input data are RDF job descriptions prepared using our [data upload utilities](https://github.com/saved-models/data-utilities). The RDF structure of these is designed to be general enough to be applicable to a variety of different job types and data shape/format. Jobs are external scripts/programs with a common calling convention.

The pipeline is written in Elixir, which is a fairly new programming language implemented on top of Erlang/OTP. The pipeline uses the [GenStage library](https://elixir-lang.org/blog/2016/07/14/announcing-genstage/). This worked quite well in practice as stages are processes running on the Erlang BEAM virtual machine, and GenStage provides the machinery to handle back-pressure and demand in the way that OTP provides the machinery to handle message-handling and fault tolerance. Additionally, the [RDF tooling](https://rdf-elixir.dev/) is fairly mature and worked really well, especially its mapping between RDF data schemas and Elixir structs, since functional programming languages like Elixir and Erlang are declarative.

## Modelling work and results

As well as model validation results, the pipeline outputs a description of **processing** or work done by the pipeline, as an RDF graph. This uses the [PROV ontology](https://www.w3.org/TR/2013/REC-prov-o-20130430/), which is particularly neat, as its semantics map remarkably well to Elixir and Erlang/OTP. Specifically:

1. PROV [Agents](https://www.w3.org/TR/2013/REC-prov-o-20130430/#Agent) (specifically, [SoftwareAgents](https://www.w3.org/TR/2013/REC-prov-o-20130430/#SoftwareAgent)) map closely to GenStage's stages, as well as the pipeline OTP application. It may apply even more generally than this, probably to GenServer, and perhaps any process running on the Erlang BEAM VM.
2. PROV [Activities](https://www.w3.org/TR/2013/REC-prov-o-20130430/#Activity) model work/processing done by stages on an event, as well as invocation of stages and the pipeline OTP application.
3. PROV [Entities](https://www.w3.org/TR/2013/REC-prov-o-20130430/#Entity) model final output produced by a pass through the pipeline of a submitted data manifest, in addition to output produced by individual stages, and results of jobs.

Output is ['baked'](https://simonwillison.net/2021/Jul/28/baked-data/) into a web page, which is the primary way that end-users receive feedback. This web page describes data which were submitted, and results and any descriptive statistics are visualised, depending on the job type.


## Pipeline demo

We have a demo running, [kindly hosted on a machine in Edinburgh](https://rap.tardis.ac/).

![saved_fisdat](https://rap.tardis.ac/saved/images/fisdat.svg)
![saved_rap](https://rap.tardis.ac/saved/images/rap.svg)
