defmodule RAP.Job.Spec do

  defmodule Column do
    @moduledoc """
    Simple struct for recording whether a given column is valid.
    There exists a mapping from this representation to an original
    `%ColumnDesc{}' struct.

    In the original usage of this (I had called this `Staging'), I had a
    struct attribute called `target'. This had two uses: the IRI of the
    target table, if that table did not exist; or the description of the
    table proper if it did exist. This dual use is really confusing, so 
    I've renamed it `table'.

    I further had a struct attribute called `signal' which I used to record
    whether the table exists or not. This is actually implicit, and is
    recorded at the above level by the job in the `errors_descriptive'
    (&c.) attribute, so don't duplicate this.

    In the generated manifests, column descriptions are blank nodes.
    """
    defstruct [ :variable, :column, :table ]

    #alias RAP.Manifest.ColumnDesc
    #alias RAP.Job.Spec.Column
    #
    #def to_column_desc(%Column{variable: var, label: col, table: table_uri}) do
    #  %ColumnDesc{column: col, variable: var, table: table_uri}
    #end
  end

  defmodule Resource do
    @defmodule """
    Slightly different to the above in that the validity should be defined
    on the file level. There's only one resource and schema per table,
    unlike columns where there's a bunch of columns per job from many
    places.

    In the generated manifests, this level of nesting does not exist
    because the fields are arbitrary, it's only in the Elixir RAP that we
    actually validate these. These are just named pairs of the resource
    path and whether the resource exists.
    """
    defstruct [ :path, :extant ]
  end
  defmodule Table do
    @defmodule """
    The `resource' and `schema' attributes are named resource structs as
    above, analoguous to columns. 

    However, unlike columns, there must be at most one resource file and
    one schema file (ignoring multiple formats of schemata, LinkML YAML
    and generated RDF equivalent), so in terms of pattern matching, it is
    not desirable to have multiple attributes like `invalid_resource',
    `valid_resource', `invalid_schema', `valid_schema'.

    Unlike the columns, which are blank nodes, tables are named and so can
    be associated with a name (the struct's generated `__id__' attribute).
    """
    defstruct [ :name, :title, :resource, :schema ]
  end

  defmodule Job do
    @moduledoc """
    Simple struct for recording both errors and correct paired columns
    These are not blank nodes and so are associated with a name like a
    table description.
    """
    defstruct [ :name, :title, :description,:type,
		:scope_descriptive,   :scope_collected,  :scope_modelled,
		:errors_descriptive,  :errors_collected, :errors_modelled ]

    #alias RAP.Manifest.{ColumnDesc, JobDesc}
    #alias RAP.Job.Spec.{Column,     Job}
    #
    #def to_job_desc(%Job{} = job) do
    #  %JobDesc{
    #    title:                 job.title,
    #    description:           job.description,
    #    job_type:              job.type
    #    job_scope_descriptive: job.scope_descriptive,
    #    job_scope_collected:   job.scope_collected,
    #    job_scope_modelled:    job.scope_modelled
    #  }
    #end
  end

  defmodule Manifest do
    @moduledoc """
    At the moment, this module just includes a single struct definition.
    The aim here is to separate the declaration of the structure of the
    job from the structure of the producer, which holds its own state.

    While the manifest is associated with a name (it's not a blank node),
    this is hard-coded as RootManifest.
    """

    defstruct [ :title,
		:description,    :local_version,
		:manifest_path,  :resources,
		:staging_tables, :staging_jobs ]
  end

end
