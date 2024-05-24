defmodule RAP.Job.ScopeSpec do
  @moduledoc """
  Simple struct for recording whether a given column is valid.
  There exists a mapping from this representation to an original
  `%ScopeDesc{}' struct.

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

  This struct does duplicate the purported path (base name, to be exact)
  of the referenced table. This is necessary for pattern matching over
  processed job descriptions, and does not occur in our RDF manifests.

  There are both a URI
  (e.g., `https://marine.gov.scot/metadata/saved/column_descriptive')
  and CURIE (e.g. `saved:column_descriptive') fields. Resolving the URI
  into a CURIE is useful for reporting, i.e. use this in generated HTML
  documents instead of the full URI. Doing this robustly involves
  querying an RDF graph's prefix map, so we add a private field to the
  Grax struct which is filled out on loading the RDF graph using Grax.

  In addition to the base name of the resource file, further record th
  atomic name of the table, which is useful for generating reporting.
  """
  defstruct [ :variable_uri,  :variable_curie, :column,
	      :resource_name, :resource_base ]
end

defmodule RAP.Job.ResourceSpec do
  @moduledoc """
  Slightly different to the above in that the validity should be defined
  on the file level. There's only one resource and schema per table,
  unlike columns where there's a bunch of columns per job from many
  places.

  In the generated manifests, this level of nesting does not exist
  because the fields are arbitrary, it's only in the Elixir RAP that we
  actually validate these. These are just named pairs of the resource
  path and whether the resource exists.
  """
  defstruct [ :base, :extant ]
end

defmodule RAP.Job.TableSpec do
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

`  The `resource' and `schema' attributes are not fully qualified file
  names or base file names, but instances of the above `%ResourceSpec{}'
  struct.
  """
  defstruct [ :name, :title, :description, :resource, :schema ]
end

defmodule RAP.Job.JobSpec do
  @moduledoc """
  Simple struct for recording both errors and correct paired columns
  These are not blank nodes and so are associated with a name like a
  table description.
  """
  defstruct [ :name, :title, :description, :type,
	      :scope_descriptive,   :scope_collected,  :scope_modelled,
	      :errors_descriptive,  :errors_collected, :errors_modelled ]
end

defmodule RAP.Job.ManifestSpec do
  @moduledoc """
  While the manifest is associated with a name (it's not a blank node),
  this is always hard-coded as `RootManifest'.

  This module reincoprorates the UUID, path of the manifest proper, and
  paths of the various resources associated with the job.
  """
  defstruct [ :title,
	      :description,    :local_version,
	      :uuid,
	      :manifest_base,  :resource_bases,
	      :staging_tables, :staging_jobs ]
end

