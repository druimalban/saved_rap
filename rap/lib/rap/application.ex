defmodule RAP.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  # Hard-code these for brevity, should be configurable at program init.
  # Same cache dir for both the 'local' store and GCP
  @interval_seconds 10
  @cache_directory  "./data_cache"
  @index_file       ".index"
  @index_fall_back  "manifest.ttl"
  @gcp_bucket       "saved-rap-test"
  @local_directory  "/var/db/saved"

  # This is the one file name which must be hard-coded, because there's
  # no way to know which file is which. The manifest describes all other
  # files of interest, and further describes the shape of the job and
  # the results to be run.
  
  
  use Application

  @doc
  """
  Initial / global pipeline state

  Define a number of hard-coded attributes here, this makes it very easy
  to be able to read these later as CLI arguments. There are quite a few
  of these and the named struct makes pattern matching work well, and can
  also accept nil values - e.g. we don't define a connection here, but in
  the `RAP.Storage.Monitor' producer stage.

  Use a single GCP bucket for now, because in practice, if we want to
  monitor more than one, this would imply a distinct stages, since it is
  presumed that the reason behind using multiple buckets is to treat the
  data contained within differently.
  """
  defstruct [ :interval_seconds,
	      :cache_directory,
	      :index_file,
	      :gcp_bucket,
	      :local_directory,
	      :gcp_session,
	      :last_poll,
	      :staging_objects,
	      :poll_signal ]
  
  @impl true
  def start(_type, _args) do
    current_ts = DateTime.utc_now() |> DateTime.to_unix()
    initial_ts = current_ts - @interval_seconds
    
    children = [
      {RAP.Storage.Monitor,
       %RAP.Application{
	 interval_seconds: @interval_seconds,
	 cache_directory:  @cache_dir,
	 index_file:       @index_file,
	 gcp_bucket:       @gcp_bucket,
	 local_directory:  @local_source_directory,
	 gcp_session:      nil,
	 last_poll:        initial_ts,
	 staging_objects:  [],
	 poll_signal:      :continue
       }},      
      #RAP.Storage.TestConsumer
      RAP.Storage.GCP,
      RAP.Job.Producer,
      RAP.Job.Runner,
      RAP.Job.Cache.Supervisor
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: RAP.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
