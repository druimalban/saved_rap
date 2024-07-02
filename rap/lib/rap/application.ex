defmodule RAP.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false
  #
  # Hard-code these for brevity, should be configurable at program init.
  # Same cache directory for both the 'local' store and objects fetched
  # from GCP. Additional 'local' directory, i.e. a directory on the local
  # filesystem which we monitor for changes, rather than a place to *put*
  # things monitored locally.
  #
  @interval_seconds   300
  @index_file         ".index"
  @index_fall_back    "manifest.ttl"
  @gcp_bucket         "saved-rap-test"
  @local_directory    "/var/db/saved"  # Monitor this like GCP
  @cache_directory    "./data_cache"
  @bakery_directory   "./bakery"       # The place to output results
  @linked_result_stem "manifest_post"
  @time_zone          "GB-Eire"
  @rap_uri_prefix     "/saved/rap"
  @rap_style_sheet    "/saved/rap/assets/app.css"
  @rap_js_lib_plotly  "/saved/rap/assets/plotly-2.32.0.min.js"
  @rap_js_lib_d3      "/saved/rap/assets/d3.v7.min.js"
  @html_directory     "./html_fragments"
  
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
  defstruct [ :gcp_session,
	      :last_poll,	      
	      interval_seconds:   @interval_seconds,
	      index_file:         @index_file,
	      index_fall_back:    @index_fall_back,
	      gcp_bucket:         @gcp_bucket,
	      local_directory:    @local_directory,
	      cache_directory:    @cache_directory,
	      bakery_directory:   @bakery_directory,
	      linked_result_stem: @linked_result_stem,
	      time_zone:          @time_zone,
	      rap_uri_prefix:     @rap_uri_prefix,
	      rap_style_sheet:    @rap_style_sheet,
	      rap_js_lib_plotly:  @rap_js_lib_plotly,
	      rap_js_lib_d3:      @rap_js_lib_d3,
	      html_directory:     @html_directory,
	      staging_objects:    []               ]
  
  @impl true
  def start(_type, _args) do
    # Fudge: set initial time stamp to five minutes (or given interval)
    # in the past so that the monitoring triggers immediately.
    current_ts = DateTime.utc_now() |> DateTime.to_unix()
    initial_ts = current_ts - @interval_seconds

    hardcoded_state = %RAP.Application{
      last_poll: initial_ts
    }
    
    children = [
      {RAP.Storage.Monitor, hardcoded_state},      
      {RAP.Storage.GCP,     hardcoded_state},
      {RAP.Job.Producer,    hardcoded_state},
      {RAP.Job.Runner,      hardcoded_state},
      {RAP.Bakery.Prepare,  hardcoded_state},
      {RAP.Bakery.Compose,  hardcoded_state}
      #RAP.Job.Cache.Supervisor,
      #RAP.Storage.TestConsumer
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: RAP.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
