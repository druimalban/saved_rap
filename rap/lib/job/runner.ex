defmodule RAP.Job.Runner do

  use GenStage
  require Logger

  alias RAP.Storage.PreRun
  alias RAP.Job.{Producer, Result}
  alias RAP.Job.ManifestSpec
  alias RAP.Provenance.Work

  def start_link initial_state do
    Logger.info "Called Job.Runner.start_link (_)"
    GenStage.start_link __MODULE__, initial_state, name: __MODULE__
  end

  def init initial_state do
    Logger.info "Called Job.Runner.init (initial_state = #{inspect initial_state})"
    curr_ts = DateTime.utc_now() |> DateTime.to_unix()
    subscription = [{ Producer, min_demand: 0, max_demand: 1 }]
    invocation_state = %{ initial_state |
			  stage_invoked_at:    curr_ts,
			  stage_type:          :producer_consumer,
			  stage_subscriptions: subscription,
			  stage_dispatcher:    GenStage.DemandDispatcher }
    
    { invocation_state.stage_type, invocation_state,
      subscribe_to: invocation_state.stage_subscriptions,
      dispatcher:   invocation_state.stage_dispatcher }
  end
  
  def handle_events events, _from, state do
    ie = inspect events
    is = inspect state
    Logger.info "Called Job.Runner.handle_events (events = #{ie}, _, state = #{is})"
    input_work = events |> Enum.map(& &1.work)
    Logger.info "Job.Runner received objects with the following work defined: #{inspect input_work}"
    
    target_events = events
    |> Enum.map(&process_jobs(&1, state.cache_directory, state.python_call, state.stage_invoked_at, state.stage_type, state.stage_subscriptions, state.stage_dispatcher))
    
    { :noreply, target_events, state }
  end
  
  def process_jobs(%ManifestSpec{signal: :working, jobs: staging} = spec, cache_directory, python_call, stage_invoked_at, stage_type, stage_subscriptions, stage_dispatcher) do  
    Logger.info "Staging jobs: #{inspect staging}"
    work_started_at = DateTime.utc_now() |> DateTime.to_unix()
    
    result_contents = staging
    |> Enum.map(&Result.run_job(spec.uuid, cache_directory, python_call, spec.base_prefix, &1))

    # Do need to have a notion of different signals
    overall_signal =
      if Enum.any?(result_contents, &(&1.signal == :working)) do
	:working
      else
	:job_errors
      end

    overall_work = Work.append_work(spec.work, __MODULE__, overall_signal, work_started_at, stage_invoked_at, stage_type, stage_subscriptions, stage_dispatcher)

    %{ spec | signal: overall_signal, results: result_contents, work: overall_work }
  end

  def process_jobs(%ManifestSpec{signal: :see_pre} = spec, _cache, _interpreter, stage_invoked_at, stage_type, stage_subscriptions, stage_dispatcher) do
    work_started_at = DateTime.utc_now() |> DateTime.to_unix()
    overall_work = Work.append_work(spec.work, __MODULE__, :see_pre, work_started_at, stage_invoked_at, stage_type, stage_subscriptions, stage_dispatcher)
    %{ spec | signal: :see_pre, work: overall_work }
  end

  @doc """
  # :empty_manifest | :bad_input_graph | :bad_manifest_tables
  # We thus have access to anything in the `minimal_manifest/2' function:

  def minimal_manifest(%MidRun{} = prev, curr_signal) do
    %ManifestSpec{ name:               prev.manifest_name,
		   uuid:               prev.uuid,
		   pre_signal:         prev.signal,
		   signal:             curr_signal,
		   manifest_base_ttl:  prev.manifest_ttl,
		   manifest_base_yaml: prev.manifest_yaml,
		   resource_bases:     prev.resources    }
  end
  """
  def process_jobs(%ManifestSpec{} = spec, _cache, _interpreter, stage_invoked_at, stage_type, stage_subscriptions, stage_dispatcher) do
    work_started_at = DateTime.utc_now() |> DateTime.to_unix()
    overall_work = Work.append_work(spec.work, __MODULE__, :see_producer, work_started_at, stage_invoked_at, stage_type, stage_subscriptions, stage_dispatcher)
    %{ spec | signal: :see_producer, work: overall_work }
  end
end
