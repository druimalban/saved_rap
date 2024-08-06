defmodule RAP.Job.Runner do

  use GenStage
  require Logger
  
  alias RAP.Job.{Producer, Result}
  alias RAP.Job.ManifestSpec
  alias RAP.Provenance.Work

  @type stage_dispatcher   :: GenStage.BroadcastDispatcher | GenStage.DemandDispatcher | GenStage.PartitionDispatcher
  @type stage_type         :: :consumer | :producer_consumer | :producer
  @type stage_subscription :: {atom(), min_demand: integer(), max_demand: integer()}
  @type stage_state :: %{
    stage_dispatcher:    stage_dispatcher(),
    stage_invoked_at:    integer(),
    stage_subscriptions: [stage_subscription()],
    stage_type:          atom()
  }

  def start_link([] = initial_state) do
    Logger.info "Start link to Job.Runner"
    GenStage.start_link __MODULE__, initial_state, name: __MODULE__
  end

  def init([] = _initial_state) do
    Logger.info "Initialise Job.Runner"
    curr_ts = DateTime.utc_now() |> DateTime.to_unix()
    subscription = [{ Producer, min_demand: 0, max_demand: 1 }]
    stage_state = %{ stage_invoked_at:    curr_ts,
		     stage_type:          :producer_consumer,
		     stage_subscriptions: subscription,
		     stage_dispatcher:    GenStage.DemandDispatcher }    
    { stage_state.stage_type, stage_state,
      subscribe_to: stage_state.stage_subscriptions,
      dispatcher:   stage_state.stage_dispatcher }
  end
  
  def handle_events(events, _from, stage_state) do
    Logger.info "Called Job.Runner.handle_events (events = #{inspect events}, state = #{inspect stage_state})"
    input_work = events |> Enum.map(& &1.work)
    Logger.info "Job.Runner received objects with the following work defined: #{inspect input_work}"
    
    target_events = events
    |> Enum.map(&process_jobs(&1, stage_state))
    { :noreply, target_events, stage_state }
  end

  @spec process_jobs(%ManifestSpec{}, stage_state()) :: %ManifestSpec{}
  def process_jobs(%ManifestSpec{signal: :working, jobs: staging} = spec, stage_state) do  
    Logger.info "Staging jobs: #{inspect staging}"

    work_started_at = DateTime.utc_now() |> DateTime.to_unix()
    
    result_contents = staging
    |> Enum.map(&Result.run_job(&1, spec.uuid, spec.base_prefix))

    # Do need to have a notion of different signals
    overall_signal =
      if Enum.any?(result_contents, &(&1.signal not in [:working, :ignored])) do
	:job_errors
      else
	:working
      end

    overall_work = Work.append_work(spec.work, __MODULE__, overall_signal, work_started_at, stage_state)

    %{ spec | signal: overall_signal, results: result_contents, work: overall_work }
  end

  def process_jobs(%ManifestSpec{signal: :see_pre} = spec, stage_state) do
    work_started_at = DateTime.utc_now() |> DateTime.to_unix()
    overall_work = Work.append_work(spec.work, __MODULE__, :see_pre, work_started_at, stage_state)
    %{ spec | signal: :see_pre, work: overall_work }
  end

  @doc """
  # :empty_manifest | :bad_input_graph | :bad_manifest_tables
  # We thus have access to anything in the `minimal_manifest' function
  """
  def process_jobs(%ManifestSpec{} = spec, stage_state) do
    work_started_at = DateTime.utc_now() |> DateTime.to_unix()
    overall_work = Work.append_work(spec.work, __MODULE__, :see_producer, work_started_at, stage_state)
    %{ spec | signal: :see_producer, work: overall_work }
  end
end
