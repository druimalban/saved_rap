defmodule RAP.Provenance.RAPProcess do

  use Grax.Schema, depth: +5
  import RDF.Sigils
  alias RDF.NS.RDFS
  alias RAP.Vocabulary.{PROV, SAVED}

  schema SAVED.RAPStage do
    property :label, RDFS.label, type: :string
  end

  def on_to_rdf(%__MODULE__{__id__: id} = agent, graph, _opts) do
    {:ok, RDF.Graph.add(graph, RDF.type(id, PROV.SoftwareAgent))}
  end
end

defmodule RAP.Provenance.RAPStageSubscription do

  use Grax.Schema, depth: +5
  import RDF.Sigils

  alias RDF.NS.RDFS
  alias RAP.Vocabulary.{PAV, PROV, DCAT, SAVED}

  schema SAVED.RAPStageSubscription do
    property :label,         RDFS.label,                 type: :string
    property :subscribed_to, SAVED.beam_module,          type: :iri
    property :min_demand,    SAVED.gen_stage_min_demand, type: :integer
    property :max_demand,    SAVED.gen_stage_max_demand, type: :integer
  end
  
end

defmodule RAP.Provenance.RAPInvocation do

  use Grax.Schema, depth: +5
  import RDF.Sigils

  alias RDF.NS.RDFS
  alias RAP.Vocabulary.{PAV, PROV, DCAT, SAVED}
  alias RAP.Provenance.{RAPProcess, RAPStageSubscription}

  schema SAVED.RAPInvocation do
    property :label,                RDFS.label,                   type: :string
    property :version,              PAV.version,                  type: :string
    property :beam_application,     SAVED.beam_application,       type: :string
    property :beam_node,            SAVED.beam_node,              type: :string
    property :beam_module,          SAVED.beam_module,            type: :string
    property :beam_module_pid,      SAVED.beam_module_pid,        type: :string
    property :otp_version,          SAVED.otp_version,            type: :string
    property :elixir_version,       SAVED.elixir_version,         type: :string
    property :gen_stage_type,       SAVED.gen_stage_type,         type: :string
    property :gen_stage_dispatcher, SAVED.gen_stage_dispatcher,   type: :string
    property :started_at,           PROV.startedAtTime,           type: :date_time
    link associated_with:           PROV.wasAssociatedWith,       type: list_of(RAPProcess)
    link gen_stage_subscriptions:   SAVED.gen_stage_subscription, type: list_of(RAPStageSubscription)
  end

  def on_to_rdf(%__MODULE__{__id__: id} = agent, graph, _opts) do
    {:ok, RDF.Graph.add(graph, RDF.type(id, PROV.Activity))}
  end
end

defmodule RAP.Provenance.RAPStageProcessing do

  use Grax.Schema, depth: +5
  import RDF.Sigils

  alias RDF.NS.RDFS
  alias RAP.Vocabulary.{PAV, PROV, DCAT, SAVED}
  alias RAP.Provenance.RAPStageResponse

  schema SAVED.RAPStageProcessing do
    property :label,                RDFS.label,             type: :string
    property :started_at,           PROV.startedAtTime,     type: :date_time
    property :ended_at,             PROV.endedAtTime,       type: :date_time
    property :associated_with,      PROV.wasAssociatedWith, type: list_of(:iri)
    property :used_previous_output, PROV.used,              type: list_of(:iri)
    property :signal_text,          SAVED.stage_signal,     type: :string
    link generated_entities: PROV.generated, type: list_of(RAPStageResponse)
  end

  def on_to_rdf(%__MODULE__{__id__: id} = agent, graph, _opts) do
    {:ok, RDF.Graph.add(graph, RDF.type(id, PROV.Activity))}
  end
end

defmodule RAP.Provenance.RAPStageResponse do

  use Grax.Schema, depth: +5
  import RDF.Sigils
  
  alias RDF.NS.RDFS
  alias RAP.Vocabulary.{PAV, PROV, DCAT, SAVED}

  schema SAVED.RAPStageResponse do
    property :label,        RDFS.label,          type: :string
    property :derived_from, PROV.wasDerivedFrom, type: list_of(:iri)
  end

  def on_to_rdf(%__MODULE__{__id__: id} = agent, graph, _opts) do
    {:ok, RDF.Graph.add(graph, RDF.type(id, PROV.Entity))}
  end
end

defmodule RAP.Provenance.Work do

  alias RAP.Provenance.{RAPProcess, RAPInvocation, RAPStageSubscription, RAPStageProcessing, RAPStageResponse}
  require Logger

  def append_work(past_work, stage_atom, curr_signal, work_started_at, %{} = stage_state, work_input \\ [], work_output \\ []) do
    work_ended_at =  DateTime.utc_now() |> DateTime.to_unix()
    curr_pid = self() |> :erlang.pid_to_list() |> to_string()
    
    work = [{stage_atom,
	     %{
	       stage_pid:           curr_pid,
	       stage_invoked_at:    stage_state.stage_invoked_at,
	       stage_type:          stage_state.stage_type,
	       stage_subscriptions: stage_state.stage_subscriptions,
	       stage_dispatcher:    stage_state.stage_dispatcher,
	       signal:              curr_signal,
	       work_started_at:     work_started_at,
	       work_ended_at:       work_ended_at,
	       work_input:          work_input,
	       work_output:         work_output
	     }}]
    if is_nil(past_work) do
      work
    else
      past_work ++ work
    end
  end
  
  @doc """
  An event is notionally a submitted manifest, which moves through the pipeline.
  As it does so, it records work at each stage, which is associated with the following structs:
  1. A well-defined stage software agent, we basically model the OTP module name
  2. The original invocation activity of that stage by the RAP, which includes information about the stage
  3. The processing activity done on the manifest as it passed through the stage
  4. Notional output entity of the processing activity, which primarily models stage order (which may not be linear if processes were dynamically started by supervisors)

  Note, what actually invoked the stage invocation activity isn't the stage, but the RAP application. However, the RAP application is in turn invoked by something which we don't model as it's out of scope. The modelling here is slightly vague in the sense that a stage's invocation is also probably associated with the stage agent (and I model it as such)

  We are primarily interested in activities which went on in the pipeline, so these are primarily modelled in the serialised RDF output in terms of activities as the manifest passed through the pipeline, as opposed to just listing stages, which aren't very informative on their own.

  Currently broken in the sense that sorting out what produces what is vague
  """  
  def traverse_work(work, base_prefix, rap_invoked_at, final_output_iris) do
    rap_prefix = RAP.Vocabulary.RAP.__base_iri__
    
    app_agent      = app_agent(rap_prefix)
    app_invocation = app_invocation_activity(app_agent, base_prefix, rap_invoked_at)
    
    produce =
      fn({st, wd}, prev_prod) ->
	stage_in_scope   = gen_stage_agent(st, rap_prefix)
	
	stage_invocation = gen_stage_invocation_activity(
	  st, stage_in_scope, app_agent, base_prefix, rap_prefix, wd
	)
	staged_work_output = gen_stage_output_entity(
	  st, base_prefix, wd.stage_pid, prev_prod
	)
	staging_processing = gen_stage_processing_activity(
	  st, base_prefix, rap_prefix, wd, [staged_work_output.__id__]
	)
	%{ stage:      stage_in_scope,
	   invocation: stage_invocation,
	   processing: staging_processing,
	   production: staged_work_output }
    end
    
    collapse = fn
      {st, wd}, nil ->
	trans = produce.({st, wd}, nil)
        {trans, trans}
      {st, wd}, acc ->
        trans = produce.({st, wd}, acc.processing.__id__)
        {trans, trans}
    end

    {last_key, _last_work} = Enum.at(work, -1)

    gather_base_case = %{stages: [], invocations: [], processing: [], }
    stages_work = work
    |> put_in([last_key, :work_output], final_output_iris)
    |> Enum.map_reduce(nil, collapse)
    |> elem(0)
    |> Enum.reduce(
         gather_base_case,
         fn(curr, acc) ->
	   res = %{ stages:      acc.stages      ++ [curr.stage],
		    invocations: acc.invocations ++ [curr.invocation],
		    processing:  acc.processing  ++ [curr.processing] }
         end)
    Map.merge(stages_work, %{app_agent: app_agent, app_invocation: app_invocation})
  end
  
  defp uncase(module_atom) do
    module_atom
    |> to_string()
    |> String.split(".")
    |> Enum.at(-1)
    |> Macro.underscore()
  end
  
  def app_agent(rap_prefix) do
    # RAP.Application -> "rap_application"
    agent_iri = RDF.IRI.new(rap_prefix <> "application")
    agent_lbl = "RAP OTP application agent"
    %RAPProcess{ __id__: agent_iri, label:  agent_lbl }
  end
  def gen_stage_agent(stage_atom, rap_prefix) do
    agent_name = uncase(stage_atom)
    agent_iri  = RDF.IRI.new(rap_prefix <> "stage_" <> agent_name)
    agent_lbl = "#{stage_atom} stage agent"
    %RAPProcess{ __id__: agent_iri, label:  agent_lbl }
  end
  
  def app_invocation_activity(app_agent, base_prefix, invoked_at) do
    with {:ok, time_zone} <- Application.fetch_env(:rap, :time_zone),
	 [_|_] = spec     <- Application.spec(:rap),
         local_version    <- Keyword.get(spec, :vsn) do
      
      invocation_ts = invoked_at
      |> DateTime.from_unix!()
      |> DateTime.shift_zone!(time_zone)

      invocation_iri = RDF.IRI.new(base_prefix <> "application_invocation")
      invocation_lbl = "OTP application invocation activity"
      
      %RAPInvocation{
	__id__:           invocation_iri,
	label:            invocation_lbl,
	version:          to_string(local_version),
	beam_application: "rap",
	beam_node:        to_string(node()),
	otp_version:      System.otp_release(),
	elixir_version:   System.version(),
	started_at:       invocation_ts,
	associated_with:  [app_agent]
      }
    else
      :error -> {:error, "Cannot fetch keys from RAP configuration"}
    end
  end

  def gen_stage_subscription({target_module, min_demand: min, max_demand: max}, rap_prefix) do
    sub_iri = RDF.IRI.new(rap_prefix <> "stage_" <> uncase(target_module))
    %RAPStageSubscription{
      __id__:        RDF.BlankNode.new(),
      subscribed_to: sub_iri,
      min_demand:    min,
      max_demand:    max
    }
  end
  
  def gen_stage_invocation_activity(
    stage_atom,
    stage_agent, app_agent,
    base_prefix, rap_prefix,
    %{} = work
  ) do
    with {:ok, time_zone} <- Application.fetch_env(:rap, :time_zone),
	 [_|_] = spec     <- Application.spec(:rap),
         local_version    <- Keyword.get(spec, :vsn) do
    
      invocation_ts = work.stage_invoked_at
      |> DateTime.from_unix!()
      |> DateTime.shift_zone!(time_zone)

      stage_norm    = uncase(stage_atom)
      stage_inv_iri = RDF.IRI.new(base_prefix <> "stage_" <> stage_norm <> "_invocation")
      stage_inv_lbl = "#{stage_atom} (#{work.stage_pid}) stage invocation activity"
      
      subscriptions = work.stage_subscriptions
      |> Enum.map(&gen_stage_subscription(&1, rap_prefix))
    
      %RAPInvocation{
	__id__:                  stage_inv_iri,
	label:                   stage_inv_lbl,
	version:                 to_string(local_version),
	beam_application:        "rap", #:rap
	beam_node:               to_string(node()),
	beam_module:             to_string(stage_atom), # key provided as stage_atom arg, not in the work map
	beam_module_pid:         work.stage_pid,
	otp_version:             System.otp_release(),
	gen_stage_type:          to_string(work.stage_type),
	gen_stage_subscriptions: subscriptions,
	gen_stage_dispatcher:    to_string(work.stage_dispatcher),
	elixir_version:          System.version(),
	started_at:              invocation_ts,
	associated_with:         [app_agent, stage_agent]
      }
    else
      :error -> {:error, "Cannot RAP configuration fetch keywords"}
    end
  end
  
  def gen_stage_processing_activity(
    stage_atom,
    base_prefix, rap_prefix,
    %{} = work,
    output_entities \\ []
  ) do
    with {:ok, time_zone} <- Application.fetch_env(:rap, :time_zone) do
      stage_norm     = uncase(stage_atom)
      stage_iri      = RDF.IRI.new(rap_prefix <> "stage_" <> stage_norm)
      stage_proc_iri = RDF.IRI.new(base_prefix <> "stage_" <> stage_norm <> "_processing")
      stage_proc_lbl = "#{stage_atom} (#{work.stage_pid}) stage processing activity"
      
      start_ts = work.work_started_at
      |> DateTime.from_unix!() |> DateTime.shift_zone!(time_zone)
      end_ts = work.work_ended_at
      |> DateTime.from_unix!() |> DateTime.shift_zone!(time_zone)
      
      work_input_all  = work.work_input
      work_output_all = work.work_output ++ output_entities
      
      signal_text = to_string(work.signal)
            
      %RAPStageProcessing{
	__id__:               stage_proc_iri,
	label:                stage_proc_lbl,
	started_at:           start_ts,
	ended_at:             end_ts,
	associated_with:      [stage_iri],
	used_previous_output: work_input_all,
	generated_entities:   work_output_all,
	signal_text:          signal_text
      }
    else
      :error -> {:error, "Cannot RAP configuration fetch keywords"}
    end
  end

  def gen_stage_output_entity(stage_atom, base_prefix, stage_pid, prev_output_iris) do
    stage_norm = uncase(stage_atom)
    stage_output_iri = RDF.IRI.new(base_prefix <> "stage_" <> stage_norm <> "_output")
    stage_output_lbl = "#{stage_atom} (#{stage_pid}) stage ouptut entity"

    %RAPStageResponse{
      __id__:       stage_output_iri,
      label:        stage_output_lbl,
      derived_from: prev_output_iris
    }
  end
  
end
