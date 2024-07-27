defmodule RAP.Bakery.Provenance do

  use RDF
  alias RDF.NS.RDFS
  alias RAP.Vocabulary.{PAV, PROV, DCAT, SAVED, RAP}

  @doc """
  We have no notion of state, yet
  """
  def gen_invocation_activity(invocation_iri, application_iri, invoked_at, local_version) do
    invocation_ts = invoked_at
    #|> DateTime.from_unix!()
    #|> DateTime.shift_zone!(@time_zone)
    
    RDF.Description.new(invocation_iri)
    |> RDFS.label("RAP application invocation description")
    |> PAV.version(local_version)
    |> SAVED.beam_application("RAP")
    |> SAVED.beam_node(node())
    |> SAVED.beam_module(inspect __MODULE__)
    |> SAVED.otp_version(System.otp_release())
    |> SAVED.elixir_version(System.version())
    |> PROV.startedAtTime(invocation_ts)
    |> PROV.wasAssociatedWith(application_iri)
  end

  def traverse_work(work, base_prefix, rap_prefix, rap_invoked_at, tz) do
    produce =
      fn({st, wd}, prev) ->
	stage = gen_stage_entity(              st, rap_prefix)
	inv   = gen_stage_invocation_activity( st, base_prefix, rap_prefix, tz, wd)
	procs = gen_stage_processing_activity( st, base_prefix, rap_prefix, tz, wd)
	out   = gen_stage_output_entity(       st, base_prefix, prev)
	%{ stage: stage, invocation: inv, processing: procs, production: out }
      end
    collapse = fn
      {st, wd}, [] ->
	trans = produce.({st, wd}, [])
        {trans, trans}
      {st, wd}, acc ->
        trans = produce.({st, wd}, acc.production.subject)
        {trans, trans}
    end
    Enum.map_reduce(work, [], collapse)
  end
  
  defp uncase(module_atom) do
    module_atom
    |> to_string()
    |> String.split(".")
    |> Enum.at(-1)
    |> Macro.underscore()
  end

  def gen_stage_entity(stage_atom, rap_prefix) do
    stage_norm = uncase(stage_atom)
    stage_iri  = RDF.IRI.new(rap_prefix <> "stage_" <> stage_norm)
    RDF.Description.new(stage_iri)
    |> RDFS.label("#{inspect stage_norm} stage description")
  end

  # signal stage_invoked_at, work_started_at, work_ended_at, work_input, work_output
  def gen_stage_processing_activity(
    stage_atom,
    base_prefix, rap_prefix, tz,
    %{} = work,
    prev_output_extra \\ [],
    output_extra \\ []
  ) do
    stage_norm     = uncase(stage_atom)
    stage_iri      = RDF.IRI.new(rap_prefix <> "stage_" <> stage_norm)
    stage_proc_iri = RDF.IRI.new(base_prefix <> "stage_" <> stage_norm <> "_processing")

    start_ts = work.work_started_at
    #|> DateTime.from_unix!() |> DateTime.shift_zone!(tz)
    end_ts = work.work_ended_at
    #|> DateTime.from_unix!() |> DateTime.shift_zone!(tz)

    work_input_all  = work.work_input  ++ prev_output_extra
    work_output_all = work.work_output ++ output_extra

    RDF.Description.new(stage_proc_iri)
    |> RDFS.label("#{inspect stage_norm} stage event-processing activity")
    |> PROV.startedAtTime(     start_ts        )
    |> PROV.endedAtTime(       end_ts          )
    |> PROV.wasAssociatedWith( stage_iri       )
    |> PROV.used(              work_input_all  )
    |> PROV.generated(         work_output_all )
  end

  def gen_stage_output_entity(stage_atom, base_prefix, prev_output_iris) do
    stage_norm = uncase(stage_atom)
    stage_output_iri = RDF.IRI.new(base_prefix <> "stage_" <> stage_norm <> "_output")
    
    RDF.Description.new(stage_output_iri)
    |> RDFS.label("#{inspect stage_norm} intermediate output entity")
    |> PROV.wasDerivedFrom(prev_output_iris)
  end

  def gen_stage_invocation_activity(
    stage_atom,
    base_prefix, rap_prefix, tz,
    %{} = work
  ) do
    invocation_ts = work.stage_invoked_at
    #|> DateTime.from_unix!()
    #|> DateTime.shift_zone!(tz)

    stage_norm    = uncase(stage_atom)
    stage_iri     = RDF.IRI.new(rap_prefix  <> "stage_" <> stage_norm)
    stage_inv_iri = RDF.IRI.new(base_prefix <> "stage_" <> stage_norm <> "_invocation")
    
    RDF.Description.new(stage_inv_iri)
    |> RDFS.label("#{inspect stage_norm} stage invocation activity")
    |> SAVED.beam_application(       "RAP"                      )
    |> SAVED.beam_node(              to_string(node())          )
    |> SAVED.beam_module(            to_string(stage_atom)      )
    |> SAVED.otp_version(            System.otp_release()       )
    |> SAVED.elixir_version(         System.version()           )
    |> SAVED.gen_stage_type(         to_string(work.stage_type) )
    |> SAVED.gen_stage_subscrptions( to_string(work.stage_subscriptions))
    |> SAVED.gen_stage_dispatcher(   to_string(work.stage_dispatcher))
    |> PROV.startedAtTime(           invocation_ts              )
    |> PROV.wasAssociatedWith(       stage_iri                  )
  end

end
