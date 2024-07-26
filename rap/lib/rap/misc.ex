defmodule RAP.Miscellaneous do

  alias RAP.Storage.{PreRun, Monitor}
  
  def pretty_print_object(%Monitor{path: fp}), do: fp
  def pretty_print_object(%PreRun{uuid: uuid, resources: res}) do
    pretty_resources = res |> Enum.map(&pretty_print_object/1)
    "%{UUID: #{uuid}, resources: #{inspect pretty_resources}}"
  end
  def pretty_print_object(n), do: inspect n
  
  def format_time(nil, _tz), do: nil
  def format_time(unix_ts, time_zone) do    
    weekdays = [ "Monday",  "Tuesday",  "Wednesday", "Thursday",
		 "Friday",  "Saturday", "Sunday"   ]
    months =   [ "January", "February", "March",
		 "April",   "May",      "June",
		 "July",    "August",   "September",
		 "October", "November", "December" ]
    dt = unix_ts |> DateTime.from_unix!() |> DateTime.shift_zone!(time_zone)
    
    # These range from 1-7, 1-12 but lists are zero-indexed
    day_name      = weekdays  |> Enum.fetch!(Date.day_of_week(dt) - 1)
    month_name    = months    |> Enum.fetch!(dt.month - 1)
    
    padded_hour   = dt.hour   |> to_string |> String.pad_leading(2, "0")
    padded_minute = dt.minute |> to_string |> String.pad_leading(2, "0") 
    
    "#{day_name}, #{dt.day} #{month_name} #{dt.year}, #{padded_hour}:#{padded_minute} (GMT)"
  end

  def gen_stage_entity(stage_iri, stage_module) do
    RDFS.Description.new(stage_iri)
    |> RDFS.label("#{inspect stage_module} stage description")
  end

  def gen_stage_processing_activity(stage_processing_iri, stage_iri, prev_output_iris, stage_module, output_iris, started_at, ended_at, tz) do
    start_ts = started_at |> DateTime.from_unix!() |> DateTime.shift_zone!(tz)
    end_ts   = ended_at   |> DateTime.from_unix!() |> DateTime.shift_zone!(tz)
    
    RDF.Description.new(stage_processing_iri)
    |> RDFS.label("#{inspect stage_module} stage event-processing activity")
    |> PROV.startedAtTime(     start_ts         )
    |> PROV.endedAtTime(       end_ts           )
    |> PROV.wasAssociatedWith( stage_iri        )
    |> PROV.used(              prev_output_iris )
    |> PROV.generated(         output_iris      )
  end

  def gen_stage_output_entity(stage_output_iri, prev_output_iris, stage_module) do
    RDF.Description.new(stage_output_iri)
    |> RDFS.label("#{inspect stage_module} intermediate output entity")
    |> PROV.wasDerivedFrom(prev_output_iris)
  end

  def gen_stage_invocation_activity(stage_invocation_iri, stage_iri, stage_module, invoked_at, tz) do
    invocation_ts = invoked_at
    |> DateTime.from_unix!()
    |> DateTime.shift_zone!(tz)
    
    RDF.Description.new(stage_invocation_iri)
    |> RDFS.label("#{inspect stage_module} stage invocation activity")
    |> SAVED.beam_application( "RAP"                )
    |> SAVED.beam_node(        node()               )
    |> SAVED.beam_module(      inspect stage_module )
    |> SAVED.otp_version(      System.otp_release() )
    |> SAVED.elixir_version(   System.version()     )
    |> PROV.startedAtTime(     invocation_ts        )
    |> PROV.wasAssociatedWith( stage_iri            )
  end

  # Leave this here for now, but have a notion of serialising this as RDF, later
  def gen_result(results_desc, result_iri, last_output_iri, software_agents, download_url) do
    label = # To be fixed later, by making title mandatory
      case results_desc.title do
	nil -> "Result of job #{results_desc.name}"
	nom -> "Result of job #{results_desc.name} (#{results_desc.title})"
      end
    RDF.Description.new(result_iri)
    |> RDFS.label(          label                      )
    |> PAV.createdAt(       download_url               )
    |> PAV.createdWith(     software_agents            )
    |> PROV.wasDerivedFrom( last_output_iri            )
    |> DCAT.mediaType(      results_desc.output_format )
    |> DCAT.format(         results_desc.output_format )
    # |> DCAT.downloadURL(    download_url               )
  end
  
end
