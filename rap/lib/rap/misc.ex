defmodule RAP.Miscellaneous do

  alias RAP.Storage.{PreRun, Monitor}
  
  def pretty_print_object(%Monitor{path: fp}), do: fp
  def pretty_print_object(%PreRun{uuid: uuid, resources: res}) do
    pretty_resources = res |> Enum.map(&pretty_print_object/1)
    "%{UUID: #{uuid}, resources: #{inspect pretty_resources}}"
  end
  def pretty_print_object(n), do: inspect n
  
  def format_time(nil, _tz), do: nil
  def format_time(time_stamp, time_zone) do    
    weekdays = [ "Monday",  "Tuesday",  "Wednesday", "Thursday",
		 "Friday",  "Saturday", "Sunday"   ]
    months =   [ "January", "February", "March",
		 "April",   "May",      "June",
		 "July",    "August",   "September",
		 "October", "November", "December" ]

    {:ok, date_time} = DateTime.shift_zone(time_stamp, time_zone)
    
    # These range from 1-7, 1-12 but lists are zero-indexed
    day_name      = weekdays |> Enum.fetch!(Date.day_of_week(date_time) - 1)
    month_name    = months   |> Enum.fetch!(date_time.month - 1)
    
    padded_hour   = date_time.hour   |> to_string |> String.pad_leading(2, "0")
    padded_minute = date_time.minute |> to_string |> String.pad_leading(2, "0") 
    
    "#{day_name}, #{date_time.day} #{month_name} #{date_time.year}, #{padded_hour}:#{padded_minute} (GMT)"
  end

  
  
end
