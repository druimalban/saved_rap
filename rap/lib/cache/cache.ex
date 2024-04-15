defmodule RAP.Job.Cache do

  require Logger

  def start_link(%RAP.Job.Spec{title: title, result: res}) do
    Task.start_link fn ->
      RAP.Cache.Spec.dummy res
    end
  end

end
