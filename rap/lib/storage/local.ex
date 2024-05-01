defmodule RAP.Storage.Local do

  @doc """
  Given a directory, check for sub-directories which will have UUIDs
  This is primarily useful if we're monitoring local storage.
  """
  def get_local_uuids(directory) do
    directory
    |> File.ls()
    |> elem(1)
    |> Enum.map(fn(dir) -> {dir, File.stat("/etc" <> "/" <> dir)} end)
    |> Enum.filter(fn ({_, {:ok, %File.Stat{type: type}}}) -> type == :dir end)
    |> Enum.map(&elem &1, 0)
  end

end
