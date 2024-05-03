defmodule RAP.Storage.GCP do
  @moduledoc """
  This is the first stage of the pipeline, but the first stage could
  easily be monitoring local storage or some other cloud storage thing.

  The files are uploaded as if there's a directory structure, but there
  isn't really because the GCP storage buckets are a flat object-based
  structure. Thus associating and gathering files into jobs is somewhat
  non-trivial and more involved than it should be.
  """
  use GenStage
  require Logger

  alias GoogleApi.Storage.V1.Connection,    as: GCPConn
  alias GoogleApi.Storage.V1.Model.Object,  as: GCPObj
  alias GoogleApi.Storage.V1.Model.Objects, as: GCPObjs
  alias GoogleApi.Storage.V1.Api.Objects,   as: GCPReqObjs

  alias RAP.Storage.Monitor
  alias RAP.Storage.Transactions

  @doc """
  Simple helper function to compare MD5 checksums given by the storage
  objects API to the actual file downloaded.

  Erlang's `:crypto' works on a binary, not a file path, which is very
  convenient because it avoids writing to disk duff file responses.

  Further note that in `fetch_job_deps/3', sets `:decode' to false, as
  there may be a risk that the decoding breaks this workflow.
  """
  defp dl_success?(obj, body) do
    purported = obj.md5
    actual    = :crypto.hash(:md5, body) |> Base.encode64()
    if purported == actual do
      { :ok, "correct", purported }
    else
      { :error, "incorrect", purported }
    end  
  end
  
  @doc """
  Actually fetch the objects in question

  The data cache directory effectively mirrors the GCP object store's
  purported structure but we have the benefit of the mnesia database to
  cache results.
  """
  def fetch_job_deps(session, bucket_name, obj) do
    Logger.info "Called `Storage.GCP.fetch_uuid_deps' for job with UUID #{obj.uuid}"
    target_dir  = "#{@cache_dir}/#{obj.owner}/#{obj.uuid}"
    target_file = "#{target_dir}/#{obj.file}"
    with {:ok, %Tesla.Env{body: body, status: 200}} <- GCPReqObjs.storage_objects_get(session, bucket_name, obj.name, [alt: "media"], decode: false),
	 {:ok, "correct", _md5} <- Monitor.dl_success?(obj, body),
         false <- File.exists?(target_file),
         :ok   <- File.mkdir_p(target_dir),
         :ok   <- File.write(target_file, body)
      do
        Logger.info "`Storage.GCP.fetch_job_deps/3': Successfully wrote #{target_file}"
        {:ok, target_file}
      else
	{:error, error = %Tesla.Env{status: 401}} ->
	  Logger.info "Query of GCP bucket #{bucket_name} appeared to time out, seek new session"
	  # If this bit fails, we know there's really something up!
	  {:ok, new_session} = Monitor.new_connection()
	  fetch_job_deps(new_session, bucket_name, obj)
	{:error, error = %Tesla.Env{status: code, url: uri, body: msg}} ->
	  Logger.info "Query of GCP failed with code #{code} and error message #{msg}"
	  {:error, uri, code, msg}
        {:error, "incorrect", md5} ->
	  Logger.info "Downloaded file does not match purported checksum #{md5}!"
	  {:error, md5}
	true ->
	  Logger.info "Target directory already exists, but job #{obj.uuid} is neither running nor has been completed"
	  File.rm_rf(target_dir)
	  fetch_job_deps(session, bucket_name, obj)
	{:error, raison } ->
	  Logger.info "Error creating file/directory due to #{raison}"
	  {:error, target_dir}
    end
  end

  @doc """
  There are two elements of this. Firstly, we have a notion of UUIDs
  which have been cached. There is a single row per UUID because the UUID
  has a notion of a job, or collection of jobs. Secondly, we have a
  notion of files, and it is expected that there would be many different
  files associated with an UUID.

  The most efficient way to check which jobs are feasible is to summarise
  the list of objects into unique UUIDs. This is just a map to extract
  the UUIDs then run `Enum.uniq/2'. We then filter these unique UUIDs
  using `Transactions.feasible/1' and `ets_feasible/1', which produce a
  set of jobs which are neither cached (finished) nor currently running
  (in Erlang term storage ~ `:ets').

  We further group all objects provided by UUID. For each UUID which made
  it out of the filtering, use this as the key to lookup the collection
  of files associated with the UUID.

  There are certain well-founded assumptions which are governed by the
  functionality of `fisdat' and/or `fisup'. Firstly, there is at most one
  manifest per UUID, because that's the single file we fed into `fisup'
  in order to upload the files (and a unique UUID is created per
  invocation of `fisup'). Secondly, the `fisup' program will have
  normalised the name of the manifest to `manifest.ttl'. Indeed, this is
  the only normalisation that need take place, because as it currently
  stands, the manifest refers to all of the dependent files.
  """ #### MOVE ME MOVE ME MOVE ME
  
  def handle_cast({:storage_changed, str_invocation}, state) do
    Logger.info "Received fake :storage_changed signal for #{str_invocation}"
    { :noreply, [str_invocation], state }
  end

end
