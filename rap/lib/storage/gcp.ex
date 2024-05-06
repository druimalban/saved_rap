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

  alias RAP.Application
  alias RAP.Storage.{GCP, Monitor, Staging}

  defstruct [ :uuid, :manifest, :resources ]

  def start_link initial_state do
    Logger.info "Called Storage.GCP.start_link (_)"
    GenStage.start_link __MODULE__, initial_state, name: __MODULE__
  end

  def init initial_state do
    Logger.info "Called Storage.GCP.init (initial_state = #{inspect initial_state})"
    subscription = [
      { Monitor, min_demand: 0, max_demand: 1 }
    ]
    { :producer_consumer, initial_state, subscribe_to: subscription }
  end
  
  def handle_events(events, _from, %Application{} = state) do
    Logger.info "Called `Storage.GCP.handle_events (events = #{inspect events}, …)'"
    processed = Enum.map(events, &coalesce_job(state.cache_directory, state.index_file, &1))
    { :noreply, processed, state }
  end
  
  @doc """
  Simple helper function to compare MD5 checksums given by the storage
  objects API to the actual file downloaded.

  Erlang's `:crypto' works on a binary, not a file path, which is very
  convenient because it avoids writing to disk duff file responses.

  Further note that in `fetch_job_deps/3', sets `:decode' to false, as
  there may be a risk that the decoding breaks this workflow.
  """
  defp dl_success?(purported_md5, body) do
    actual_md5 = :crypto.hash(:md5, body) |> Base.encode64()
    purported_md5 == actual_md5
  end

  defp wrap_gcp_fetch(session, obj) do
    GCPReqObjs.storage_objects_get(session, obj.gcp_bucket, obj.gcp_name, [alt: "media"], decode: false)
  end

  @doc """
  Given an MD5 checksum by the API listing (in the %Monitor{} struct):
  1. Check that the file exists.
  2. If so, check checksum.
  3. If not, download and check checksum.
  (Make sure this isn't recursive.)
  
  Events are a `%Staging{}' struct: UUID, index object, and all other
  objects. Objects are a `%Monitor{}' struct, which includes all the
  information needed to fetch.
  
  GCP OAuth tokens seem to have an expiry of one hour. Sessions are
  retrieved from the monitor process (which handles renewing these) with
  the GenStage call `:yield_session'. There is relatively low risk of
  race conditions here, because the stage runs immediately after the
  monitor process hands over new events, while catching expired sessions.

  The data cache directory effectively mirrors the GCP object store's
  purported structure but we have the benefit of the mnesia database to
  cache results.
  """
  defp fetch_object(target_dir, obj) do
    target_file = "#{target_dir}/#{obj.path}"
    Logger.info "Polling GCP storage bucket for flat object #{obj.gcp_name}"
    with false <- File.exists?(target_file) && dl_success?(obj.gcp_md5, File.read!(target_file)),
         session <- GenStage.call(RAP.Storage.Monitor, :yield_session),
	 {:ok, %Tesla.Env{body: body, status: 200}} <- wrap_gcp_fetch(session, obj),
	 :ok <- File.write(target_file, body) do
      Logger.info "Successfully wrote #{target_file}"
      {:ok, target_file}
    else
      true ->
	Logger.info "File #{target_file} already exists and MD5 checksum matches API's"
        {:ok, target_file}
      {:error, error = %Tesla.Env{status: code, url: uri, body: msg}} ->
	Logger.info "Query of GCP failed with code #{code} and error message #{msg}"
        {:error, uri, code, msg}
      {:error, reason} ->
	Logger.info "Error writing file due to #{inspect reason}"
        {:error, reason}
    end
  end

  
  defp reject_error({:ok, fp}), do: true
  defp reject_error({:error, _uri, _code, _msg}), do: false
  defp reject_error({:error, "incorrect", _md5}), do: false
  defp reject_error({:error, _reason}), do: false
      
  defp fetch_job_deps(cache_directory, %RAP.Storage.Staging{} = job) do
    Logger.info "Called `Storage.GCP.fetch_job_deps' for job with UUID #{job.uuid}"
    all_resources = [job.index | job.resources]
    target_dir    = "#{cache_directory}/#{job.uuid}"
    
    with :ok     <- File.mkdir_p(target_dir),
	 signals <- Enum.map(all_resources, &fetch_object(target_dir, &1)) do

      errors = signals |> Enum.reject(&reject_error/1)
      if (length errors) > 0 do
	Logger.info "Job #{job.uuid}: Found errors #{inspect errors}"
	{:error, job.uuid, errors}	
      else
	Logger.info "Job #{job.uuid}: No errors to report"
	file_paths = signals |> Enum.map(&elem(&1, 1))
	{:ok, job.uuid, file_paths}
      end
    end
  end

  defp coalesce_job(cache_directory, index, %RAP.Storage.Staging{} = job) do
    target_dir = "#{cache_directory}/#{job.uuid}"
    index_path = "#{target_dir}/#{index}"
    with {:ok, uuid, file_paths} <- fetch_job_deps(cache_directory, job),
         {:ok, manifest}         <- File.read(index_path) do
      Logger.info "Index file is #{inspect index_path}"

      manifest_path = target_dir <> "/" <> String.trim(manifest)
      Logger.info "Manifest file is #{inspect manifest_path}"
      
      non_manifests = file_paths
      |> List.delete(manifest_path)
      |> List.delete(index_path)      
      Logger.info "Non-manifest files are #{inspect non_manifests}"      

      %RAP.Storage.GCP{uuid: uuid, manifest: manifest_path, resources: non_manifests}
    else
      {:error, uuid, errors} -> {:error, job.uuid, errors}
      {:error, reason}       ->
	Logger.info "Could not read index file #{index_path}"
	{:error, reason}
    end
  end

end
