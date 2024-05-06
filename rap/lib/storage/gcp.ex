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

  alias RAP.Storage.{GCP, Monitor, Staging}

  defstruct [ :uuid, :manifest, :resources ]

  def start_link _args do
    Logger.info "Called Storage.GCP.start_link (_)"
    initial_state = []
    GenStage.start_link __MODULE__, initial_state, name: __MODULE__
  end

  def init initial_state do
    Logger.info "Called Storage.GCP.init (initial_state = #{inspect initial_state})"
    subscription = [
      { Monitor, min_demand: 0, max_demand: 1 }
    ]
    { :producer_consumer, initial_state, subscribe_to: subscription }
  end
  
  def handle_events events, _from, state do
    Logger.info "Called Storage.GCP.handle_events (events = #{inspect events}, _, state = #{inspect state})"
    processed = Enum.map(events, &coalesce_job/1)
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
  defp dl_success?(obj, body) do
    purported = obj.gcp_md5
    actual    = :crypto.hash(:md5, body) |> Base.encode64()
    if purported == actual do
      { :ok, "correct", purported }
    else
      { :error, "incorrect", purported }
    end  
  end

  @doc """
  Events are a `%Staging{}' struct: UUID, index object, and all other
  objects. Objects are a `%Monitor{}' struct, which includes all the
  information needed to fetch.
  
  GCP OAuth tokens seem to have an expiry of one hour. Sessions are
  retrieved from the monitor process (which handles renewing these) with
  the GenStage call `:yield_session'. There is relatively low risk of
  race conditions here, because the stage runs immediately after the
  monitor process hands over new events, while catching expired sessions.
  """
  defp wrap_gcp_fetch(session, %RAP.Storage.Monitor{} = obj) do
    Logger.info "Polling GCP storage bucket for flat object #{obj.gcp_name}"  
    GCPReqObjs.storage_objects_get(session, obj.gcp_bucket, obj.gcp_name, [alt: "media"], decode: false)
  end

  @doc """
  Actually fetch the objects in question

  The data cache directory effectively mirrors the GCP object store's
  purported structure but we have the benefit of the mnesia database to
  cache results.
  """
  def fetch_object(target_dir, %RAP.Storage.Monitor{} = obj) do
    target_file = "#{target_dir}/#{obj.path}"
    with session <- GenStage.call(RAP.Storage.Monitor, :yield_session),
         {:ok, %Tesla.Env{body: body, status: 200}} <- wrap_gcp_fetch(session, obj),
         false <- File.exists?(target_file),
         {:ok, "correct", _md5} <- dl_success?(obj, body),
         :ok <- File.write(target_file, body) do
      Logger.info "`Storage.GCP.fetch_job_deps/3': Successfully wrote #{target_file}"
      {:ok, target_file}
    else
      {:error, error = %Tesla.Env{status: code, url: uri, body: msg}} ->
	Logger.info "Query of GCP failed with code #{code} and error message #{msg}"
        {:error, uri, code, msg}
      {:error, "incorrect", md5} ->
	Logger.info "Downloaded file does not match purported checksum #{md5}. Not writing to disk."
	{:error, md5}
      {:error, reason} ->
	Logger.info "Error writing file due to #{inspect reason}"
        {:error, reason}
    end
  end
  defp reject_error({:ok, fp}), do: true
  defp reject_error({:error, _uri, _code, _msg}), do: false
  defp reject_error({:error, "incorrect", _md5}), do: false
  defp reject_error({:error, _reason}), do: false
      
  def fetch_job_deps(%RAP.Storage.Staging{} = job) do
    Logger.info "Called `Storage.GCP.fetch_job_deps' for job with UUID #{job.uuid}"
    all_resources = [job.index | job.resources]
    target_dir  = "./data_cache/#{job.uuid}"
    with :ok          <- File.mkdir_p(target_dir),
	 signals      <- Enum.map(all_resources, &fetch_object(target_dir, &1)) do
      errors = signals |> Enum.reject(&reject_error/1)
      if (length errors) > 0 do
	Logger.info "Job #{job.uuid}: Found errors #{inspect errors}"
	{:error, job.uuid, errors}	
      else
	Logger.info "Job #{job.uuid}: No errors to report"
	file_paths = signals |> Enum.map(&elem(&1, 0))
	{:ok, job.uuid, file_paths}
      end
    end
  end

  def coalesce_job(%RAP.Storage.Staging{} = job) do
    target_dir = "./data_cache/#{job.uuid}"
    index_path = "#{target_dir}/.index"
    with {:ok, uuid, file_paths} <- fetch_job_deps(job),
         {:ok, manifest}         <- File.read(index_path) do
      manifest_path = "#{target_dir}/#{manifest}"
      Logger.info "Job #{uuid}: manifest file is #{manifest_path}"
      
      non_manifests = file_paths |> List.delete(index_path)
      Logger.info "Job #{uuid}: remaining files are #{inspect non_manifests}"      
      
      %RAP.Storage.GCP{uuid: uuid, manifest: manifest_path, resources: non_manifests}
    else
      {:error, uuid, errors} -> {:error, job.uuid, errors}
      {:error, reason}       ->
	Logger.info "Could not read index file #{index_path}"
	{:error, reason}
    end
  end



end
