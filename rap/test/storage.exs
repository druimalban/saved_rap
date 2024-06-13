defmodule RAP.Test.Storage.PreRun do
  use Amnesia
  use ExUnit.Case, async: true
  doctest RAP.Storage.PreRun

  alias RAP.Storage.PreRun
  
  defdatabase DB do
    deftable Manifest, [
      :uuid, :data_source,
      :name, :title, :description,
      :start_time, :end_time,
      :manifest_pre_base_ttl,
      :manifest_pre_base_yaml,
      :resource_bases,
      :pre_signal,
      :producer_signal,
      :runner_signal,
      :result_bases,
      :results,
      :staged_tables,
      :staged_jobs
    ]
  end

  test "Put/read UUID into ETS (`Storage.PreRun.ets_feasible?/2')" do
    :ets.new(:test, [:set, :public, :named_table])
    
    fake_uuids = [ "c7c69380-0dfe-4361-a09b-002f40234662",
		   "33f7c11e-fc03-4ee6-8038-2a51fad0f68b",
		   "cb102d33-0aad-4228-becc-70d19942aeeb" ]
    extant_uuid     = "33f7c11e-fc03-4ee6-8038-2a51fad0f68b" # above
    non_extant_uuid = "9c30b60e-21fb-46d2-a07d-5755e9dd7993" # not above
    
    curr_ts    = DateTime.utc_now() |> DateTime.to_unix()
    fake_pairs = fake_uuids |> Enum.zip([curr_ts, curr_ts-300, curr_ts-600])
    fake_pairs |> Enum.each(fn k -> :ets.insert(:test, k) end)

    # Already extant/cached in temporary ETS, i.e. not feasible
    assert not PreRun.ets_feasible?(extant_uuid, :test)
    # Not already extant/cached in temporary ETS, i.e. feasible
    assert PreRun.ets_feasible?(non_extant_uuid, :test)
  end

  test "Check MD5 caching plumbing (`Storage.PreRun.dl_success/3')" do

    test0_text = File.read!("/etc/profile")
    test1_text = File.read!("/etc/profile")
    test2_text = File.read!("/etc/ttys")

    test0_sum = :crypto.hash(:md5, test0_text) |> Base.encode64()
    test1_sum = :crypto.hash(:md5, test1_text) |> Base.encode64()
    test2_sum = :crypto.hash(:md5, test2_text) |> Base.encode64()

    # Default opts `:input_md5 => false'
    # Compare text content against corresponding check-sum
    assert PreRun.dl_success?(test0_sum, test0_text)
    # Compare text content against bad check-sum
    assert not PreRun.dl_success?(test2_sum, test1_text)
    # Compare identical two text contents
    assert PreRun.dl_success?(test1_text, test0_text, opts: [input_md5: false])
    # Compare differing text contents
    assert not PreRun.dl_success?(test2_text, test1_text, opts: [input_md5: false])
  end
  
end

defmodule RAP.Test.Storage.PostRun do

  use Amnesia
  use ExUnit.Case, async: true
  doctest RAP.Storage.PostRun

  alias RAP.Storage.PostRun
  
end

defmodule RAP.Test.Storage.Monitor do

  use Amnesia
  use ExUnit.Case, async: true
  doctest RAP.Storage.Monitor

  alias GoogleApi.Storage.V1.Model.Object, as: GCPObj
  
  alias RAP.Storage.Monitor
  
  test "Check UUID helper regex inputs" do

    owner_good0 = "scratch"
    date_good0  = "19700101"
    uuid_good0  = "33f7c11e-fc03-4ee6-8038-2a51fad0f68b"
    path_good0  = "profile.txt"

    owner_bad0 = "/some/where/"
    owner_bad1 = "file://etc/"
    date_bad0  = "1970MAR01"
    date_bad1  = "1970Apr1st"
    uuid_bad0  = "33f7c11e:fc03:4ee6:8038:2a51fad0f68b"
    uuid_bad1  = "33f7c11e_fc03_4ee6_8038_2a51fad0f68b"
    path_bad0  = "/some/file"
    path_bad1  = "\/#fun#\/"

    obj_good0 = %GCPObj{ name: "#{owner_good0}/#{date_good0}/#{uuid_good0}/#{path_good0}" }
    obj_bad0 = %GCPObj{ name: "#{owner_bad0}/#{date_bad0}/#{uuid_bad0}/#{path_bad0}" }
    obj_bad1 = %GCPObj{ name: "#{owner_bad1}/#{date_bad1}/#{uuid_bad1}/#{path_bad1}" }
    obj_bad2 = %GCPObj{}
    
    assert not is_nil(Monitor.uuid_helper(obj_good0))
    assert is_nil(Monitor.uuid_helper(obj_bad0))
    assert is_nil(Monitor.uuid_helper(obj_bad1))
    assert is_nil(Monitor.uuid_helper(obj_bad2))
  end
  
end

defmodule RAP.Test.Storage.GCP do

  use Amnesia
  use ExUnit.Case, async: true
  doctest RAP.Storage.GCP
  alias RAP.Storage.GCP

  test "Test various index files" do
    # coalesce_job(cache_dir, index_base, %PreRun{} = job)
  end
  
end
