defmodule RAP.Test.Storage.PreRun do

  require Amnesia
  require Amnesia.Helper
  require RAP.Storage.DB.Manifest, as: ManifestTable
  
  use ExUnit.Case, async: false
  doctest RAP.Storage.PreRun

  alias RAP.Storage.PreRun

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

  test "Put/read UUID in Amnesia DB (`Storage.PreRun.mnesia_feasible?/1')" do
    # This test revealed that we have to start :mnesia in test_helper.exs
    # if launching the tests with the --no-start option.

    # 1. Generate a UUID, but don't add it
    uuid0 = UUID.uuid4()
    # 2. Run mnesia_feasible/1 (should return true as it's not cached)
    assert PreRun.mnesia_feasible?(uuid0)
    # 3. Inject into appropriate struct and insert struct into mnesia DB
    struct0 = %ManifestTable{ uuid: uuid0 }
    Amnesia.transaction do
      struct0 |> ManifestTable.write()
    end
    # 4. Run mnesia_feasible/1 (should return false now)
    assert not PreRun.mnesia_feasible?(uuid0)
    # 5. Remove from mnesia DB
    Amnesia.transaction do
      uuid0 |> ManifestTable.delete()
    end
    # 6. Run mnesia_feasible/1 (should return true again)
    assert PreRun.mnesia_feasible?(uuid0)
    # Done!
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

  require Amnesia
  require Amnesia.Helper
  require Logger

  require RAP.Storage.DB.Manifest, as: ManifestTable
  
  use ExUnit.Case, async: false
  doctest RAP.Storage.PostRun

  alias RAP.Storage.{PreRun, PostRun}
  alias RAP.Bakery.Prepare

#  test "Test manifest caching function" do
#
#    uuid0   = UUID.uuid4()
#    struct0 = %Prepare{uuid: uuid0}
#
#    curr_ts    = DateTime.utc_now() |> DateTime.to_unix()
#    :ets.new(:test, [:set, :public, :named_table])
#    :ets.insert(:test, {uuid0, curr_ts})
#
#    assert PreRun.mnesia_feasible?(uuid0)
#    assert PostRun.cache_manifest(struct0, :test)
#
#    Amnesia.transaction do
#      uuid0 |> ManifestTable.delete()
#    end
#    assert PreRun.mnesia_feasible?(uuid0)
#  end

  test "Yield various Manifest objects from database" do
    curr_ts = DateTime.utc_now() |> DateTime.to_unix()
    
    uuid0 = UUID.uuid4()
    uuid1 = UUID.uuid4()
    uuid2 = UUID.uuid4()

    struct0 = %ManifestTable{ uuid: uuid0, start_time: curr_ts - 450 }
    struct1 = %ManifestTable{ uuid: uuid1, start_time: curr_ts - 300 }
    struct2 = %ManifestTable{ uuid: uuid2, start_time: curr_ts - 10 }
    
    Amnesia.transaction do
      struct0 |> ManifestTable.write()
      struct1 |> ManifestTable.write()
      struct2 |> ManifestTable.write()

      val = ManifestTable.read(uuid0)
      Logger.info("VAL: #{inspect val}")
    end

    res_exp_none  = PostRun.yield_manifests(curr_ts, "GB-Eire")
    res_exp_uuid2 = PostRun.yield_manifests(curr_ts - 100, "GB-Eire")
    res_exp_all0  = PostRun.yield_manifests("GB-Eire")
    res_exp_all1  = PostRun.yield_manifests(curr_ts - 3000, "GB-Eire")
    
    Logger.info ("Result #1 (expected none):    #{inspect res_exp_none}")
    Logger.info ("Result #2 (expected UUID #2): #{inspect res_exp_uuid2}")
    Logger.info ("Result #3 (expected all):     #{inspect res_exp_all0}")
    Logger.info ("Result #4 (expected all):     #{inspect res_exp_all1}")
    
    Amnesia.transaction do
      uuid0 |> ManifestTable.delete()
      uuid1 |> ManifestTable.delete()
      uuid2 |> ManifestTable.delete()
    end

  end

  
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
  alias RAP.Storage.{GCP, PreRun, MidRun}
  
  #  test "Test various index files" do
  #    # coalesce_job(cache_dir, index_base, %PreRun{} = job)
  #
  #    test_cache_dir = "test/manual_test"
  #    test_job_spec = %PreRun{ uuid: "9a55d938-7f50-45b5-8960-08c78d73facc" }
  #
  #    test_run0 = GCP.coalesce_job(test_cache_dir, "dot.index.good",  test_job_spec)
  #    test_run1 = GCP.coalesce_job(test_cache_dir, "dot.index.empty", test_job_spec)
  #    test_run2 = GCP.coalesce_job(test_cache_dir, "dot.index.bad0",  test_job_spec)
  #    test_run3 = GCP.coalesce_job(test_cache_dir, "dot.index.bad1",  test_job_spec)
  #    test_run4 = GCP.coalesce_job(test_cache_dir, "dot.index.fake",  test_job_spec)
  #
  #    assert match?(test_run0, %MidRun{signal: :working})
  #    assert match?(test_run1, %MidRun{signal: :empty_index})
  #    assert match?(test_run2, %MidRun{signal: :bad_index})
  #    assert match?(test_run3, %MidRun{signal: :bad_index})
  #    assert match?(test_run4, %MidRun{})
  #    
  #  end
  
end
