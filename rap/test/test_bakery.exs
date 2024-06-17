defmodule RAP.Test.Bakery.Prepare do

  use ExUnit.Case, async: true
  doctest RAP.Bakery.Prepare

  alias RAP.Job.{Runner, Result}
  alias RAP.Bakery.Prepare

  @doc """
  The tests assume that the mnesia data-base is empty, since we want to test the data in isolation. These are unit tests. Of course, there is an assumption that the data-base exists, but that's reasonable.

  We have two directories in `test/manual_test/', one is the basic density job and the other is derived from sentinel cages.

  1. Randomly-generate three UUIDs (one will be a 'good' test case with all data present, the other two will have some/none of the data present);
  2. Copy the files from one of the directories in test/manual_test to test/data_cache, under one of the UUIDs, which is what we'd expect if we'd been running jobs over them;
  3. Run the bake_data/4 function on each one
  4. a) Look the error state, b) whether the files actually get moved, c) whether the files show up in the cache
  5. Flush the cache

  %Runner{ uuid:               spec.uuid,
	     data_source:        spec.data_source,
	     local_version:      spec.local_version,
	     name:               spec.name,
	     title:              spec.title,
	     description:        spec.description,
	     manifest_base_ttl:  spec.manifest_base_ttl,
	     manifest_base_yaml: spec.manifest_base_yaml,
	     resource_bases:     spec.resource_bases,
	     staging_tables:     spec.staging_tables,
	     staging_jobs:       spec.staging_jobs,
	     pre_signal:         spec.pre_signal,
	     producer_signal:    spec.signal,
	     signal:             overall_signal,
	     results:            result_contents    }

   %Result{
      name:        spec.name,
      title:       spec.title,
      description: spec.description,
      source_job:  spec.name,
      type:        "ignore",
      signal:      :ok,
      contents:    "Dummy/ignored job"
    }
  """
  test "Test bakery cache/moving" do

    # See (1)
    uuid0 = UUID.uuid4()
    curr_ts = DateTime.utc_now() |> DateTime.to_unix()
    
    # Pretend we've already checked these into ETS
    :ets.new(:test, [:set, :public, :named_table])
    :ets.insert(:test, {uuid0, curr_ts})
    
    source0  = "test/manual_test/9a55d938-7f50-45b5-8960-08c78d73facc"
    cache0   = "test/data_cache/#{uuid0}"
    dest0    = "test/bakery/#{uuid0}"
    res0     = "{ \"result\": \"test\" }"

    target_resources = [ "Sentinel_cage_station_info_6.csv",
			 "sentinel_cages_site.ttl",
			 "sentinel_cages_site.yaml",
			 "sentinel_cages_cleaned.csv",
			 "sentinel_cages_sampling.ttl",
			 "sentinel_cages_sampling.yaml"    ]
    
    test0 = %Runner{
      uuid:               uuid0,
      signal:             :working,
      manifest_base_ttl:  "manifest.ttl",
      manifest_base_yaml: "manifest.yaml",
      resource_bases:     target_resources,
      results: [
	%Result{
	  name:        "test_result0",
	  title:       "Test result 0 (ignored)",
	  source_job:  "test0",
	  type:        "ignore",
	  signal:      :ok,
	  contents:    "Dummy/ignored job"
	},
	%Result{
	  name:          "test_result1",
	  title:         "Test result 1 (density)",
	  source_job:    "test1",
	  type:          "density",
	  signal:        :working,
	  contents:      res0,
	  result_format: "json",
	  result_stem:   "density"
	}
      ]
    }

    # See (2)
    File.cp_r(source0, cache0)

    # See (3)
    #def bake_data(%Runner{} = processed, cache_dir, bakery_dir, _linked_stem) when processed.signal in [:working, :job_errors] do
    Prepare.bake_data(test0, "test/data_cache", "test/bakery", "post", :test)

    # These should all have been copied over
    assert Enum.all?(target_resources, &File.exists?("#{dest0}/#{&1}"))
    # This should not have been written (note catch-all extension is .txt)
    assert not File.exists?("#{dest0}/ignore_test_result0.txt")
    # This should exist as it's a valid job type which is run by Job.Runner
    assert File.exists?("#{dest0}/density_test_result1.json")
    # We could test that the file got removed altogether, but this isn't necessarily desirable. Better to clean up periodically, I think, since keeping a cache  enables us to avoid downloads from GCP unneccesarily.
    #assert not File.exists?("#{cache0}")
    
    # Clean up
    :ets.delete(:test, uuid0)

  end

end
