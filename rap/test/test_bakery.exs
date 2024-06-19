defmodule RAP.Test.Bakery.Prepare do

  use ExUnit.Case, async: true
  doctest RAP.Bakery.Prepare

  alias RAP.Job.{ScopeSpec, ResourceSpec, TableSpec, JobSpec, ManifestSpec}
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
	  output_format: "json",
	  output_stem:   "density"
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




  
  test "Test web page generation" do
    # Note ,this is fairly manual, as far as tests go, since we're primarily looking to see if the HTML document generation is successful. 

    #    defstruct [ :uuid, :data_source,
    #              :name, :title, :description,
    #              :start_time, :end_time,
    #              :manifest_pre_base_ttl,
    #              :manifest_pre_base_yaml,
    #              :resource_bases,
    #              :pre_signal,
    #              :producer_signal,
    #              :runner_signal,
    #              :result_bases,
    #              :results,
    #              :staged_tables,
    #              :staged_jobs          ]

    uuid0    = UUID.uuid4()
    source0  = "test/manual_test/7a0c9260-19b8-11ef-bd35-86d813ecdcdd"
    cache0   = "test/data_cache/#{uuid0}"
    dest0    = "test/bakery/#{uuid0}"
    res0     = "{ \"result\": \"test\" }"

    target_resources0 = [ "density.yaml", "density.ttl",
		          "sentinel_cages_sampling.yaml", "sentinel_cages_sampling.ttl",
			  "density.csv", "cagedata-10.csv" ]
    
    curr_ts = DateTime.utc_now() |> DateTime.to_unix()
    fake_ts = curr_ts - 300

    # Need to process extant: true/false sensibly/consistently
    staging_tables0 = [
      %TableSpec{
	name:     "time_density_simple",
	title:    "Placeholder time/density description",
	resource: %ResourceSpec{ base: "density.csv", extant: true },
	schema:   %ResourceSpec{ base: "density.ttl", extant: true } 
      },
      %TableSpec{
	name:     "sampling",
	title:    "Sentinel cages sampling: known-good test table",
	resource: %ResourceSpec{ base: "cagedata-10.csv",             extant: true },
	schema:   %ResourceSpec{ base: "sentinel_cages_sampling.ttl", extant: true }
      }
    ]

    # … %JobSpec{} …
    # defstruct [ :name, :title, :description,
    # :type, :result_format, :result_stem,
    # :scope_descriptive,   :scope_collected,  :scope_modelled,
    # :errors_descriptive,  :errors_collected, :errors_modelled ]
    #
    # … %ScopeSpec{} …
    #defstruct [ :variable_uri,  :variable_curie, :column,
    #	      :resource_name, :resource_base ]
    staging_jobs0 = [
      %JobSpec{
	name:          "job_example_time_density_simple",
	title:         "Example job time_density_simple",
	result_format: "json",
	result_stem:   "result_density",
	type:          "density",
	scope_collected: [
	  %ScopeSpec{
	    column:         "TOTAL",
	    resource_name:  "sampling",
	    resource_base:  "cagedata-10.csv",
	    variable_curie: "saved:lice_af_total",
	    variable_uri:   "https://marine.gov.scot/metadata/saved/schema/lice_af_total"
	  }
	],
	scope_modelled: [
	  %ScopeSpec{
	    column:         "time",
	    resource_name:  "time_density_simple",
	    resource_base:  "density.csv",
	    variable_curie: "saved:time",
	    variable_uri:   "https://marine.gov.scot/metadata/saved/schema/time"
	  },
	  %ScopeSpec{
	    column:         "density",
	    resource_name:  "time_density_simple",
	    resource_base:  "density.csv",
	    variable_curie: "saved:density",
	    variable_uri:   "https://marine.gov.scot/metadata/saved/schema/density"
	  }
	]
      }
    ]
    
    target_results0 = [
      %Result{
	name:        "result_job_example_ignore",
	title:       "Test result 0 (ignored)",
	source_job:  "job_example_ignore",
	type:        "ignore",
	signal:      :ok,
	contents:    "Dummy/ignored job"
      },
      %Result{
	name:          "result_job_example_time_density_simple",
	title:         "Test result 1 (density)",
	source_job:    "job_example_time_density_simple",
	type:          "density",
	signal:        :working,
	contents:      res0,
	output_format: "json",
	output_stem:   "density"
      }
    ]
      
    # compose_document(html_directory, rap_uri, style_sheet, time_zone, prep) -> %{uuid:_, contents:_}
    desc_full0 = %Prepare{
      uuid:                   uuid0,
      data_source:            :gcp,
      name:                   "LeafManifest",
      title:                  "Fully-filled out manifest for testing",
      description:            "Longer-form description",
      start_time:             fake_ts,
      end_time:               curr_ts,
      manifest_pre_base_ttl:  "prepared_manifest1_pre.ttl", ## Not renamed as we're copying from source dir
      manifest_pre_base_yaml: "prepared_manifest1_pre.yaml",
      resource_bases:         target_resources0,
      pre_signal:             :working,
      producer_signal:        :working,
      runner_signal:          :working,
      result_bases:           "results_job_example_time_density_simple.json",
      results:                target_results0,
      staged_tables:          staging_tables0,
      staged_jobs:            staging_jobs0
    }
    
    
  end

end
