defmodule RAP.Test.Bakery.Prepare do

  use ExUnit.Case, async: false
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
    uuid1 = UUID.uuid4()
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

end

defmodule RAP.Test.Bakery.Compose do

  use ExUnit.Case, async: false
  doctest RAP.Bakery.Compose

  alias RAP.Job.{ScopeSpec, ResourceSpec, TableSpec, JobSpec, ManifestSpec}
  alias RAP.Job.{Runner, Result}
  alias RAP.Bakery.{Prepare, Compose}

  require Logger

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

    uuids = for _ <- 1..20, do: UUID.uuid4()
    
    res0 = "{ \"result\": \"test\" }"

    target_resources = [ "density.yaml", "density.ttl",
		         "sentinel_cages_sampling.yaml", "sentinel_cages_sampling.ttl",
			 "density.csv", "cagedata-10.csv" ]
    
    curr_ts = DateTime.utc_now() |> DateTime.to_unix()
    fake_ts = curr_ts - 300

    # Need to process extant: true/false sensibly/consistently
    staging_tables = [
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

    job_ignore = %JobSpec{
      name:          "job_example_ignore",
      title:         "Example empty/ignored job",
      result_format: "txt",
      result_stem:   "result_ignore",
      type:          "ignore"
    }
    
    job_dens_working = %JobSpec{
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
    job_dens_job_err = %JobSpec{
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
      ]
    }

    


    res_ignore = %Result{
      name:        "result_job_example_ignore",
      title:       "Test result 0 (ignored; working)",
      source_job:  "job_example_ignore",
      type:        "ignore",
      signal:      :ignored,
      contents:    "Dummy/ignored job",
      start_time:  fake_ts+10,
      end_time:    fake_ts+90
    }
    res_dens_working = %Result{
      name:          "result_job_example_time_density_simple",
      title:         "Test result 1 (density; working)",
      source_job:    "job_example_time_density_simple",
      type:          "density",
      signal:        :working,
      contents:      res0,
      output_format: "json",
      output_stem:   "density",
      start_time:    fake_ts+91,
      end_time:      curr_ts-90
    }
    res_dens_job_err = %Result{
      name:          "result_job_example_time_density_simple",
      title:         "Test result 2 (density; job error)",
      source_job:    "job_example_time_density_simple",
      type:          "density",
      signal:        :job_error,
      contents:      nil,
      output_format: "json",
      output_stem:   "density",
      start_time:    fake_ts+91,
      end_time:      curr_ts-90
    }
    res_dens_py_err = %Result{
      name:          "result_job_example_time_density_simple",
      title:         "Test result 3 (density; Python error)",
      source_job:    "job_example_time_density_simple",
      type:          "density",
      signal:        :python_error,
      contents:      nil,
      output_format: "json",
      output_stem:   "density",
      start_time:    fake_ts+91,
      end_time:      curr_ts-90
    }

    staging_jobs_working   = [ job_ignore, job_dens_working ]
    target_results_working = [ res_ignore, res_dens_working ]
    # Last test case for well-formed job, but, somehow, call to Python/external command fails
    staging_jobs_errors    = [ job_ignore, job_dens_working, job_dens_job_err, job_dens_working ]
    target_results_errors  = [ res_ignore, res_dens_working, res_dens_job_err, res_dens_py_err ]
      
    # compose_document(html_directory, rap_uri, style_sheet, time_zone, prep) -> %{uuid:_, contents:_}
    desc_full_working = %Prepare{
      uuid:                   Enum.at(uuids, 0),
      data_source:            :gcp,
      name:                   "LeafManifest0",
      title:                  "Fully filled out manifest for testing",
      description:            "Longer-form description",
      start_time:             fake_ts,
      end_time:               curr_ts,
      manifest_pre_base_ttl:  "prepared_manifest1_pre.ttl", ## Not renamed as we're copying from source dir
      manifest_pre_base_yaml: "prepared_manifest1_pre.yaml",
      resource_bases:         target_resources,
      pre_signal:             :working,
      producer_signal:        :working,
      runner_signal:          :working,
      result_bases:           "results_job_example_time_density_simple.json",
      results:                target_results_working,
      staged_tables:          staging_tables,
      staged_jobs:            staging_jobs_working
    }
    desc_full_job_errors = %Prepare{
      uuid:                   Enum.at(uuids, 1),
      data_source:            :gcp,
      name:                   "LeafManifest1",
      title:                  "Nominally fully filled out manifest for testing",
      description:            "Failure signal :job_errors",
      start_time:             fake_ts,
      end_time:               curr_ts,
      manifest_pre_base_ttl:  "prepared_manifest1_pre.ttl", ## Not renamed as we're copying from source dir
      manifest_pre_base_yaml: "prepared_manifest1_pre.yaml",
      resource_bases:         target_resources,
      pre_signal:             :working,
      producer_signal:        :working,
      runner_signal:          :job_errors,
      result_bases:           "results_job_example_time_density_simple.json",
      results:                target_results_errors,
      staged_tables:          staging_tables,
      staged_jobs:            staging_jobs_errors
    }

    prep_up_to_producer = fn uuid, sig ->
       %Prepare{
	 uuid:                   uuid,
	 data_source:            :gcp,
	 name:                   "LeafManifest2",
	 title:                  "Partially filled out manifest for testing",
	 description:            "Up to producer",
	 manifest_pre_base_ttl:  "prepared_manifest1_pre.ttl", ## Not renamed as we're copying from source dir
	 manifest_pre_base_yaml: "prepared_manifest1_pre.yaml",
	 resource_bases:         target_resources,
	 runner_signal:          :see_producer,
	 producer_signal:        sig,
	 pre_signal:             :working
       }
    end
    
    desc_up_to_producer0 = prep_up_to_producer.(Enum.at(uuids, 2), :empty_manifest)
    desc_up_to_producer1 = prep_up_to_producer.(Enum.at(uuids, 3), :bad_manifest_tables)
    desc_up_to_producer2 = prep_up_to_producer.(Enum.at(uuids, 4), :bad_input_graph)
    desc_up_to_producer3 = prep_up_to_producer.(Enum.at(uuids, 5), :working) # weird state, treat as 'unspecified'?
    desc_up_to_producer4 = prep_up_to_producer.(Enum.at(uuids, 6), nil)
    desc_up_to_producer5 = prep_up_to_producer.(Enum.at(uuids, 7), :some_other_error)
    
    prep_up_to_pre = fn u, sig ->
      %Prepare{
	uuid:            Enum.at(uuids, u),
	data_source:     :gcp,
	name:            "LeafManifest3",
	title:           "Partially filled out manifest for testing",
	description:     "Up to pre-producer",
	runner_signal:   :see_pre,
	producer_signal: :see_pre,
	pre_signal:      sig
      }
    end

    # As if we couldn't read the index file pointing to the manifest
    desc_up_to_pre0 = prep_up_to_pre.(8,  :empty_index)
    desc_up_to_pre1 = prep_up_to_pre.(9,  :bad_index)
    desc_up_to_pre2 = prep_up_to_pre.(10, :working)
    desc_up_to_pre3 = prep_up_to_pre.(11, nil)
    desc_up_to_pre4 = prep_up_to_pre.(12, :some_other_error)
    

    quick_lhs_inject = fn
      u, :see_producer, err ->
	%Compose{
	  uuid:               Enum.at(uuids, u),
	  output_stem:        "index",
	  output_format:      "html",
	  runner_signal:      :see_producer,
	  runner_signal_full: "Reading the manifest file failed: #{err}"
        }
      u, :see_pre, err ->
	%Compose{
	  uuid:               Enum.at(uuids, u),
	  output_stem:        "index",
	  output_format:      "html",
	  runner_signal:      :see_pre,
	  runner_signal_full: "Reading the index file failed: #{err}"
        }
      u, sig, fsig ->
	%Compose{
	  uuid:               Enum.at(uuids, u),
	  output_stem:        "index",
	  output_format:      "html",
	  runner_signal:      sig,
	  runner_signal_full: fsig
        }
    end
    quick_rhs_compose = fn desc ->
      Compose.compose_document(
	"./html_fragments",
	"/saved/rap",
	"/saved/rap/assets/app.css",
	"GB-Eire",
	desc
      )
    end

    # Simple all stages succeeded &c.
    lhs_full_working    = quick_lhs_inject.(0, :working,    "All stages succeeded.")
    lhs_full_job_errors = quick_lhs_inject.(1, :job_errors, "Some jobs have failed. See below.")

    #  "Reading the manifest failed: <err>"
    
    lhs_up_to_producer0 = quick_lhs_inject.(2, :see_producer, "Name/IRI of manifest was malformed")
    lhs_up_to_producer1 = quick_lhs_inject.(3, :see_producer, "RDF graph was valid, but referenced tables were malformed")
    lhs_up_to_producer2 = quick_lhs_inject.(4, :see_producer, "RDF graph was malformed and could not be load at all")
    lhs_up_to_producer3 = quick_lhs_inject.(5, :see_producer, "Passing loaded manifest to job runner stage failed")
    lhs_up_to_producer4 = quick_lhs_inject.(6, :see_producer, "Other error loading manifest: unspecified signal")
    lhs_up_to_producer5 = quick_lhs_inject.(7, :see_producer, "Other error loading manifest: some_other_error")

    # "Reading the index file failed: <err>"
    lhs_up_to_pre0      = quick_lhs_inject.(8, :see_pre, "Index file was empty")
    lhs_up_to_pre1      = quick_lhs_inject.(9, :see_pre, "Index file was malformed")
    lhs_up_to_pre2      = quick_lhs_inject.(10, :see_pre, "Passing loaded index to job producer stage failed")
    lhs_up_to_pre3      = quick_lhs_inject.(11, :see_pre, "Other error loading index: unspecified signal")
    lhs_up_to_pre4      = quick_lhs_inject.(12, :see_pre, "Other error loading index: some_other_error")

    lhs_to_test = [ lhs_full_working, lhs_full_job_errors,
		    lhs_up_to_producer0, lhs_up_to_producer1,
		    lhs_up_to_producer2, lhs_up_to_producer3,
		    lhs_up_to_producer4, lhs_up_to_producer5,
		    lhs_up_to_pre0, lhs_up_to_pre1,
		    lhs_up_to_pre2, lhs_up_to_pre3,
		    lhs_up_to_pre4 ]
    
    rhs_full_working    = quick_rhs_compose.(desc_full_working)
    rhs_full_job_errors = quick_rhs_compose.(desc_full_job_errors)
    rhs_up_to_producer0 = quick_rhs_compose.(desc_up_to_producer0)
    rhs_up_to_producer1 = quick_rhs_compose.(desc_up_to_producer1)
    rhs_up_to_producer2 = quick_rhs_compose.(desc_up_to_producer2)
    rhs_up_to_producer3 = quick_rhs_compose.(desc_up_to_producer3)
    rhs_up_to_producer4 = quick_rhs_compose.(desc_up_to_producer4)
    rhs_up_to_producer5 = quick_rhs_compose.(desc_up_to_producer5)
    rhs_up_to_pre0      = quick_rhs_compose.(desc_up_to_pre0)
    rhs_up_to_pre1      = quick_rhs_compose.(desc_up_to_pre1)
    rhs_up_to_pre2      = quick_rhs_compose.(desc_up_to_pre2)
    rhs_up_to_pre3      = quick_rhs_compose.(desc_up_to_pre3)
    rhs_up_to_pre4      = quick_rhs_compose.(desc_up_to_pre4)
    
    rhs_to_test = [ rhs_full_working, rhs_full_job_errors,
		    rhs_up_to_producer0, rhs_up_to_producer1,
		    rhs_up_to_producer2, rhs_up_to_producer3,
		    rhs_up_to_producer4, rhs_up_to_producer5,
		    rhs_up_to_pre0, rhs_up_to_pre1,
		    rhs_up_to_pre2, rhs_up_to_pre3,
		    rhs_up_to_pre4 ]


    side_by_side  = Enum.zip(lhs_to_test, rhs_to_test)
    side_by_index = side_by_side |> Enum.zip(1..13) |> Enum.map(fn {{a, b}, i} -> {a, b, i} end)

    for {lhs, rhs, i} <- side_by_index do
      Logger.info "Testing UUID ##{i}"
      Logger.info "LHS was #{inspect lhs}"
      Logger.info "RHS was #{inspect rhs}"
      assert lhs.runner_signal      == rhs.runner_signal
      assert lhs.runner_signal_full == rhs.runner_signal_full
    end
    
  end

end
