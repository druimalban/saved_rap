defmodule RAP.Test.Job.Producer do
  use Amnesia
  use ExUnit.Case, async: true
  use RDF
  doctest RAP.Job.Producer
  
  alias RAP.Job.Producer
  alias RAP.Manifest.TableDesc

  # Fix me
  alias RAP.Manifest.{TableDesc, ScopeDesc, JobDesc, ManifestDesc}
  alias RAP.Storage.{MidRun, GCP}
  alias RAP.Job.{ScopeSpec, ResourceSpec, TableSpec, JobSpec, ManifestSpec}
  
  test "Extraction of URIs various" do

    fp_good0 = "/metadata/test.rdf"
    fp_good1 = "/metadata/triples/unix/test.rdf"
    fp_bad0  = "/metadata/test.rdf/"
    fp_bad1  = "/metadata/triples/unix/test.rdf/"
    
    test_single0 = %URI{ path: "test.rdf" }
    test_single1 = %URI{ path: fp_good0   }
    test_single2 = %URI{ path: fp_bad0    }
    
    assert Producer.extract_uri(test_single0) == "test.rdf"
    assert Producer.extract_uri(test_single1) == fp_good0
    assert Producer.extract_uri(test_single2) == fp_bad0
    assert is_nil(Producer.extract_uri(%URI{}))

    test_full0 = %URI{ scheme: "https", path: fp_good0 }
    test_full1 = %URI{ scheme: "file",  path: fp_good1 }
    test_full2 = %URI{ scheme: "https", path: fp_bad0  }
    test_full3 = %URI{ scheme: "file",  path: fp_bad1  }

    assert Producer.extract_uri(test_full0) == "test.rdf"
    assert Producer.extract_uri(test_full1) == "test.rdf"
    assert Producer.extract_uri(test_full2) == "test.rdf"
    assert Producer.extract_uri(test_full3) == "test.rdf"
  end

  test "Extraction of IDs various" do

    id0 = RDF.iri("https://marine.gov.scot/metadata/saved/schema/RootManifest/")
    id1 = RDF.iri("https://marine.gov.scot/metadata/saved/schema/job_example_sampling/")
    id2 = RDF.iri("https://marine.gov.scot/metadata/saved/rap#")
    id3 = RDF.iri("https://marine.gov.scot/metadata/saved/schema#")
    id4 = RDF.iri("file://localhost/test.rdf")
    id5 = RDF.iri("test.rdf")
    id6 = RDF.iri("/some/where/place/test1#/test0#")
    id7 = RDF.iri("")

    assert Producer.extract_id(id0) == "RootManifest"
    assert Producer.extract_id(id1) == "job_example_sampling"
    assert Producer.extract_id(id2) == "rap#"
    assert Producer.extract_id(id3) == "schema#"
    assert Producer.extract_id(id4) == "test.rdf"
    assert Producer.extract_id(id5) == "test.rdf"
    assert Producer.extract_id(id6) == "test0#"
    assert Producer.extract_id(id7) == ""
    
  end

  test "Test table-checking" do

    id_sampling = RDF.iri("https://marine.gov.scot/metadata/saved/rap/sentinel_cages_sampling/")
    id_stations = RDF.iri("https://marine.gov.scot/metadata/saved/rap/sentinel_cages_site/")
    
    
    table_sampling = %TableDesc{
      __id__:           id_sampling,
      title:            "Sentinel cages sampling: known-good test table",
      resource_path:    %URI{path: "sentinel_cages_cleaned.csv"},
      schema_path_yaml: %URI{path: "sentinel_cages_sampling.yaml"},
      schema_path_ttl:  %URI{path: "sentinel_cages_sampling.ttl"},
      resource_hash:    "01512e62e56f3cb5b46ff453ac434ee0946fbccd4c36604960e0fee9a84fbe8816229ece8a2be3a68042e3d14fdc5ae0"
    }
    table_stations = %TableDesc{
      __id__:           id_stations,
      title:            "Sentinel cages site: known-good test table",
      resource_path:    %URI{path: "Sentinel_cage_station_info_6.csv"},
      schema_path_yaml: %URI{path: "sentinel_cages_site.yaml"},
      schema_path_ttl:  %URI{path: "sentinel_cages_site.ttl"},
      resource_hash:    "1bc2d590faa0093562e87dc01037afb83c186940737d87b0d74c498f2305c024e6847bc527bd09b2de9adafe9da3c124"
    }
    table_bad_resource = %TableDesc{
      __id__:           id_sampling,
      title:            "Sentinel cages sampling: test variant with bad resource",
      resource_path:    %URI{path: "cleaned.csv"},
      schema_path_yaml: %URI{path: "sentinel_cages_sampling.yaml"},
      schema_path_ttl:  %URI{path: "sentinel_cages_sampling.ttl"},
      resource_hash:    "01512e62e56f3cb5b46ff453ac434ee0946fbccd4c36604960e0fee9a84fbe8816229ece8a2be3a68042e3d14fdc5ae0"
    }
    table_bad_schemata = %TableDesc{
      __id__:           id_sampling,
      title:            "Sentinel cages sampling: test variant with bad schemata",
      resource_path:    %URI{path: "sentinel_cages_cleaned.csv"},
      schema_path_yaml: %URI{path: "sampling.yaml"},
      schema_path_ttl:  %URI{path: "sampling.ttl"},
      resource_hash:    "01512e62e56f3cb5b46ff453ac434ee0946fbccd4c36604960e0fee9a84fbe8816229ece8a2be3a68042e3d14fdc5ae0"
    }
    table_bad_general = %TableDesc{
      __id__:           id_stations,
      title:            "Sentinel cages site: test variant with various non-existent resources",
      resource_path:    %URI{path: "stations.csv"},
      schema_path_yaml: %URI{path: "stations.yaml"},
      schema_path_ttl:  %URI{path: "stations.ttl"},
      resource_hash:    "0xdeadbeef"
    }

    resources = [ "sentinel_cages_cleaned.csv",
		  "Sentinel_cage_station_info_6.csv",
		  "sentinel_cages_sampling.ttl",
		  "sentinel_cages_site.ttl"         ]

    # LHSen
    lhs_table_sampling = %TableSpec{
      name:     "sentinel_cages_sampling",
      title:    "Sentinel cages sampling: known-good test table",
      resource: %ResourceSpec{ base: "sentinel_cages_cleaned.csv",  extant: true },
      schema:   %ResourceSpec{ base: "sentinel_cages_sampling.ttl", extant: true } 
    }
    lhs_table_stations = %TableSpec{
      name:     "sentinel_cages_site",
      title:    "Sentinel cages site: known-good test table",
      resource: %ResourceSpec{ base: "Sentinel_cage_station_info_6.csv", extant: true },
      schema:   %ResourceSpec{ base: "sentinel_cages_site.ttl",          extant: true }
    }
    lhs_table_bad_resource = %TableSpec{
      name:     "sentinel_cages_sampling",
      title:    "Sentinel cages sampling: test variant with bad resource",
      resource: %ResourceSpec{ base: "cleaned.csv",                 extant: false },
      schema:   %ResourceSpec{ base: "sentinel_cages_sampling.ttl", extant: true  }
    }
    lhs_table_bad_schemata = %TableSpec{
      name:     "sentinel_cages_sampling",
      title:    "Sentinel cages sampling: test variant with bad schemata",
      resource: %ResourceSpec{ base: "sentinel_cages_cleaned.csv",  extant: true  },
      schema:   %ResourceSpec{ base: "sampling.ttl",                extant: false }
    }
    lhs_table_bad_misc = %TableSpec{
      name:     "sentinel_cages_site",
      title:    "Sentinel cages site: test variant with various non-existent resources",
      resource: %ResourceSpec{ base: "station_info.csv",  extant: false },
      schema:   %ResourceSpec{ base: "stations.ttl", extant: false}
    }

    #RHSen
    rhs_table_sampling     = Producer.check_table(table_sampling,     resources)
    rhs_table_stations     = Producer.check_table(table_stations,     resources)
    rhs_table_bad_resource = Producer.check_table(table_bad_resource, resources)
    rhs_table_bad_schemata = Producer.check_table(table_bad_schemata, resources)
    rhs_table_bad_misc     = Producer.check_table(table_bad_general,  resources)
  
    assert match?(lhs_table_sampling,     rhs_table_sampling)
    assert match?(lhs_table_stations,     rhs_table_stations)
    assert match?(lhs_table_bad_resource, rhs_table_bad_resource)
    assert match?(lhs_table_bad_schemata, rhs_table_bad_schemata)
    assert match?(lhs_table_bad_misc,     rhs_table_bad_misc)
    
  end

  # Can flesh this out later…
  test "Test load/injection of empty manifest" do
    assert match?({:error, :empty_manifest}, Producer.check_manifest(%ManifestDesc{}, nil, nil))
  end
  
  test "Test load/injection of manifest" do
    table_sampling = %TableSpec{
      name:     "sentinel_cages_sampling",
      title:    "Sentinel cages sampling: known-good test table",
      resource: %ResourceSpec{ base: "sentinel_cages_cleaned.csv",  extant: true },
      schema:   %ResourceSpec{ base: "sentinel_cages_sampling.ttl", extant: true } 
    }
    table_stations = %TableSpec{
      name:     "sentinel_cages_site",
      title:    "Sentinel cages site: known-good test table",
      resource: %ResourceSpec{ base: "Sentinel_cage_station_info_6.csv", extant: true },
      schema:   %ResourceSpec{ base: "sentinel_cages_site.ttl",          extant: true }
    }
    
    test_resources = [ "Sentinel_cage_station_info_6.csv",
		       "sentinel_cages_cleaned.csv",
		       "sentinel_cages_site.ttl",
		       "sentinel_cages_sampling.ttl" ]
    
    desc_working = %MidRun{
      signal:        :working,
      uuid:          "9a55d938-7f50-45b5-8960-08c78d73facc",
      manifest_name: "RootManifest",
      manifest_ttl:  "manifest.ttl",
      resources:     test_resources
    }
    desc_bad_cardinality = %MidRun{
      signal:        :working,
      uuid:          "9a55d938-7f50-45b5-8960-08c78d73facc",
      manifest_name: "RootManifest",
      manifest_ttl:  "manifest.bad_cardinality.ttl",
      resources:     test_resources
    }
    desc_bad_tables = %MidRun{
      signal:        :working,
      uuid:          "9a55d938-7f50-45b5-8960-08c78d73facc",
      manifest_name: "RootManifest",
      manifest_ttl:  "manifest.bad_tables.ttl",
      resources:     test_resources
    }
    desc_alt_base = %MidRun{
      # A valid RDF graph, but with different base
      signal:        :working,
      uuid:          "9a55d938-7f50-45b5-8960-08c78d73facc",
      manifest_name: "RootManifest",
      manifest_ttl:  "manifest.alt_base.ttl",
      resources:     test_resources
    }
    
    rhs_working         = Producer.invoke_manifest(desc_working,         "test/manual_test")
    rhs_bad_cardinality = Producer.invoke_manifest(desc_bad_cardinality, "test/manual_test")
    rhs_bad_tables      = Producer.invoke_manifest(desc_bad_tables,      "test/manual_test")
    #rhs_empty           = Producer.invoke_manifest(desc_empty,           "test/manual_test")
    alt_base     = "https://marine.gov.scot/metadata/saved/rap_alt/"
    rhs_alt_base = Producer.invoke_manifest(desc_alt_base, "test/manual_test", alt_base)

    assert match?(%ManifestSpec{signal: :working},             rhs_working)
    assert match?(%ManifestSpec{signal: :working},             rhs_alt_base)
    assert match?(%ManifestSpec{signal: :bad_input_graph},     rhs_bad_cardinality)
    assert match?(%ManifestSpec{signal: :bad_manifest_tables}, rhs_bad_tables)
    #assert match?(%ManifestSpec{signal: :empty_manifest},      rhs_empty)
  end
  
end

defmodule RAP.Test.Job.Result do
  use Amnesia
  use ExUnit.Case, async: false
  use RDF
  doctest RAP.Job.Result
  alias RAP.Job.{JobSpec, Result}

  require Logger

  test "Test external command wrapper error behaviour" do

    file_path_count   = "test/manual_test/7a0c9260-19b8-11ef-bd35-86d813ecdcdd/cagedata-10.csv"
    file_path_density = "test/manual_test/7a0c9260-19b8-11ef-bd35-86d813ecdcdd/density.csv"
    
    # Case 1: Exits cleanly with expected status 0
    res0 = Result.cmd_wrapper("python3.12", "contrib/density_count_ode.py", [
	  file_path_count,   "TOTAL",
	  file_path_density, "time",  "density"
	])
    # Case 2: Exits uncleanly with a different status
    res1 = Result.cmd_wrapper("python3.12", "contrib/density_count_ode.py", [
	  file_path_count,   "total",
	  file_path_density, "time",  "density"
	])
    res2 = Result.cmd_wrapper("python3.12", "contrib/ode.py", [
	  file_path_count,   "TOTAL",
	  file_path_density, "time",  "density"
	])
    # Case 3: Throw an ErlangError with :enoent
    res3 = Result.cmd_wrapper("python3.4", "contrib/density_count_ode.py", [
	  file_path_count,   "TOTAL",
	  file_path_density, "time",  "density"
	])
    
    assert match?({:run_success,  0, _res}, res0)
    assert match?({:run_error, _sig, _res}, res1)
    assert match?({:run_error, _sig, _res}, res2)
    assert match?({:call_error, _se, _res}, res3)
    
  end

  
  test "Test handling of dummy/ignored jobs" do

    test_uuid = "9a55d938-7f50-45b5-8960-08c78d73facc"
    cache_dir = "test/manual_test"
    
    desc_ignore = %JobSpec{ type: "ignore" }
    desc_fake   = %JobSpec{ type: "fake"   }

    lhs_ignore = %Result{
      type:     "ignore",
      signal:   :ok,
      contents: "Dummy/ignored job"
    }
    lhs_fake = %Result{
      type:     "fake",
      signal:   :error,
      contents: "Fake/unrecognised job"
    }
    rhs_ignore = Result.run_job(test_uuid, cache_dir, desc_ignore)
    rhs_fake   = Result.run_job(test_uuid, cache_dir, desc_fake)
    
    assert match?(lhs_ignore, rhs_ignore)
    assert match?(lhs_fake,   rhs_fake)

  end
  
end
