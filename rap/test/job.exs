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
      resource: %ResourceSpec{ base: "sentinel_cages_cleaned.csv",  extant: true },
      schema:   %ResourceSpec{ base: "sentinel_cages_site.ttl",     extant: true }
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
      resource: %ResourceSpec{ base: "stations.csv",  extant: false },
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
    # with {:ok, rdf_graph}    <- RDF.Turtle.read_file(manifest_full_path),
    #      {:ok, ex_struct}    <- Grax.load(rdf_graph, load_target, ManifestDesc),
    #      {:ok, manifest_obj} <- check_manifest(ex_struct, prev, :working)
    #  do

    # CHECK NOTE MADE EARLIER ABOUT POSSIBLE TEST CASES
    # Test case #1: :working:             Good RDF graph, good load target, wholly valid tables
    # Test case #2: :bad_manifest_tables: Good RDF graph, good load target, bad table reference
    # Test case #3: :empty_manifest:      Good RDF graph, bad load target
    # Test case #4: :bad_input_graph:     Bad RDF graph

    desc_working = %MidRun{
      signal:        :working,
      uuid:          "9a55d938-7f50-45b5-8960-08c78d73facc",
      manifest_name: "RootManifest",
      manifest_ttl:  "manifest0.ttl"
    }

    assert match?(%ManifestSpec{signal: :working}, Producer.invoke_manifest(desc_working, "test/manual_test"))
  end
  
end
