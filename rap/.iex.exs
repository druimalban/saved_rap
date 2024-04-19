k0 = 1..20 |> Enum.into([])
k1 = 2..21 |> Enum.into([])
k2 = 3..22 |> Enum.into([])
k3 = 0..100 |> Enum.into([])
k4 = 20..120 |> Enum.into([])

job0dummy = { :job_dummy, k0, k1 }
job1dummy = { :job_dummy, k1, k2 }
job2dummy = { :job_dummy, k3, k4 }

job0mae = { :job_mae, k0, k1 }
job1mae = { :job_mae, k1, k2 }
job2mae = { :job_mae, k3, k4 }

job0rmsd = { :job_rmsd, k0, k1 }
job1rmsd = { :job_rmsd, k1, k2 }
job2rmsd = { :job_rmsd, k3, k4 }

menagerie = [ job0dummy, job1dummy, job2dummy,
	      job0mae,   job1mae,   job2mae,
	      job0rmsd,  job1rmsd,  job2rmsd ]


# {:ok,graph0} = RDF.Turtle.read_file "manual_test/manifest0.ttl"
# {:ok,graph1} = RDF.Turtle.read_file "manual_test/manifest1.ttl"
# {:ok,egret0} = Grax.load graph0, RAP.Vocabulary.SAVED.RootManifest, RAP.Manifest.ManifestDesc
# {:ok,egret1} = Grax.load graph1, RAP.Vocabulary.SAVED.RootManifest, RAP.Manifest.ManifestDesc

# tables   = egret1.tables
# base_iri = "http://localhost/saved/"
#[res0] = RAP.Job.Producer.check_sources_in_tables base_iri, tables, job0
#res_inv = { :invalid_table, "arr", "inv" }

# {:ok, graph0} = RDF.Turtle.read_file "manual_test/manifest3.ttl"

alias RDF.Graph
import RDF.Sigils
alias RAP.Vocabulary.SAVED
alias RAP.Manifest.{ManifestDesc,SourceDesc,TableDesc,JobDesc,Plumbing}

{:ok, graph0} = RDF.Turtle.read_file "manual_test/manifest3.ttl"
# {:ok, egret0} = Grax.load graph0, SAVED.RootManifest, ManifestDesc

{:ok, graph1} = RDF.Turtle.read_file "manual_test/manifest4.ttl"
# {:ok, egret1} = Grax.load graph1, SAVED.RootManifest, ManifestDesc
