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

base_iri = "http://localhost/saved/"

alias RDF.Graph
import RDF.Sigils
alias RAP.Manifest.{ManifestDesc,SourceDesc,TableDesc,JobDesc,Plumbing}
alias RAP.Job.{Producer,Runner}

#{:ok, graph} = RDF.Turtle.read_file "manual_test/manifest7.ttl"
#{:ok, egret} = Grax.load graph, SAVED.RootManifest, ManifestDesc

alias RAP.Storage.{Monitor,TestConsumer}

spew_events = fn -> GenStage.cast(Monitor, {:stage_objects, Enum.into(1..10, [])}) end
