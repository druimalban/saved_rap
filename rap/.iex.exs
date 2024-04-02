k0 = 1..20 |> Enum.into([])
k1 = 2..21 |> Enum.into([])
k2 = 3..22 |> Enum.into([])
k3 = 0..100 |> Enum.into([])
k4 = 20..120 |> Enum.into([])

job0 = { :job_dummy, k0, k1 }
job1 = { :job_dummy, k1, k2 }
job2 = { :job_dummy, k3, k4 }
