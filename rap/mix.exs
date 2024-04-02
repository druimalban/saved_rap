defmodule RAP.MixProject do
  use Mix.Project

  def project do
    [
      app: :rap,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end
  
  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {RAP.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:gen_stage, "~> 1.2"} #,
      # {:explorer, "~> 0.8"},
      # {:nx, "~> 0.7"}
    ]
  end
end
