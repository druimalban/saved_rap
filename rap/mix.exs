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
      extra_applications: [:logger, :grax],
      mod: {RAP.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:amnesia,             "~> 0.2.8" },
      { :gen_stage,          "~> 1.2"   },
      { :google_api_storage, "~> 0.37"  },
      { :goth,               "~> 1.4"   },
      { :grax,               "~> 0.4"   },
      { :math,               "~> 0.7"   },
      { :rdf,                "~> 1.2"   },
      { :uuid,               "~> 1.1.8" },
      { :zoneinfo,           "~> 0.1.0" }
      # {:explorer, "~> 0.8"},
      # {:nx, "~> 0.7"}
    ]
  end

  defp package do
    [files: ["lib", "mix.exs", "README.md", "LICENSE", "config", "priv"],
     maintainers: ["Duncan Guthrie"],
     licenses: [" AGPL-3.0-or-later"]
  end
end
