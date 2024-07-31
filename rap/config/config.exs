import Config

config :elixir, time_zone_database: Zoneinfo.TimeZoneDatabase
config :mnesia, dir: 'mnesia/#{Mix.env}/#{node()}'
config :rap,
  gcp_bucket:  "saved-fisdat",
  python_call: "/opt/local/bin/python3.12",
  bakery_directory: "./bakery"

# config :tesla, :adapter, Tesla.Adapter.Mint
