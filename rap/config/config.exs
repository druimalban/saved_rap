import Config

config :elixir, time_zone_database: Zoneinfo.TimeZoneDatabase
config :mnesia, dir: 'mnesia/#{Mix.env}/#{node()}'
# config :tesla, :adapter, Tesla.Adapter.Mint
