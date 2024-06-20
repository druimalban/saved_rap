Application.ensure_all_started(:grax)
Application.ensure_all_started(:mnesia)
Application.ensure_all_started(:zoneinfo)
ExUnit.start()
