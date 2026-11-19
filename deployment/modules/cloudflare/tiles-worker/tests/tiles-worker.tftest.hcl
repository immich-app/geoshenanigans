# Plans the module against mocked providers and remote state (`tofu test`), with a stand-in for the built worker.
mock_provider "cloudflare" {}

mock_provider "grafana" {}

override_data {
  target = data.terraform_remote_state.api_keys_state
  values = {
    outputs = { terraform_key_cloudflare_account = "not-a-token" }
  }
}

override_data {
  target = data.terraform_remote_state.tiles_state
  values = {
    outputs = { d1_dev_database = "dev-database", d1_global_database = "global-database" }
  }
}

override_data {
  target = data.terraform_remote_state.bunny_tiles
  values = {
    outputs = { pull_zone_id = 12345 }
  }
}

variables {
  cloudflare_account_id      = "account"
  tf_state_postgres_conn_str = "postgres://unused"
  pmtiles_deployment_key     = "20261101"
  storage_prefix             = ""
  tiles_build_dir            = "tests/build"
  env                        = "prod"
  o11y_vmauth_token          = "not-a-token"
  grafana_url                = "https://grafana.invalid"
  grafana_token              = "not-a-token"
  tigris_read_key_id         = "not-a-key"
  tigris_read_access_key     = "not-a-key"
  bunny_api_key              = "not-a-key"
  globalping_token           = "not-a-token"
  bunny_public_url           = "https://tiles.example.com"
}

# The probe alerts' 30-minute windows need two runs at all times (o11y/alerts/geoshenanigans-tiles.yaml): 14 minutes
# apart is the longest that keeps them, and the worker brings other values into 5 to 14
# (tiles/src/bunny-logs/config.ts).
run "probe_interval_longest" {
  command = plan

  variables {
    bunny_probe_interval_minutes = 14
  }

  assert {
    condition     = contains([for binding in cloudflare_workers_script.tiles.plain_text_binding : "${binding.name}=${binding.text}"], "BUNNY_PROBE_INTERVAL_MINUTES=14")
    error_message = "The worker probes every 14 minutes."
  }
}

run "probe_interval_too_long" {
  command = plan

  variables {
    bunny_probe_interval_minutes = 15
  }

  expect_failures = [var.bunny_probe_interval_minutes]
}

run "probe_interval_too_short" {
  command = plan

  variables {
    bunny_probe_interval_minutes = 4
  }

  expect_failures = [var.bunny_probe_interval_minutes]
}

run "probe_interval_fraction" {
  command = plan

  variables {
    bunny_probe_interval_minutes = 7.5
  }

  expect_failures = [var.bunny_probe_interval_minutes]
}
