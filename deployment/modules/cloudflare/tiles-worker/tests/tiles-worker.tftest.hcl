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
  bunny_pull_zone_id         = "12345"
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

# Prod's secrets are in tiles-worker-bunny.env, not .env: an apply that resolved .env alone stops instead of turning
# the log reader and the probe off. Dev runs neither and needs neither.
run "prod_without_the_bunny_secrets" {
  command = plan

  variables {
    bunny_api_key    = ""
    globalping_token = ""
  }

  expect_failures = [cloudflare_workers_script.tiles]
}

run "prod_without_the_globalping_token" {
  command = plan

  variables {
    globalping_token = ""
  }

  expect_failures = [cloudflare_workers_script.tiles]
}

run "dev_without_the_bunny_secrets" {
  command = plan

  variables {
    env              = "dev"
    bunny_api_key    = ""
    globalping_token = ""
  }

  assert {
    condition     = length(terraform_data.bunny_secrets) == 0
    error_message = "Dev puts no Bunny secret."
  }

  assert {
    condition     = length([for binding in cloudflare_workers_script.tiles.plain_text_binding : binding if startswith(binding.name, "BUNNY_")]) == 0
    error_message = "Dev runs neither the log reader nor the probe."
  }
}

# The two secrets are Worker secrets put through Cloudflare's API after the upload, never bindings: OpenTofu would keep
# a binding's value in the state.
run "prod_puts_the_bunny_secrets_outside_the_state" {
  command = plan

  variables {
    bunny_api_key    = "plan-stand-in"
    globalping_token = "plan-stand-in"
  }

  assert {
    condition     = length([for binding in cloudflare_workers_script.tiles.secret_text_binding : binding if binding.name == "BUNNY_API_KEY" || binding.name == "GLOBALPING_TOKEN"]) == 0
    error_message = "No Bunny secret is a binding of the script."
  }

  assert {
    condition     = jsonencode(terraform_data.bunny_secrets[0].input) == jsonencode({ script = "tiles-prod", secrets = ["BUNNY_API_KEY", "GLOBALPING_TOKEN"] })
    error_message = "Prod puts both secrets on its script, and only their names are in the resource."
  }

  assert {
    condition     = contains([for binding in cloudflare_workers_script.tiles.plain_text_binding : "${binding.name}=${binding.text}"], "BUNNY_PULL_ZONE_ID=12345")
    error_message = "The log reader gets the pull zone id."
  }
}

# Until the Bunny unit has applied there is no pull zone id: the log reader stays off and its key isn't put, while the
# probe runs. No Bunny state is read, so the worker applies either way.
run "prod_before_the_bunny_unit_applies" {
  command = plan

  variables {
    bunny_pull_zone_id = ""
  }

  assert {
    condition     = length([for binding in cloudflare_workers_script.tiles.plain_text_binding : binding if binding.name == "BUNNY_PULL_ZONE_ID"]) == 0
    error_message = "No pull zone id, no log reader."
  }

  assert {
    condition     = jsonencode(terraform_data.bunny_secrets[0].input.secrets) == jsonencode(["GLOBALPING_TOKEN"])
    error_message = "Only the probe's token is put."
  }
}

run "prod_before_the_probe_is_on" {
  command = plan

  variables {
    bunny_pull_zone_id = ""
    bunny_public_url   = ""
  }

  assert {
    condition     = length(terraform_data.bunny_secrets) == 0
    error_message = "Neither the log reader nor the probe runs, so no secret is put."
  }
}

run "pull_zone_id_not_a_number" {
  command = plan

  variables {
    bunny_pull_zone_id = "immich-tiles"
  }

  expect_failures = [var.bunny_pull_zone_id]
}

# The worker takes an absolute http(s) URL (tiles/src/bunny-logs/config.ts) and skips the probe on anything else, after
# the Globalping token has gone on: such a value stops the plan instead.
run "public_url_bare_hostname" {
  command = plan

  variables {
    bunny_public_url = "tiles.example.com"
  }

  expect_failures = [var.bunny_public_url]
}

run "public_url_not_http" {
  command = plan

  variables {
    bunny_public_url = "ftp://tiles.example.com"
  }

  expect_failures = [var.bunny_public_url]
}

run "public_url_with_a_query" {
  command = plan

  variables {
    bunny_public_url = "https://tiles.example.com/?v=1"
  }

  expect_failures = [var.bunny_public_url]
}

run "public_url_under_a_path" {
  command = plan

  variables {
    bunny_public_url = "https://cdn.example.com/tiles"
  }

  assert {
    condition     = contains([for binding in cloudflare_workers_script.tiles.plain_text_binding : "${binding.name}=${binding.text}"], "BUNNY_PUBLIC_URL=https://cdn.example.com/tiles")
    error_message = "A URL with a path is the probe's public URL."
  }
}
