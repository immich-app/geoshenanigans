data "cloudflare_zone" "immich_cloud" {
  name = "immich.cloud"
}

locals {
  # The Bunny unit's output is a number; "" before that unit has applied, and in dev, which doesn't read it.
  bunny_pull_zone_id = try(tostring(data.terraform_remote_state.bunny_tiles[0].outputs.pull_zone_id), "")
  # Only prod reads Bunny's logs and probes: the Bunny account is production's, and a dev worker would read the same pull
  # zone's logs again and spend the Globalping quota twice. Whether a secret is set isn't itself secret.
  bunny_log_reader = (var.env == "prod" && nonsensitive(var.bunny_api_key != "")
  && can(regex("^[0-9]+$", local.bunny_pull_zone_id)))
  bunny_probe = var.env == "prod" && nonsensitive(var.globalping_token != "") && var.bunny_public_url != ""
}

resource "cloudflare_workers_script" "tiles" {
  account_id = var.cloudflare_account_id
  name       = "tiles-${var.env}"
  content    = file("${var.tiles_build_dir}/index.js")
  module     = true

  plain_text_binding {
    name = "DEPLOYMENT_KEY"
    text = var.pmtiles_deployment_key
  }

  plain_text_binding {
    name = "ENVIRONMENT"
    text = var.env
  }

  dynamic "plain_text_binding" {
    for_each = var.storage_prefix == "" ? [] : [var.storage_prefix]
    content {
      name = "STORAGE_PREFIX"
      text = plain_text_binding.value
    }
  }

  # Metrics and logs go to FUTO's o11y stack through its vmauth gateway: prod to the production store, dev to staging.
  plain_text_binding {
    name = "O11Y_URL"
    text = var.env == "prod" ? "https://vmauth.futostatus.com" : "https://vmauth.staging.futostatus.com"
  }

  secret_text_binding {
    name = "O11Y_TOKEN"
    text = var.o11y_vmauth_token
  }

  secret_text_binding {
    name = "TIGRIS_KEY_ID"
    text = var.tigris_read_key_id
  }

  secret_text_binding {
    name = "TIGRIS_ACCESS_KEY"
    text = var.tigris_read_access_key
  }

  # Bunny CDN request logs and synthetic probe, run by the cron (tiles/src/bunny-logs).
  dynamic "secret_text_binding" {
    for_each = local.bunny_log_reader ? [1] : []
    content {
      name = "BUNNY_API_KEY"
      text = var.bunny_api_key
    }
  }

  dynamic "plain_text_binding" {
    for_each = local.bunny_log_reader ? [local.bunny_pull_zone_id] : []
    content {
      name = "BUNNY_PULL_ZONE_ID"
      text = plain_text_binding.value
    }
  }

  dynamic "secret_text_binding" {
    for_each = local.bunny_probe ? [1] : []
    content {
      name = "GLOBALPING_TOKEN"
      text = var.globalping_token
    }
  }

  dynamic "plain_text_binding" {
    for_each = local.bunny_probe ? {
      BUNNY_PUBLIC_URL             = var.bunny_public_url
      BUNNY_PROBE_INTERVAL_MINUTES = tostring(var.bunny_probe_interval_minutes)
    } : {}
    content {
      name = plain_text_binding.key
      text = plain_text_binding.value
    }
  }

  d1_database_binding {
    database_id = var.env != "prod" ? data.terraform_remote_state.tiles_state.outputs.d1_dev_database : data.terraform_remote_state.tiles_state.outputs.d1_global_database
    name        = "D1_GLOBAL"
  }

  compatibility_date  = "2024-07-29"
  compatibility_flags = ["nodejs_compat"]

  lifecycle {
    # Prod gets these from deployment/tiles-worker-bunny.env, which .env no longer names: a prod run that resolved .env
    # alone would otherwise turn the log reader and the probe off.
    precondition {
      condition     = var.env != "prod" || nonsensitive(var.bunny_api_key != "" && var.globalping_token != "")
      error_message = "Prod needs bunny_api_key and globalping_token: resolve deployment/tiles-worker-bunny.env too (op run --env-file=.env --env-file=tiles-worker-bunny.env)."
    }
  }
}

# Every minute the worker's scheduled handler sends o11y its heartbeat, which the bundle's
# geoshenanigans-tiles-heartbeat alert watches (o11y/README.md), reads a closed minute of Bunny's request logs and, every
# bunny_probe_interval_minutes, runs the synthetic probe (tiles/src/bunny-logs).
resource "cloudflare_workers_cron_trigger" "tiles" {
  account_id  = var.cloudflare_account_id
  script_name = cloudflare_workers_script.tiles.name
  schedules   = ["* * * * *"]
}

resource "cloudflare_workers_domain" "tiles" {
  account_id = var.cloudflare_account_id
  hostname   = "tiles.${var.env}.immich.cloud"
  service    = cloudflare_workers_script.tiles.id
  zone_id    = data.cloudflare_zone.immich_cloud.zone_id
}

resource "cloudflare_workers_domain" "tiles_prod" {
  count      = var.env == "prod" ? 1 : 0
  account_id = var.cloudflare_account_id
  hostname   = "tiles.immich.cloud"
  service    = cloudflare_workers_script.tiles.id
  zone_id    = data.cloudflare_zone.immich_cloud.zone_id
}
