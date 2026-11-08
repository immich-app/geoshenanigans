data "cloudflare_zone" "immich_cloud" {
  name = "immich.cloud"
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

  d1_database_binding {
    database_id = var.env != "prod" ? data.terraform_remote_state.tiles_state.outputs.d1_dev_database : data.terraform_remote_state.tiles_state.outputs.d1_global_database
    name        = "D1_GLOBAL"
  }

  compatibility_date  = "2024-07-29"
  compatibility_flags = ["nodejs_compat"]
}

# Every minute the worker's scheduled handler sends o11y its heartbeat, which the bundle's
# geoshenanigans-tiles-heartbeat alert watches (o11y/README.md).
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
