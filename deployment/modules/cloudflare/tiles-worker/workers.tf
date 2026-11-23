data "cloudflare_zone" "immich_cloud" {
  filter = {
    name = "immich.cloud"
  }
}

locals {
  script_name = "tiles-${var.env}"
  # Every hostname the worker answers on.
  hostnames = var.env == "prod" ? ["tiles.immich.cloud", "tiles.prod.immich.cloud"] : ["tiles.${var.env}.immich.cloud"]
}

resource "cloudflare_workers_script" "tiles" {
  account_id  = var.cloudflare_account_id
  script_name = local.script_name
  content     = file("${var.tiles_build_dir}/index.js")
  main_module = "worker.js"

  bindings = concat(
    [
      { type = "plain_text", name = "DEPLOYMENT_KEY", text = var.pmtiles_deployment_key },
      { type = "plain_text", name = "ENVIRONMENT", text = var.env },
    ],
    var.storage_prefix == "" ? [] : [{ type = "plain_text", name = "STORAGE_PREFIX", text = var.storage_prefix }],
    [
      # Metrics and logs go to FUTO's o11y stack through its vmauth gateway: prod to the production store, dev to
      # staging.
      {
        type = "plain_text"
        name = "O11Y_URL"
        text = var.env == "prod" ? "https://vmauth.futostatus.com" : "https://vmauth.staging.futostatus.com"
      },
      { type = "secret_text", name = "O11Y_TOKEN", text = var.o11y_vmauth_token },
      { type = "secret_text", name = "TIGRIS_KEY_ID", text = var.tigris_read_key_id },
      { type = "secret_text", name = "TIGRIS_ACCESS_KEY", text = var.tigris_read_access_key },
      {
        type = "d1"
        name = "D1_GLOBAL"
        id   = var.env != "prod" ? data.terraform_remote_state.tiles_state.outputs.d1_dev_database : data.terraform_remote_state.tiles_state.outputs.d1_global_database
      },
    ],
  )

  compatibility_date  = "2024-07-29"
  compatibility_flags = ["nodejs_compat"]
}

# Every minute the worker's scheduled handler sends o11y its heartbeat, which the bundle's
# geoshenanigans-tiles-heartbeat alert watches (o11y/README.md).
resource "cloudflare_workers_cron_trigger" "heartbeat" {
  account_id  = var.cloudflare_account_id
  script_name = cloudflare_workers_script.tiles.script_name
  schedules   = [{ cron = "* * * * *" }]
}

resource "cloudflare_workers_custom_domain" "tiles" {
  for_each   = toset(local.hostnames)
  account_id = var.cloudflare_account_id
  hostname   = each.value
  service    = cloudflare_workers_script.tiles.script_name
  zone_id    = data.cloudflare_zone.immich_cloud.id
}

# Provider v4 left the cron trigger and the domains in state in a form v5 can't read (a cloudflare_workers_domain type
# v5 doesn't have, and v4's cron schedules, which v5's state upgrader rejects), and v5 can't move either. The deploy
# removes those entries from state (deployment/scripts/tiles-worker-v4-state.sh), and these blocks adopt the live
# objects without changing them. An import whose target is already in state does nothing. Remove the blocks, the data
# source and the script once prod and dev have applied.
data "cloudflare_workers_custom_domains" "attached" {
  account_id = var.cloudflare_account_id
  service    = local.script_name
}

import {
  for_each = {
    for domain in data.cloudflare_workers_custom_domains.attached.result : domain.hostname => domain.id
    if contains(local.hostnames, domain.hostname)
  }
  to = cloudflare_workers_custom_domain.tiles[each.key]
  id = "${var.cloudflare_account_id}/${each.value}"
}

import {
  to = cloudflare_workers_cron_trigger.heartbeat
  id = "${var.cloudflare_account_id}/${local.script_name}"
}
