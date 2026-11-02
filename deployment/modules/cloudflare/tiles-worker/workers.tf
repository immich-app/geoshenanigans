data "cloudflare_zone" "immich_cloud" {
  filter = {
    name = "immich.cloud"
  }
}

locals {
  # Every hostname the worker answers on. The first is canonical: Workers Cache doesn't key by host, so the TileJSON and
  # dev styles point clients there whichever host they asked.
  hostnames = var.env == "prod" ? ["tiles.immich.cloud", "tiles.prod.immich.cloud"] : ["tiles.${var.env}.immich.cloud"]
}

resource "cloudflare_workers_script" "tiles" {
  account_id  = var.cloudflare_account_id
  script_name = "tiles-${var.env}"
  content     = file("${var.tiles_build_dir}/index.js")
  # The name the v4 state upgrader gives a module worker's main module.
  main_module = "worker.js"

  bindings = concat(
    [
      { type = "plain_text", name = "DEPLOYMENT_KEY", text = var.pmtiles_deployment_key },
      { type = "plain_text", name = "ENVIRONMENT", text = var.env },
      { type = "plain_text", name = "PUBLIC_URL", text = "https://${local.hostnames[0]}" },
      { type = "secret_text", name = "VMETRICS_API_TOKEN", text = var.vmetrics_api_token },
      { type = "secret_text", name = "TIGRIS_KEY_ID", text = var.tigris_read_key_id },
      { type = "secret_text", name = "TIGRIS_ACCESS_KEY", text = var.tigris_read_access_key },
      {
        type = "d1"
        name = "D1_GLOBAL"
        id   = var.env != "prod" ? data.terraform_remote_state.tiles_state.outputs.d1_dev_database : data.terraform_remote_state.tiles_state.outputs.d1_global_database
      },
    ],
    var.storage_prefix == "" ? [] : [{ type = "plain_text", name = "STORAGE_PREFIX", text = var.storage_prefix }],
  )

  # Keep in step with tiles/wrangler.toml, which the bundle is built for.
  compatibility_date  = "2026-11-01"
  compatibility_flags = ["nodejs_compat"]

  # Workers Cache: the default export (the gateway) runs on every request; the Tiles entrypoint behind it is cached.
  # Entries outlive code-only deploys, and the gateway puts the deployment key in their URLs, so a data deploy starts a
  # fresh set.
  cache_options = {
    enabled             = true
    cross_version_cache = true
  }
  exports = {
    default = { type = "worker", cache = { enabled = false } }
    Tiles   = { type = "worker", cache = { enabled = true } }
  }
}

resource "cloudflare_workers_custom_domain" "tiles" {
  for_each   = toset(local.hostnames)
  account_id = var.cloudflare_account_id
  hostname   = each.value
  service    = cloudflare_workers_script.tiles.script_name
  zone_id    = data.cloudflare_zone.immich_cloud.id
}

# Adopts the domains the v4 provider attached (as cloudflare_workers_domain, which v5 can neither read nor move) without
# touching them; see MIGRATION.md. Only domains that exist are imported, and an import of a domain already in state is a
# no-op. Both blocks can go once dev and prod have applied.
data "cloudflare_workers_custom_domains" "existing" {
  for_each   = toset(local.hostnames)
  account_id = var.cloudflare_account_id
  hostname   = each.value
}

import {
  for_each = {
    for hostname, found in data.cloudflare_workers_custom_domains.existing :
    hostname => one([for domain in found.result : domain.id if domain.hostname == hostname])
    if anytrue([for domain in found.result : domain.hostname == hostname])
  }
  to = cloudflare_workers_custom_domain.tiles[each.key]
  id = "${var.cloudflare_account_id}/${each.value}"
}
