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

  secret_text_binding {
    name = "VMETRICS_API_TOKEN"
    text = var.vmetrics_api_token
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
