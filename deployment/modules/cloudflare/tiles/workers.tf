data "cloudflare_zone" "immich_cloud" {
  name = "immich.cloud"
}

resource "cloudflare_workers_script" "tiles_d1_proxy" {
  account_id = var.cloudflare_account_id
  name       = "tiles-d1-proxy"
  content    = file("${var.tiles_build_dir}/index.js")
  module     = true

  plain_text_binding {
    name = "WORKER_TYPE"
    text = "D1_PROXY"
  }

  d1_database_binding {
    database_id = cloudflare_d1_database.tiles.id
    name        = "D1_GLOBAL"
  }

  compatibility_date  = "2024-07-29"
  compatibility_flags = ["nodejs_compat"]
}

resource "cloudflare_workers_domain" "tiles" {
  account_id = var.cloudflare_account_id
  hostname   = "tiles-d1-proxy.immich.cloud"
  service    = cloudflare_workers_script.tiles_d1_proxy.id
  zone_id    = data.cloudflare_zone.immich_cloud.zone_id
}
