data "cloudflare_zone" "immich_cloud" {
  name = "immich.cloud"
}

resource "random_password" "d1_proxy_token" {
  length  = 64
  special = false
}

output "d1_proxy_token" {
  value     = random_password.d1_proxy_token.result
  sensitive = true
}

resource "cloudflare_workers_script" "tiles_d1_proxy" {
  account_id = var.cloudflare_account_id
  name       = var.env == "prod" ? "tiles-d1-proxy" : "tiles-d1-proxy-${var.env}"
  content    = file("${var.tiles_build_dir}/index.js")
  module     = true

  plain_text_binding {
    name = "WORKER_TYPE"
    text = "D1_PROXY"
  }

  secret_text_binding {
    name = "D1_PROXY_TOKEN"
    text = random_password.d1_proxy_token.result
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
  hostname   = var.env == "prod" ? "tiles-d1-proxy.immich.cloud" : "tiles-d1-proxy.${var.env}.immich.cloud"
  service    = cloudflare_workers_script.tiles_d1_proxy.id
  zone_id    = data.cloudflare_zone.immich_cloud.zone_id
}

output "d1_proxy_url" {
  value = "https://${cloudflare_workers_domain.tiles.hostname}"
}
