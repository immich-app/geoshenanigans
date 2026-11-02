data "cloudflare_zone" "immich_cloud" {
  name = "immich.cloud"
}

# Bearer tokens the tiles-deploy workflow uses to write deployments through the D1 proxy, one per environment so a
# dev deploy can't write to prod's database.
resource "random_password" "d1_proxy_token" {
  for_each = toset(["prod", "dev"])
  length   = 64
  special  = false
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

  secret_text_binding {
    name = "D1_PROXY_TOKEN_PROD"
    text = random_password.d1_proxy_token["prod"].result
  }

  secret_text_binding {
    name = "D1_PROXY_TOKEN_DEV"
    text = random_password.d1_proxy_token["dev"].result
  }

  d1_database_binding {
    database_id = cloudflare_d1_database.tiles.id
    name        = "D1_PROD"
  }

  d1_database_binding {
    database_id = cloudflare_d1_database.tiles_dev.id
    name        = "D1_DEV"
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

output "d1_proxy_url" {
  value = "https://${cloudflare_workers_domain.tiles.hostname}"
}

# Separate outputs so a deploy reads only its own environment's token.
output "d1_proxy_token_prod" {
  value     = random_password.d1_proxy_token["prod"].result
  sensitive = true
}

output "d1_proxy_token_dev" {
  value     = random_password.d1_proxy_token["dev"].result
  sensitive = true
}
