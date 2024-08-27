data "cloudflare_zone" "immich_cloud" {
  name = "immich.cloud"
}

resource "cloudflare_workers_script" "tiles" {
  account_id = var.cloudflare_account_id
  name       = "tiles-prod"
  content    = file("${var.tiles_build_dir}/index.js")
  module     = true

  plain_text_binding {
    name = "PMTILES_FILE_NAME"
    text = var.pmtiles_file_name
  }

  plain_text_binding {
    name = "ENVIRONMENT"
    text = "production"
  }

  secret_text_binding {
    name = "VMETRICS_API_TOKEN"
    text = var.vmetrics_api_token
  }

  r2_bucket_binding {
    name        = "BUCKET"
    bucket_name = cloudflare_r2_bucket.tiles.name
  }

  kv_namespace_binding {
    name         = "KV"
    namespace_id = cloudflare_workers_kv_namespace.tiles.id
  }
}

resource "cloudflare_workers_domain" "tiles" {
  account_id = var.cloudflare_account_id
  hostname   = "tiles.immich.cloud"
  service    = cloudflare_workers_script.tiles.id
  zone_id    = data.cloudflare_zone.immich_cloud.zone_id
}
