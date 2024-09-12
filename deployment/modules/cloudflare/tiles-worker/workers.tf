data "cloudflare_zone" "immich_cloud" {
  name = "immich.cloud"
}

resource "cloudflare_workers_script" "tiles" {
  account_id = var.cloudflare_account_id
  name       = "tiles-${var.env}"
  content    = file("${var.tiles_build_dir}/index.js")
  module     = true

  plain_text_binding {
    name = "PMTILES_FILE_NAME"
    text = var.pmtiles_file_name
  }

  plain_text_binding {
    name = "ENVIRONMENT"
    text = var.env
  }

  secret_text_binding {
    name = "VMETRICS_API_TOKEN"
    text = var.vmetrics_api_token
  }

  dynamic "r2_bucket_binding" {
    for_each = data.terraform_remote_state.tiles_state.outputs.r2_regional_buckets
    content {
      name        = "BUCKET_${r2_bucket_binding.key}"
      bucket_name = r2_bucket_binding.value
    }
  }

  kv_namespace_binding {
    name         = "KV"
    namespace_id = data.terraform_remote_state.tiles_state.outputs.kv_namespace_id
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

import {
  for_each = var.env == "prod" ? { import : cloudflare_workers_script.tiles } : {}
  to       = cloudflare_workers_script.tiles
  id       = "${var.cloudflare_account_id}/tiles-prod"
}

import {
  for_each = var.env == "prod" ? { import : cloudflare_workers_domain.tiles } : {}
  to       = cloudflare_workers_domain.tiles
  id       = "${var.cloudflare_account_id}/17b3213004b9d5f077563e3c4ea3171ef9d0c5fc"
}
