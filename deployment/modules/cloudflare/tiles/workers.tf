data "cloudflare_zone" "immich_cloud" {
  name = "immich.cloud"
}

resource "cloudflare_worker_script" "tiles" {
  account_id = var.cloudflare_account_id
  name       = "tiles"
  content    = file("${path.module}/dist/tiles.js")
  module     = true

  plain_text_binding {
    name = "CACHE_CONTROL"
    text = "public, max-age=2678400" // 31 days - cloudflare cdn maximum
  }

  plain_text_binding {
    name = "ALLOWED_ORIGINS"
    text = "*"
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

resource "cloudflare_worker_domain" "tiles" {
  account_id = var.cloudflare_account_id
  hostname   = "tiles.immich.cloud"
  service    = cloudflare_worker_script.tiles.id
  zone_id    = data.cloudflare_zone.immich_cloud.zone_id
}

import {
  to = cloudflare_worker_script.tiles
  id = "${var.cloudflare_account_id}/tiles"
}
