locals {
  r2_regions = ["WNAM", "ENAM", "WEUR", "EEUR", "APAC"]
}

resource "cloudflare_r2_bucket" "tiles" {
  account_id = var.cloudflare_account_id
  name       = "tiles"
}

resource "cloudflare_r2_bucket" "regional_tiles" {
  for_each   = { for region in local.r2_regions : region => region }
  account_id = var.cloudflare_account_id
  name       = "tiles-${lower(each.value)}"
  location   = each.value
}

import {
  to = cloudflare_r2_bucket.tiles
  id = "${var.cloudflare_account_id}/tiles"
}

output "r2_regional_buckets" {
  value = { for region, bucket in cloudflare_r2_bucket.regional_tiles : region => bucket.id }
}
