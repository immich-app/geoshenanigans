locals {
  r2_regions = ["WNAM", "ENAM", "WEUR", "EEUR", "APAC", "OC"]
}

resource "cloudflare_r2_bucket" "regional_tiles" {
  for_each   = { for region in local.r2_regions : region => region }
  account_id = var.cloudflare_account_id
  name       = "tiles-${lower(each.value)}"
  location   = each.value
}

output "r2_regional_buckets" {
  value = { for region, bucket in cloudflare_r2_bucket.regional_tiles : region => bucket.id }
}
