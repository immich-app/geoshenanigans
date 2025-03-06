resource "cloudflare_d1_database" "tiles_lookup" {
  account_id = var.cloudflare_account_id
  name       = "tiles-lookup"
  lifecycle {
    prevent_destroy = true
  }
}

output "tiles_lookup_d1_id" {
  value = cloudflare_d1_database.tiles_lookup.id
}

import {
  to = cloudflare_d1_database.tiles_lookup
  id = "${var.cloudflare_account_id}/d498d485-7709-410a-9e16-143c8ff4f016"
}
