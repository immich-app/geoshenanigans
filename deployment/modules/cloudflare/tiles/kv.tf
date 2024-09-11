resource "cloudflare_workers_kv_namespace" "tiles" {
  account_id = var.cloudflare_account_id
  title      = "tiles"
}

import {
  to = cloudflare_workers_kv_namespace.tiles
  id = "${var.cloudflare_account_id}/5a4b82694e8b490db8b8904cdaea4f00"
}

output "kv_namespace_id" {
  value = cloudflare_workers_kv_namespace.tiles.id
}
