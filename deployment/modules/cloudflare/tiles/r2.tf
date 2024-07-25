resource "cloudflare_r2_bucket" "tiles" {
  account_id = var.cloudflare_account_id
  name       = "tiles"
}

import {
  to = cloudflare_r2_bucket.tiles
  id = "${var.cloudflare_account_id}/tiles"
}
