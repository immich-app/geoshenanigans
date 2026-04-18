resource "cloudflare_d1_database" "tiles" {
  account_id = var.cloudflare_account_id
  name       = var.env == "prod" ? "tiles" : "tiles-${var.env}"
  lifecycle {
    prevent_destroy = true
  }
}

output "d1_database_id" {
  value = cloudflare_d1_database.tiles.id
}
