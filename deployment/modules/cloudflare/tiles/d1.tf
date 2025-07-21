resource "cloudflare_d1_database" "tiles" {
  account_id = var.cloudflare_account_id
  name       = "tiles"
  lifecycle {
    prevent_destroy = true
  }
}

output "d1_global_database" {
  value = cloudflare_d1_database.tiles.id
}
