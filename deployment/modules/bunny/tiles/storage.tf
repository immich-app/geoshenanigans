# Every tile position as its own file (/v1/{z}/{x}/{y}.mvt, the gzip bytes of the Protomaps build), plus the TileJSON
# and styles. Edge SSD in the main region, replicated to every other SSD region. Imported (README.md).
resource "bunnynet_storage_zone" "tiles" {
  name                = var.storage_zone_name
  region              = var.storage_region
  zone_tier           = "Edge"
  replication_regions = var.replication_regions

  # Every position is uploaded, ocean included, so a missing file is a real 404 rather than a fallback tile.
  rewrite_404_to_200 = false

  lifecycle {
    prevent_destroy = true
  }
}

# The sync's manifests and state.json. Never served: no pull zone, no replicas. Imported (README.md).
resource "bunnynet_storage_zone" "state" {
  name      = var.state_zone_name
  region    = var.storage_region
  zone_tier = "Standard"

  lifecycle {
    prevent_destroy = true
  }
}

output "storage_zone_id" {
  value = bunnynet_storage_zone.tiles.id
}

output "state_zone_id" {
  value = bunnynet_storage_zone.state.id
}
