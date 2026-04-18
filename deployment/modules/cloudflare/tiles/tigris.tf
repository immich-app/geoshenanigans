resource "tigris_bucket" "tiles_geo" {
  bucket               = var.env == "prod" ? "tiles-geo" : "tiles-geo-${var.env}"
  default_storage_tier = "STANDARD"
  enable_snapshot      = false

  location {
    type = "dual"
    regions = [
      "fra",
      "gru",
      "hkg",
      "iad",
      "jnb",
      "lhr",
      "nrt",
      "ord",
      "sin",
      "sjc",
      "syd",
    ]
  }
}

output "tigris_bucket_name" {
  value = tigris_bucket.tiles_geo.bucket
}
