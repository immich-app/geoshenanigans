import {
  to = tigris_bucket.reverse_geocoding
  id = "geoshenanigans-reverse-geocoding"
}

resource "tigris_bucket" "reverse_geocoding" {
  bucket               = "geoshenanigans-reverse-geocoding"
  default_storage_tier = "STANDARD"
  enable_snapshot      = false

  location {
    type = "dual"
    regions = [
      "ams",
      "fra",
      "gru",
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
