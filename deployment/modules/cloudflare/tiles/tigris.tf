import {
  to = tigris_bucket.tiles_geo
  id = "tiles-geo"
}

resource "tigris_bucket" "tiles_geo" {
  bucket               = "tiles-geo"
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
