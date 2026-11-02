import {
  to = tigris_bucket.tiles_geo
  id = "tiles-geo"
}

resource "tigris_bucket" "tiles_geo" {
  bucket               = "tiles-geo"
  default_storage_tier = "STANDARD"
  enable_snapshot      = false

  # Matches the bucket as Tigris has it: ams appeared on 2026-11-02 and Tigris no longer accepts hkg for new dual
  # configurations, so any change to this list is refused until hkg goes.
  location {
    type = "dual"
    regions = [
      "ams",
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
