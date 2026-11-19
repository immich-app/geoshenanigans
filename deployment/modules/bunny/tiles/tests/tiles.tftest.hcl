# Plans the module in each gzip_mode against a mocked provider (`tofu test`, run by the workflow's check job). What the
# edge rules do at the PoPs was measured on a test zone (README.md); this keeps the configuration that was measured.
mock_provider "bunnynet" {
  # The provider's own checks treat an id of 0 as none.
  mock_resource "bunnynet_storage_zone" {
    defaults = {
      id = 1
    }
  }

  mock_resource "bunnynet_compute_script" {
    defaults = {
      id = 2
    }
  }

  mock_resource "bunnynet_pullzone" {
    defaults = {
      cdn_domain = "b-cdn.net"
    }
  }
}

variables {
  bunny_api_key       = "not-a-key"
  storage_zone_name   = "immich-tiles"
  state_zone_name     = "immich-tiles-state"
  storage_region      = "DE"
  replication_regions = ["UK"]
  pull_zone_name      = "immich-tiles"
}

run "middleware" {
  command = plan

  variables {
    gzip_mode = "middleware"
  }

  assert {
    condition     = length(setintersection(keys(bunnynet_pullzone_edgerule.header), ["tile-content-type", "tile-content-encoding"])) == 0
    error_message = "The middleware sets the tile's Content-Type and Content-Encoding."
  }

  assert {
    condition     = bunnynet_pullzone.tiles.origin.middleware_script == bunnynet_compute_script.gzip.id && bunnynet_pullzone.tiles.routing.filters == toset(["scripting"])
    error_message = "The middleware is linked, and runs only on scripting PoPs."
  }

  assert {
    condition     = bunnynet_pullzone_edgerule.tile_force_compression.actions[0].type == "ForceCompression"
    error_message = "Force Compression on tiles in every mode."
  }
}

run "both" {
  command = plan

  variables {
    gzip_mode = "both"
  }

  assert {
    condition     = bunnynet_pullzone_edgerule.header["tile-content-encoding"].actions[0].parameter2 == "gzip"
    error_message = "both adds the Content-Encoding rule while the middleware stays linked."
  }

  assert {
    condition     = bunnynet_pullzone.tiles.origin.middleware_script == bunnynet_compute_script.gzip.id && bunnynet_pullzone.tiles.routing.filters == toset(["scripting"])
    error_message = "both keeps the middleware linked."
  }

  assert {
    condition     = bunnynet_pullzone_edgerule.tile_force_compression.actions[0].type == "ForceCompression"
    error_message = "Force Compression on tiles in every mode."
  }
}

run "edge_rule" {
  command = plan

  variables {
    gzip_mode = "edge_rule"
  }

  assert {
    condition     = bunnynet_pullzone.tiles.origin.middleware_script == 0 && bunnynet_pullzone.tiles.routing.filters == toset(["all"])
    error_message = "edge_rule unlinks the middleware and serves from every PoP."
  }

  # Deleting the script would print its deployment key in the plan.
  assert {
    condition     = bunnynet_compute_script.gzip.type == "middleware"
    error_message = "The middleware script exists in every mode."
  }

  # Without Force Compression, tiles cached through the middleware are corrupt under the Content-Encoding rule.
  assert {
    condition     = bunnynet_pullzone_edgerule.tile_force_compression.actions[0].type == "ForceCompression"
    error_message = "Force Compression on tiles in every mode."
  }

  # Bunny's 404 page for a missing file keeps Bunny's headers: no tile Cache-Control, Content-Type or Content-Encoding.
  assert {
    condition = alltrue([
      for rule in bunnynet_pullzone_edgerule.header : rule.match_type == "MatchAll" && anytrue([
        for trigger in rule.triggers : trigger.type == "StatusCode" && !contains(trigger.patterns, "404")
      ])
      if contains(["Cache-Control", "Content-Type", "Content-Encoding"], rule.actions[0].parameter1)
    ])
    error_message = "Every Cache-Control, Content-Type and Content-Encoding rule applies only to a file that exists."
  }

  assert {
    condition     = !contains(bunnynet_pullzone_edgerule.header["tile-content-encoding"].triggers[1].patterns, "304")
    error_message = "A 304 doesn't get the tile's Content-Encoding (as with the Worker)."
  }

  # The styles are also stored without .json: they need the JSON policy and type too, not the CDN's year.
  assert {
    condition = alltrue([
      contains(bunnynet_pullzone_edgerule.header["json-cache-control"].triggers[0].patterns, "*/style/*"),
      contains(bunnynet_pullzone_edgerule.header["json-content-type"].triggers[0].patterns, "*/style/*"),
      contains(bunnynet_pullzone_edgerule.json_cdn_cache_time.triggers[0].patterns, "*/style/*"),
    ])
    error_message = "v1/style/{name} gets the JSON Cache-Control, Content-Type and CDN time."
  }

  assert {
    condition     = bunnynet_pullzone.tiles.cache_expiration_time_browser == 300
    error_message = "A file no rule matches is kept 300 s by browsers, not the CDN's year."
  }

  assert {
    condition     = output.cdn_domain == "immich-tiles.b-cdn.net"
    error_message = "cdn_domain is the zone's own hostname, not the provider's suffix."
  }
}
