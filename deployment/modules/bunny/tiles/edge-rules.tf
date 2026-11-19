# The headers prod's Worker sends (tiles/src/index.ts, cachePolicy). Tile URLs are stable and the daily sync purges the
# whole zone after an update, so tiles get the Worker's policy for unversioned tile URLs: a day, then shown from cache
# while revalidating (Last-Modified) for up to 30 days. The TileJSON and styles point clients at the data and are
# rechecked every 5 minutes. URL patterns match the full URL without its query string.
locals {
  # The TileJSON (v1.json) and the styles, which are stored both as v1/style/{name}.json and as v1/style/{name}.
  json_patterns = ["*.json", "*/style/*"]

  # Responses for a file that exists. Bunny's own page for a missing file (a 404) keeps the headers Bunny gives it,
  # Cache-Control: no-cache, as the Worker's 404 has no Cache-Control: a tile policy on it would make Bunny keep a
  # missing tile until the next purge and browsers for a day, and Content-Encoding: gzip would make it undecodable.
  # Like the Worker, a 304 gets the Cache-Control but not the tile's Content-Type and Content-Encoding.
  file_statuses = ["200", "206", "304"]
  body_statuses = ["200", "206"]

  header_rules = merge(
    {
      tile-cache-control = { patterns = ["*.mvt"], statuses = local.file_statuses, header = "Cache-Control", value = "public, max-age=86400, stale-while-revalidate=2592000" }
      json-cache-control = { patterns = local.json_patterns, statuses = local.file_statuses, header = "Cache-Control", value = "public, max-age=300" }
      # Storage types a file by its extension, and v1/style/{name} has none.
      json-content-type = { patterns = local.json_patterns, statuses = local.body_statuses, header = "Content-Type", value = "application/json" }
      cors              = { patterns = ["*"], statuses = [], header = "Access-Control-Allow-Origin", value = "*" }
      vary-origin       = { patterns = ["*"], statuses = [], header = "Vary", value = "Origin" }
    },
    # Only once Bunny has fixed the PoPs that corrupt tiles when an edge rule adds Content-Encoding (README.md). A
    # client that sends no Accept-Encoding: gzip then gets the tile decompressed, except one cached through the
    # middleware (gzip_mode "both", or edge_rule before the purge that follows the switch), which breaks for it.
    local.gzip_edge_rules ? {
      tile-content-type     = { patterns = ["*.mvt"], statuses = local.body_statuses, header = "Content-Type", value = "application/x-protobuf" }
      tile-content-encoding = { patterns = ["*.mvt"], statuses = local.body_statuses, header = "Content-Encoding", value = "gzip" }
    } : {},
  )
}

resource "bunnynet_pullzone_edgerule" "header" {
  for_each    = local.header_rules
  pullzone    = bunnynet_pullzone.tiles.id
  enabled     = true
  description = "${var.pull_zone_name}-${each.key}"

  actions = [{
    type       = "SetResponseHeader"
    parameter1 = each.value.header
    parameter2 = each.value.value
    parameter3 = null
  }]

  match_type = "MatchAll"
  triggers = concat(
    [{
      type       = "Url"
      match_type = "MatchAny"
      patterns   = each.value.patterns
      parameter1 = null
      parameter2 = null
    }],
    length(each.value.statuses) == 0 ? [] : [{
      type       = "StatusCode"
      match_type = "MatchAny"
      patterns   = each.value.statuses
      parameter1 = null
      parameter2 = null
    }],
  )
}

# The TileJSON and styles stay at most 5 minutes in the CDN as well, so a change reaches clients without a purge.
resource "bunnynet_pullzone_edgerule" "json_cdn_cache_time" {
  pullzone    = bunnynet_pullzone.tiles.id
  enabled     = true
  description = "${var.pull_zone_name}-json-cdn-cache-time"

  actions = [{
    type       = "OverrideCacheTime"
    parameter1 = "300"
    parameter2 = null
    parameter3 = null
  }]

  match_type = "MatchAny"
  triggers = [{
    type       = "Url"
    match_type = "MatchAny"
    patterns   = local.json_patterns
    parameter1 = null
    parameter2 = null
  }]
}

# With the middleware marking tiles as gzip, Force Compression makes most PoPs pass the stored gzip through to clients;
# without it they are sent decompressed, because Bunny doesn't treat application/x-protobuf as compressible. It stays in
# every gzip_mode: a tile cached through the middleware and sent decompressed is corrupt once the Content-Encoding edge
# rule labels it gzip, which is what every tile cached before a switch to edge_rule would be until the next purge.
# Without the middleware it changes nothing.
resource "bunnynet_pullzone_edgerule" "tile_force_compression" {
  pullzone    = bunnynet_pullzone.tiles.id
  enabled     = true
  description = "${var.pull_zone_name}-tile-force-compression"

  actions = [{
    type       = "ForceCompression"
    parameter1 = null
    parameter2 = null
    parameter3 = null
  }]

  match_type = "MatchAny"
  triggers = [{
    type       = "Url"
    match_type = "MatchAny"
    patterns   = ["*.mvt"]
    parameter1 = null
    parameter2 = null
  }]
}
