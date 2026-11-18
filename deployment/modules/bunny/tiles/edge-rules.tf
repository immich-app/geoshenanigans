# The headers prod's Worker sends (tiles/src/index.ts, cachePolicy). Tile URLs are stable and the daily sync purges the
# whole zone after an update, so tiles get the Worker's policy for unversioned tile URLs: a day, then shown from cache
# while revalidating (Last-Modified) for up to 30 days. The TileJSON and styles point clients at the data and are
# rechecked every 5 minutes. URL patterns match the full URL without its query string.
locals {
  header_rules = merge(
    {
      tile-cache-control = { pattern = "*.mvt", header = "Cache-Control", value = "public, max-age=86400, stale-while-revalidate=2592000" }
      json-cache-control = { pattern = "*.json", header = "Cache-Control", value = "public, max-age=300" }
      cors               = { pattern = "*", header = "Access-Control-Allow-Origin", value = "*" }
      vary-origin        = { pattern = "*", header = "Vary", value = "Origin" }
    },
    # Only once Bunny has fixed the PoPs that corrupt tiles when an edge rule adds Content-Encoding (README.md).
    local.middleware ? {} : {
      tile-content-type     = { pattern = "*.mvt", header = "Content-Type", value = "application/x-protobuf" }
      tile-content-encoding = { pattern = "*.mvt", header = "Content-Encoding", value = "gzip" }
    },
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

  match_type = "MatchAny"
  triggers = [{
    type       = "Url"
    match_type = "MatchAny"
    patterns   = [each.value.pattern]
    parameter1 = null
    parameter2 = null
  }]
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
    patterns   = ["*.json"]
    parameter1 = null
    parameter2 = null
  }]
}

# With the middleware marking tiles as gzip, Force Compression makes most PoPs pass the stored gzip through to clients;
# without it they are sent decompressed, because Bunny doesn't treat application/x-protobuf as compressible.
resource "bunnynet_pullzone_edgerule" "tile_force_compression" {
  count       = local.middleware ? 1 : 0
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
