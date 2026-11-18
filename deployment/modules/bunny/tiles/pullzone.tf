locals {
  hostnames = toset(compact([var.test_hostname, var.serve_public_hostname ? var.public_hostname : null]))
}

# Serves the storage zone with no script in the request path except the gzip middleware (gzip_mode), which runs on
# cache misses only. Changes here can take ~48 minutes to reach every PoP: make them well ahead of anything that depends
# on them, never from the daily sync.
resource "bunnynet_pullzone" "tiles" {
  name = var.pull_zone_name

  origin {
    type              = "StorageZone"
    storagezone       = bunnynet_storage_zone.tiles.id
    middleware_script = local.middleware ? bunnynet_compute_script.gzip[0].id : 0
  }

  routing {
    tier = "Standard"
    # A middleware script runs only on the PoPs that support scripting.
    filters = local.middleware ? ["scripting"] : ["all"]
  }

  # Tiles change only through the daily sync, which purges the whole zone once its uploads have replicated, so the CDN
  # keeps every file for a year (smart cache off: whatever its type). The TileJSON and styles get 300 s (edge rule).
  # What browsers keep comes from the Cache-Control edge rules.
  cache_enabled                 = false
  cache_expiration_time         = 31536000
  cache_expiration_time_browser = -1
  cache_errors                  = false
  cache_chunked                 = false
  # No "querystring" here means IgnoreQueryStrings: one cached copy per path, whatever ?v= a client sends.
  cache_vary = []

  originshield_enabled   = false
  permacache_storagezone = 0

  # The log reader's source (Logging API v2), client addresses with their last digit dropped.
  log_enabled          = true
  log_anonymized       = true
  log_anonymized_style = "OneDigit"

  # CORS comes from an edge rule on every file; the built-in one only covers a list of web extensions.
  cors_enabled        = false
  block_post_requests = true
  websockets_enabled  = false

  depends_on = [terraform_data.publish_gzip]

  lifecycle {
    # Renaming replaces the zone: a new id for the purge and the log reader, and every PoP without it for up to ~48 min.
    prevent_destroy = true

    precondition {
      condition     = length(setsubtract(var.tls_hostnames, local.hostnames)) == 0
      error_message = "Every name in tls_hostnames must be an attached hostname (test_hostname, or public_hostname with serve_public_hostname)."
    }
  }
}

resource "bunnynet_pullzone_hostname" "custom" {
  for_each    = local.hostnames
  pullzone    = bunnynet_pullzone.tiles.id
  name        = each.value
  tls_enabled = contains(var.tls_hostnames, each.value)
  force_ssl   = contains(var.tls_hostnames, each.value)
}

# BUNNY_PULL_ZONE_ID for the purge and the log reader.
output "pull_zone_id" {
  value = bunnynet_pullzone.tiles.id
}

# Where the custom hostnames' CNAME records point.
output "cdn_domain" {
  value = bunnynet_pullzone.tiles.cdn_domain
}

output "hostnames" {
  value = sort(local.hostnames)
}
