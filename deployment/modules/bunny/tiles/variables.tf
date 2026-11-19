# The Bunny account API key (1Password tf_bunny/BUNNY_API_KEY, see bunny.env). Only OpenTofu, the daily purge and the log
# reader use it; uploads use the storage zone's own password.
variable "bunny_api_key" {
  type      = string
  sensitive = true
}

# The zone holding every tile position as a file, and the private zone with the sync's manifests and state. Both were
# created by hand and are imported (README.md); a change to the name, region or tier would replace them.
variable "storage_zone_name" {
  type = string
}

variable "state_zone_name" {
  type = string
}

variable "storage_region" {
  type = string
}

# Bunny can't remove a replication region once it's added.
variable "replication_regions" {
  type = set(string)
}

variable "pull_zone_name" {
  type = string
}

# How tiles get Content-Encoding: gzip. An edge rule setting it breaks tiles at some PoPs (README.md), so until Bunny
# fixes that a middleware script sets it on the origin response. "both" is the step between the two, in either
# direction.
variable "gzip_mode" {
  type    = string
  default = "middleware"

  validation {
    condition     = contains(["middleware", "both", "edge_rule"], var.gzip_mode)
    error_message = "gzip_mode must be \"middleware\", \"both\" or \"edge_rule\"."
  }
}

# A hostname to try the zone on before the DNS cutover, e.g. tiles-bunny.immich.cloud; null for none.
variable "test_hostname" {
  type    = string
  default = null
}

# The production hostname, attached once serve_public_hostname is true (the cutover).
variable "public_hostname" {
  type    = string
  default = "tiles.immich.cloud"
}

variable "serve_public_hostname" {
  type    = bool
  default = false
}

# The attached hostnames that get a Bunny-managed certificate (and HTTP to HTTPS redirects). Bunny obtains it over HTTP,
# so a name may only be listed here once its DNS points at the pull zone (output cdn_domain).
variable "tls_hostnames" {
  type    = set(string)
  default = []
}
