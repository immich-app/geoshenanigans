variable "cloudflare_account_id" {}
variable "tf_state_postgres_conn_str" {}
variable "pmtiles_deployment_key" {}

# Object-key prefix the worker reads its deployment from in the tiles-geo bucket: "" (prod's area) or "<env>/".
variable "storage_prefix" {
  type = string
}

variable "tiles_build_dir" {}
variable "env" {}

# The o11y vmauth gateway's bearer token for the env's store (tf_prod: production, tf_dev: staging), mirrored from FUTO's
# vault by core-infra-tf.
variable "o11y_vmauth_token" {
  type      = string
  sensitive = true
}

# Only for destroying the dashboards and alerts this module used to keep in the old monitoring.immich.cloud Grafana;
# remove with the provider once that apply has run in both environments.
variable "grafana_url" {}
variable "grafana_token" {}

variable "tigris_read_key_id" {}
variable "tigris_read_access_key" {}

# Bunny CDN analytics in the worker's cron (tiles/src/bunny-logs), in prod only (see workers.tf): without the pull zone
# id the log reader is skipped, without the public URL the probe is. The two secrets come from
# deployment/tiles-worker-bunny.env, which only prod applies resolve (pull requests' plans pass stand-ins): a prod plan
# or apply without them stops (workers.tf) rather than turn both off. They go to the worker as Worker secrets through
# Cloudflare's API (terraform_data.bunny_secrets), never into a resource's attributes, so no state holds them.
variable "bunny_api_key" {
  type      = string
  sensitive = true
  default   = ""
}

variable "globalping_token" {
  type      = string
  sensitive = true
  default   = ""
}

# The Bunny pull zone's id, the Bunny unit's pull_zone_id output (deployment/modules/bunny/tiles), for the log reader.
# Not a secret, and not read from that unit's state, which only its own protected jobs can decrypt: copy it into
# tiles.prod.tfvars.json once that unit has applied. Until then the log reader stays off.
variable "bunny_pull_zone_id" {
  type    = string
  default = ""

  validation {
    condition     = can(regex("^([0-9]+)?$", var.bunny_pull_zone_id))
    error_message = "bunny_pull_zone_id must be the pull zone's numeric id, or empty."
  }
}

# The Bunny-served origin the probe requests tiles from, e.g. https://tiles.immich.cloud once DNS points there (a test
# hostname before). Set it in tiles.<env>.tfvars.json. Empty turns the probe off; anything else must be a URL the
# worker takes (tiles/src/bunny-logs/config.ts), or it would get the Globalping token and skip the probe.
variable "bunny_public_url" {
  type    = string
  default = ""

  validation {
    condition     = var.bunny_public_url == "" || can(regex("^https?://[A-Za-z0-9][A-Za-z0-9.-]*(/[^?#[:space:]]*)?$", var.bunny_public_url))
    error_message = "bunny_public_url must be empty (no probe) or an absolute http(s) URL with a hostname and no port, query or fragment, e.g. https://tiles.immich.cloud."
  }
}

# Each probe run is 12 Globalping tests; every 10 minutes is 72 of the 500 free an hour. From 5 (144 an hour) to 14: the
# probe alerts' 30-minute windows need two runs at all times (o11y/alerts/geoshenanigans-tiles.yaml), and the worker
# brings any other value into the same range (tiles/src/bunny-logs/config.ts).
variable "bunny_probe_interval_minutes" {
  type    = number
  default = 10

  validation {
    condition     = floor(var.bunny_probe_interval_minutes) == var.bunny_probe_interval_minutes && var.bunny_probe_interval_minutes >= 5 && var.bunny_probe_interval_minutes <= 14
    error_message = "bunny_probe_interval_minutes must be a whole number of minutes from 5 to 14."
  }
}
