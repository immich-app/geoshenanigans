variable "cloudflare_account_id" {}
variable "tf_state_postgres_conn_str" {}
variable "pmtiles_deployment_key" {}

# Object-key prefix the worker reads its deployment from in the tiles-geo bucket: "" (prod's area) or "<env>/".
variable "storage_prefix" {
  type = string
}

# How long the gateway waits for the cache's answer to a tile GET before also reading the tile itself, in milliseconds;
# a negative value turns that race off.
variable "race_ms" {
  type    = number
  default = 120
}

variable "tiles_build_dir" {}
variable "vmetrics_api_token" {}
variable "env" {}

variable "grafana_url" {}
variable "grafana_token" {}

variable "tigris_read_key_id" {}
variable "tigris_read_access_key" {}
