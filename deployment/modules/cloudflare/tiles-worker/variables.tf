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
