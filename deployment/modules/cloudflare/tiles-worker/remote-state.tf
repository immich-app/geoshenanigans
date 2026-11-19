data "terraform_remote_state" "api_keys_state" {
  backend = "pg"

  config = {
    conn_str    = var.tf_state_postgres_conn_str
    schema_name = "prod_cloudflare_api_keys"
  }
}

data "terraform_remote_state" "tiles_state" {
  backend = "pg"

  config = {
    conn_str    = var.tf_state_postgres_conn_str
    schema_name = "prod_cloudflare_tiles"
  }
}

# The Bunny tiles zone's unit (deployment/modules/bunny/tiles), for its pull zone id: the log reader's
# BUNNY_PULL_ZONE_ID. Prod only, like the Bunny bindings (workers.tf). Until that unit has applied, its state has no
# outputs, the default applies and the log reader stays off.
data "terraform_remote_state" "bunny_tiles" {
  count   = var.env == "prod" ? 1 : 0
  backend = "pg"

  config = {
    conn_str    = var.tf_state_postgres_conn_str
    schema_name = "prod_bunny_tiles"
  }

  defaults = {
    pull_zone_id = ""
  }
}
