provider "cloudflare" {
  api_token = data.terraform_remote_state.api_keys_state.outputs.terraform_key_cloudflare_account
}

# Only for destroying the dashboards and alerts this module used to keep in the old monitoring.immich.cloud Grafana
# (they moved to the o11y bundle in o11y/); remove it, its variables and its .env entries once that apply has run in
# both environments.
provider "grafana" {
  url  = var.grafana_url
  auth = var.grafana_token
}
