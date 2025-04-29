provider "cloudflare" {
  api_token = data.terraform_remote_state.api_keys_state.outputs.terraform_key_cloudflare_account
}

provider "grafana" {
  url  = var.grafana_url
  auth = var.grafana_token
}
