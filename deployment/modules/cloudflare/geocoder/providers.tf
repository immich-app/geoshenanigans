provider "cloudflare" {
  api_token = data.terraform_remote_state.api_keys_state.outputs.terraform_key_cloudflare_account
}

provider "cloudflare" {
  alias     = "api_keys"
  api_token = var.cloudflare_api_token
}

provider "github" {
  app_auth {
    id              = var.github_app_id
    installation_id = var.github_app_installation_id
    pem_file        = var.github_app_pem_file
  }
  owner = var.github_owner
}
