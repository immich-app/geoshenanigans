resource "cloudflare_r2_bucket" "geocoder" {
  account_id = var.cloudflare_account_id
  name       = "geocoder"
  location   = "ENAM"
}

data "cloudflare_api_token_permission_groups" "all" {
  provider = cloudflare.api_keys
}

resource "cloudflare_api_token" "geocoder_r2" {
  provider = cloudflare.api_keys
  name     = "geocoder-ci-r2"

  policy {
    permission_groups = [
      data.cloudflare_api_token_permission_groups.all.account["Workers R2 Storage Read"],
      data.cloudflare_api_token_permission_groups.all.account["Workers R2 Storage Write"],
    ]
    resources = {
      "com.cloudflare.api.account.*" = "*"
    }
  }

  policy {
    permission_groups = [
      data.cloudflare_api_token_permission_groups.all.r2["Workers R2 Storage Bucket Item Read"],
      data.cloudflare_api_token_permission_groups.all.r2["Workers R2 Storage Bucket Item Write"],
    ]
    resources = {
      "com.cloudflare.edge.r2.bucket.${var.cloudflare_account_id}_default_${cloudflare_r2_bucket.geocoder.id}" = "*"
    }
  }
}

resource "github_actions_secret" "r2_bucket" {
  repository      = "geoshenanigans"
  secret_name     = "GEOCODER_R2_BUCKET"
  plaintext_value = cloudflare_r2_bucket.geocoder.name
}

resource "github_actions_secret" "r2_endpoint" {
  repository      = "geoshenanigans"
  secret_name     = "GEOCODER_R2_ENDPOINT"
  plaintext_value = "https://${var.cloudflare_account_id}.r2.cloudflarestorage.com"
}

resource "github_actions_secret" "r2_access_key_id" {
  repository      = "geoshenanigans"
  secret_name     = "GEOCODER_R2_ACCESS_KEY_ID"
  plaintext_value = cloudflare_api_token.geocoder_r2.id
}

resource "github_actions_secret" "r2_secret_access_key" {
  repository      = "geoshenanigans"
  secret_name     = "GEOCODER_R2_SECRET_ACCESS_KEY"
  plaintext_value = cloudflare_api_token.geocoder_r2.value
}
