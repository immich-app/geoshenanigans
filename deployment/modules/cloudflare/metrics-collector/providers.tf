terraform {
  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 4.0"
    }
  }
}

provider "cloudflare" {
  # Configuration is typically handled by environment variables like CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID
  # Or you can explicitly set them:
  # api_token = var.cloudflare_api_token
  # account_id = var.cloudflare_account_id
}

