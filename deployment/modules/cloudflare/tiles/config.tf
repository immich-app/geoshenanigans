terraform {
  backend "pg" {
    schema_name = "prod_cloudflare_tiles"
  }
  required_version = "~> 1.9"

  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "5.29.0"
    }
    tigris = {
      source = "tigrisdata/tigris"
    }
    random = {
      source  = "hashicorp/random"
      version = "3.9.1"
    }
  }
}
