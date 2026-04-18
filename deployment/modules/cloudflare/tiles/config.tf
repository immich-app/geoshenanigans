terraform {
  required_version = "~> 1.9"

  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "4.52.5"
    }
    tigris = {
      source = "tigrisdata/tigris"
    }
    random = {
      source = "hashicorp/random"
    }
  }
}
