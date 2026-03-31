terraform {
  backend "pg" {}
  required_version = "~> 1.9"

  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "4.52.5"
    }
    github = {
      source  = "integrations/github"
      version = "~> 6.0"
    }
  }
}
