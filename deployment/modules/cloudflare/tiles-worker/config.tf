terraform {
  backend "pg" {}
  required_version = "~> 1.9"

  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "5.28.1"
    }
    grafana = {
      source  = "grafana/grafana"
      version = "4.51.1"
    }
  }
}
