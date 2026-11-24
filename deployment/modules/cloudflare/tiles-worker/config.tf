terraform {
  backend "pg" {}
  required_version = "~> 1.9"

  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "5.29.0"
    }
    grafana = {
      source  = "grafana/grafana"
      version = "4.55.0"
    }
  }
}
