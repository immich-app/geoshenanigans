terraform {
  backend "pg" {}
  required_version = "~> 1.9"

  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "4.52.0"
    }
    grafana = {
      source  = "grafana/grafana"
      version = "3.25.8"
    }
  }
}
