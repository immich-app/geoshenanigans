terraform {
  backend "pg" {}
  required_version = "~> 1.9"

  required_providers {
    bunnynet = {
      source  = "bunnyway/bunnynet"
      version = "0.20.2"
    }
  }
}
