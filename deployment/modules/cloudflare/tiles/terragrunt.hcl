terraform {
  source = "."

  extra_arguments custom_vars {
    commands = get_terraform_commands_that_need_vars()
    arguments = [
      "-var-file=tiles.tfvars.json",
    ]
  }
}

include "cloudflare" {
  path = find_in_parent_folders("cloudflare.hcl")
}

include "root" {
  path = find_in_parent_folders("root.hcl")
}

locals {
  tiles_build_dir    = get_env("TILES_BUILD_DIR")
  vmetrics_api_token = get_env("VMETRICS_API_TOKEN")
}

inputs = {
  tiles_build_dir    = local.tiles_build_dir
  vmetrics_api_token = local.vmetrics_api_token
}
