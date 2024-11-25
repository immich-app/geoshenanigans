terraform {
  source = "."

  extra_arguments custom_vars {
    commands = get_terraform_commands_that_need_vars()
  }
}

include "root" {
  path = find_in_parent_folders("root.hcl")
}

locals {
  tiles_build_dir    = get_env("TF_VAR_tiles_build_dir")
  vmetrics_api_token = get_env("TF_VAR_vmetrics_api_token")
}

inputs = {
  tiles_build_dir    = local.tiles_build_dir
  vmetrics_api_token = local.vmetrics_api_token
}
