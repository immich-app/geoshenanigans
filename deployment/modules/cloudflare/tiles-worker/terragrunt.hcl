terraform {
  source = "."

  extra_arguments custom_vars {
    commands = get_terraform_commands_that_need_vars()
    arguments = [
      "-var-file=tiles.tfvars.json",
    ]
  }

  extra_arguments "retry_lock" {
    commands  = get_terraform_commands_that_need_locking()
    arguments = ["-lock-timeout=1m"]
  }
}

include "root" {
  path = find_in_parent_folders("root.hcl")
}

locals {
  env = get_env("TF_VAR_env")
}

remote_state {
  backend = "pg"

  config = {
    conn_str    = get_env("TF_VAR_tf_state_postgres_conn_str")
    schema_name = "${local.env}_cloudflare_tiles_worker"
  }
}

dependencies {
  paths = ["../tiles"]
}
