terraform {
  source = "."

  extra_arguments custom_vars {
    commands = get_terraform_commands_that_need_vars()
    arguments = [
      "-var-file=tiles.${local.env}.tfvars.json",
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

# The Bunny account is production only and there is no dev zone, so this unit has its own workflow
# (.github/workflows/tiles-bunny-infra.yml) and secrets (bunny.env). The `run --all` over deployment/ that the tiles
# workflows and `mise run tf` do runs per Cloudflare environment, so it leaves this unit out: run it from this directory.
exclude {
  if      = true
  actions = ["all"]
}

locals {
  env = get_env("TF_VAR_env", "prod")
}

remote_state {
  backend = "pg"

  config = {
    conn_str    = get_env("TF_VAR_tf_state_postgres_conn_str")
    schema_name = "${local.env}_bunny_tiles"
  }
}
