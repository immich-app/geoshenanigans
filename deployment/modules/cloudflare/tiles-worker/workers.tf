data "cloudflare_zone" "immich_cloud" {
  name = "immich.cloud"
}

locals {
  # Only prod reads Bunny's logs and probes: the Bunny account is production's, and a dev worker would read the same pull
  # zone's logs again and spend the Globalping quota twice. Whether a secret is set isn't itself secret.
  bunny_log_reader = var.env == "prod" && nonsensitive(var.bunny_api_key != "") && var.bunny_pull_zone_id != ""
  bunny_probe      = var.env == "prod" && nonsensitive(var.globalping_token != "") && var.bunny_public_url != ""
  # The Worker secrets terraform_data.bunny_secrets puts (below), by name.
  bunny_secret_names = concat(
    local.bunny_log_reader ? ["BUNNY_API_KEY"] : [],
    local.bunny_probe ? ["GLOBALPING_TOKEN"] : [],
  )
}

resource "cloudflare_workers_script" "tiles" {
  account_id = var.cloudflare_account_id
  name       = "tiles-${var.env}"
  content    = file("${var.tiles_build_dir}/index.js")
  module     = true

  plain_text_binding {
    name = "DEPLOYMENT_KEY"
    text = var.pmtiles_deployment_key
  }

  plain_text_binding {
    name = "ENVIRONMENT"
    text = var.env
  }

  dynamic "plain_text_binding" {
    for_each = var.storage_prefix == "" ? [] : [var.storage_prefix]
    content {
      name = "STORAGE_PREFIX"
      text = plain_text_binding.value
    }
  }

  # Metrics and logs go to FUTO's o11y stack through its vmauth gateway: prod to the production store, dev to staging.
  plain_text_binding {
    name = "O11Y_URL"
    text = var.env == "prod" ? "https://vmauth.futostatus.com" : "https://vmauth.staging.futostatus.com"
  }

  secret_text_binding {
    name = "O11Y_TOKEN"
    text = var.o11y_vmauth_token
  }

  secret_text_binding {
    name = "TIGRIS_KEY_ID"
    text = var.tigris_read_key_id
  }

  secret_text_binding {
    name = "TIGRIS_ACCESS_KEY"
    text = var.tigris_read_access_key
  }

  # Bunny CDN request logs and synthetic probe, run by the cron (tiles/src/bunny-logs). Their secrets, BUNNY_API_KEY and
  # GLOBALPING_TOKEN, are not bindings here: terraform_data.bunny_secrets puts them (below).
  dynamic "plain_text_binding" {
    for_each = local.bunny_log_reader ? [var.bunny_pull_zone_id] : []
    content {
      name = "BUNNY_PULL_ZONE_ID"
      text = plain_text_binding.value
    }
  }

  dynamic "plain_text_binding" {
    for_each = local.bunny_probe ? {
      BUNNY_PUBLIC_URL             = var.bunny_public_url
      BUNNY_PROBE_INTERVAL_MINUTES = tostring(var.bunny_probe_interval_minutes)
    } : {}
    content {
      name = plain_text_binding.key
      text = plain_text_binding.value
    }
  }

  d1_database_binding {
    database_id = var.env != "prod" ? data.terraform_remote_state.tiles_state.outputs.d1_dev_database : data.terraform_remote_state.tiles_state.outputs.d1_global_database
    name        = "D1_GLOBAL"
  }

  compatibility_date  = "2024-07-29"
  compatibility_flags = ["nodejs_compat"]

  lifecycle {
    # Prod gets these from deployment/tiles-worker-bunny.env, which .env doesn't name: a prod run that resolved .env
    # alone would otherwise turn the log reader and the probe off, after an upload that drops their secrets.
    precondition {
      condition     = var.env != "prod" || nonsensitive(var.bunny_api_key != "" && var.globalping_token != "")
      error_message = "Prod needs bunny_api_key and globalping_token: resolve deployment/tiles-worker-bunny.env too (op run --env-file=.env --env-file=tiles-worker-bunny.env)."
    }
  }
}

# The Bunny account key and the Globalping token are Worker secrets put through Cloudflare's API, not
# secret_text_bindings: OpenTofu keeps a binding's value in the state, which the backend credential every pull
# request's plan resolves can read. The values come from variables, which the state doesn't keep, and go only to this
# command's environment. Every upload drops them: provider 4.52.10 uploads only the configured bindings, and its
# refresh records the secrets it doesn't manage, without their values, so every prod plan updates the script to remove
# them. Hence this puts them after every upload, and on every apply anyway (plantimestamp), which also sends out a
# rotated value.
resource "terraform_data" "bunny_secrets" {
  count = length(local.bunny_secret_names) > 0 ? 1 : 0

  input            = { script = cloudflare_workers_script.tiles.name, secrets = local.bunny_secret_names }
  triggers_replace = [plantimestamp()]

  lifecycle {
    replace_triggered_by = [cloudflare_workers_script.tiles]
  }

  provisioner "local-exec" {
    interpreter = ["bash"]
    command     = "${path.module}/put-worker-secrets.sh"
    environment = {
      ACCOUNT_ID           = var.cloudflare_account_id
      SCRIPT               = self.input.script
      SECRETS              = join(" ", self.input.secrets)
      CLOUDFLARE_API_TOKEN = data.terraform_remote_state.api_keys_state.outputs.terraform_key_cloudflare_account
      BUNNY_API_KEY        = var.bunny_api_key
      GLOBALPING_TOKEN     = var.globalping_token
    }
  }
}

# Every minute the worker's scheduled handler sends o11y its heartbeat, which the bundle's
# geoshenanigans-tiles-heartbeat alert watches (o11y/README.md), reads a closed minute of Bunny's request logs and, every
# bunny_probe_interval_minutes, runs the synthetic probe (tiles/src/bunny-logs).
resource "cloudflare_workers_cron_trigger" "tiles" {
  account_id  = var.cloudflare_account_id
  script_name = cloudflare_workers_script.tiles.name
  schedules   = ["* * * * *"]
}

resource "cloudflare_workers_domain" "tiles" {
  account_id = var.cloudflare_account_id
  hostname   = "tiles.${var.env}.immich.cloud"
  service    = cloudflare_workers_script.tiles.id
  zone_id    = data.cloudflare_zone.immich_cloud.zone_id
}

resource "cloudflare_workers_domain" "tiles_prod" {
  count      = var.env == "prod" ? 1 : 0
  account_id = var.cloudflare_account_id
  hostname   = "tiles.immich.cloud"
  service    = cloudflare_workers_script.tiles.id
  zone_id    = data.cloudflare_zone.immich_cloud.zone_id
}
