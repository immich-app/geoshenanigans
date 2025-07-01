# This file will define the Cloudflare Worker resource.
# It assumes you are using Wrangler to bundle and prepare the script,
# and Terraform will primarily handle the deployment of the pre-built script found via var.worker_path.

data "cloudflare_account" "primary" {
  account_id = var.cloudflare_account_id
}

# Using cloudflare_worker_script might be more suitable if wrangler handles the build.
# cloudflare_worker_domain can be used if you need to attach it to a specific domain/subdomain.

# The wrangler.toml file within var.worker_path will define the script name, entry point, compatibility date, etc.
# Terraform can deploy the script based on the bundled output that Wrangler produces.
# For this setup, we'll assume Wrangler CLI is used for the actual build and upload,
# and Terraform might be more for managing the worker's existence, routes, or environment variables if not set in wrangler.toml.

# A more direct Terraform approach to deploying a script would be:
resource "cloudflare_worker_script" "metrics_collector_worker" {
  account_id = var.cloudflare_account_id
  name       = var.worker_name
  # content    = file("${var.worker_path}/dist/index.js") # Assumes wrangler builds to ./dist/index.js
  # The actual content can be sourced from the output of a wrangler build command.
  # However, wrangler deploy handles this. If you want TF to deploy the script content itself,
  # you'd need a build step before terraform apply that places the script content where TF can find it.

  # For now, this resource can manage settings around the script if wrangler deploy is the primary deployment mechanism for the code itself.
  # If wrangler.toml defines vars, they take precedence unless overridden here or via UI.

  dynamic "plain_text_variable" {
    for_each = var.plain_text_vars
    content {
      name  = plain_text_variable.key
      value = plain_text_variable.value
    }
  }

  dynamic "secret_text_variable" {
    for_each = var.secret_text_vars
    content {
      name  = secret_text_variable.key
      value = secret_text_variable.value # Ensure these are actual secrets, not plaintext
    }
  }

  # Cron triggers are defined in wrangler.toml. If you want Terraform to manage them:
  # Ensure the name in wrangler.toml is unique or this might conflict.
  # schedule {
  #   cron_triggers = var.cron_triggers
  # }

  # If you are using `wrangler deploy`, that command typically handles the script upload and settings from wrangler.toml.
  # This Terraform resource would then be for managing things not covered by wrangler.toml or for ensuring the script resource exists.
  # For a project structure where `wrangler deploy` is used from the worker's own directory (`geoshenanigans/metrics-collector/`),
  # this Terraform module might be more about setting up DNS routes, KV namespaces, or other bindings if needed,
  # rather than deploying the script content itself, to avoid conflicts with Wrangler's deployment process.

  # Given the setup, it's likely `wrangler deploy` from the `metrics-collector` directory is the intended workflow for the script's content.
  # This Terraform can ensure the script *name* is reserved or manage associated resources.
  # If you want Terraform to fully manage the script deployment (uploading content), you'd need to adjust the `content` argument
  # and potentially have a build step that outputs the final script to a known location for Terraform.

  # For now, let's assume this TF is for settings & ensuring the script (deployed by wrangler) has these settings.
  # If ACCOUNT_ID and API_TOKEN are in wrangler.toml [vars], they are set at build/deploy time by wrangler.
  # If you need TF to set them (e.g. if they are sensitive and come from a secure store for TF),
  # you would add them to secret_text_vars and ensure wrangler.toml doesn't also try to set them (or TF overrides).
  logpush            = false        # Default, can be enabled if needed
  compatibility_date = "2024-05-30" # Should match wrangler.toml or be centrally managed
}

# Example of creating a route for the worker, if it's not a scheduled-only worker
# resource "cloudflare_worker_route" "metrics_collector_route" {
#   account_id = var.cloudflare_account_id
#   zone_id    = var.cloudflare_zone_id # You'd need to add cloudflare_zone_id as a variable
#   pattern    = "example.com/metrics/*" # The pattern to trigger this worker
#   script_name = cloudflare_worker_script.metrics_collector_worker.name
# }

output "worker_script_name" {
  value = cloudflare_worker_script.metrics_collector_worker.name
}

