terraform {
  source = "../../../..//deployment/modules/cloudflare"
}

include "root" {
  path = find_in_parent_folders()
}

inputs = {
  cloudflare_account_id = get_env("TF_VAR_cloudflare_account_id", "")
  worker_name           = "metrics-collector"
  worker_path           = "${get_terragrunt_dir()}/../../../../metrics-collector" # Path to the worker source code
  # Add any other specific variables your Cloudflare module might need for workers
  # For example, if you manage secrets/vars via Terraform:
  # plain_text_vars = {
  #   "EXAMPLE_VAR" = "example_value"
  # }
  # secret_text_vars = {
  #   "API_TOKEN" = get_env("CLOUDFLARE_API_TOKEN_FOR_METRICS_WORKER", "") # Example if you were to pass it this way
  # }
  # cron_triggers = ["*/5 * * * *"] # Already in wrangler.toml, but can be managed here too if preferred
}

