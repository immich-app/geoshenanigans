variable "cloudflare_account_id" {
  type        = string
  description = "Cloudflare Account ID"
}

variable "worker_name" {
  type        = string
  description = "Name of the Cloudflare worker script"
  default     = "metrics-collector"
}

variable "worker_path" {
  type        = string
  description = "Path to the worker script directory (where wrangler.toml and src/ are located)"
}

variable "plain_text_vars" {
  type        = map(string)
  description = "Plain text environment variables for the Worker."
  default     = {}
}

variable "secret_text_vars" {
  type        = map(string)
  description = "Secret environment variables for the Worker."
  default     = {}
}

variable "cron_triggers" {
  type        = list(string)
  description = "A list of cron expressions to trigger the worker."
  default     = ["*/5 * * * *"] # Matches wrangler.toml, can be overridden
}

# Add other variables as needed, e.g., for KV namespaces, R2 buckets, D1 databases if the worker needs them

