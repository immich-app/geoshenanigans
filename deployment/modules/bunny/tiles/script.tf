locals {
  middleware = var.gzip_mode == "middleware"
}

# Sets Content-Encoding: gzip on tiles as they come from storage (script/tiles-gzip.ts). It needs no variables or
# secrets.
resource "bunnynet_compute_script" "gzip" {
  count   = local.middleware ? 1 : 0
  type    = "middleware"
  name    = "${var.pull_zone_name}-gzip"
  content = file("${path.module}/script/tiles-gzip.ts")
}

# The provider saves a new script's code but publishes it only when the code changes later, and a script without a
# release doesn't run. So the first release is published here, once per script, before the pull zone links it.
resource "terraform_data" "publish_gzip" {
  count            = local.middleware ? 1 : 0
  input            = bunnynet_compute_script.gzip[0].id
  triggers_replace = [bunnynet_compute_script.gzip[0].id]

  provisioner "local-exec" {
    command = <<-EOT
      curl --silent --show-error --fail-with-body --retry 3 -X POST \
        -H "AccessKey: $BUNNY_API_KEY" -H 'Content-Type: application/json' --data '{"Note":"opentofu: first release"}' \
        "https://api.bunny.net/compute/script/${self.input}/publish"
    EOT
    environment = {
      BUNNY_API_KEY = var.bunny_api_key
    }
  }
}

output "gzip_script_id" {
  value = local.middleware ? bunnynet_compute_script.gzip[0].id : null
}
