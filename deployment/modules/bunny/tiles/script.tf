locals {
  # "both" is the step between the other two modes (README.md): the middleware stays linked while the edge rules that
  # replace it reach every PoP.
  gzip_middleware = var.gzip_mode != "edge_rule"
  gzip_edge_rules = var.gzip_mode != "middleware"
}

# Sets Content-Encoding: gzip on tiles as they come from storage (script/tiles-gzip.ts). It needs no variables or
# secrets. It exists in every gzip_mode and edge_rule mode only unlinks it: the provider doesn't mark the script's
# deployment key sensitive, so a plan that deletes or replaces the script prints the key (README.md).
resource "bunnynet_compute_script" "gzip" {
  type    = "middleware"
  name    = "${var.pull_zone_name}-gzip"
  content = file("${path.module}/script/tiles-gzip.ts")
}

# The provider saves a new script's code but publishes it only when the code changes later, and a script without a
# release doesn't run. So the first release is published here, once per script, before the pull zone links it.
resource "terraform_data" "publish_gzip" {
  input            = bunnynet_compute_script.gzip.id
  triggers_replace = [bunnynet_compute_script.gzip.id]

  provisioner "local-exec" {
    # bash, for -H @<(...): the account key reaches curl as a header file, not on its command line, which any process
    # on the machine can read.
    interpreter = ["bash", "-c"]
    command     = <<-EOT
      curl --silent --show-error --fail-with-body --retry 3 -X POST \
        -H @<(printf 'AccessKey: %s\n' "$BUNNY_API_KEY") -H 'Content-Type: application/json' \
        --data '{"Note":"opentofu: first release"}' "https://api.bunny.net/compute/script/${self.input}/publish"
    EOT
    environment = {
      BUNNY_API_KEY = var.bunny_api_key
    }
  }
}

output "gzip_script_id" {
  value = bunnynet_compute_script.gzip.id
}
