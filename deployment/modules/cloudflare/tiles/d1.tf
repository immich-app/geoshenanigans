locals {
  d1_regions = ["WNAM", "ENAM", "WEUR", "EEUR", "APAC", "OC"]
}

resource "cloudflare_d1_database" "regional_tiles" {
  for_each   = { for region in local.d1_regions : region => region }
  account_id = var.cloudflare_account_id
  name       = "tiles-${lower(each.value)}"
  lifecycle {
    prevent_destroy = true
  }
}

resource "cloudflare_d1_database" "tiles" {
  account_id = var.cloudflare_account_id
  name       = "tiles"
  lifecycle {
    prevent_destroy = true
  }
}

output "d1_regional_databases" {
  value = { for region, database in cloudflare_d1_database.regional_tiles : region => database.id }
}

import {
  to = cloudflare_d1_database.regional_tiles["WNAM"]
  id = "${var.cloudflare_account_id}/837aa5e4-f404-4613-b30c-34f3b8bca150"
}

import {
  to = cloudflare_d1_database.regional_tiles["ENAM"]
  id = "${var.cloudflare_account_id}/f63d8430-3e5b-42bd-a6a6-45fd3f552a9a"
}

import {
  to = cloudflare_d1_database.regional_tiles["WEUR"]
  id = "${var.cloudflare_account_id}/ce5b6777-c533-4ff1-91a4-d6b5e94293da"
}

import {
  to = cloudflare_d1_database.regional_tiles["EEUR"]
  id = "${var.cloudflare_account_id}/538b5ef8-9a73-4bd8-a088-3a43c992cfd0"
}

import {
  to = cloudflare_d1_database.regional_tiles["APAC"]
  id = "${var.cloudflare_account_id}/d8501ddc-adea-48b4-a577-074640b56493"
}

import {
  to = cloudflare_d1_database.regional_tiles["OC"]
  id = "${var.cloudflare_account_id}/403f37a6-ebe8-4d05-9ffb-088aa73fa014"
}
