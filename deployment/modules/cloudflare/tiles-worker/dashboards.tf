module "static_pages" {
  # source = "/home/zack/Source/immich/devtools/tf/shared/modules/grafana"
  source = "git::https://github.com/immich-app/devtools.git//tf/shared/modules/grafana?ref=main"

  folder_name     = "tiles.immich.cloud"
  dashboards_path = "./dashboards"
  env             = var.env
}
