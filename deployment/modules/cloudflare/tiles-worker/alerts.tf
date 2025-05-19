resource "grafana_rule_group" "rule_group_0000" {
  org_id           = 1
  name             = "Minutely"
  folder_uid       = "tiles-immich-cloud"
  interval_seconds = 10

  rule {
    name      = "Tileserver Error Rate"
    condition = "C"

    data {
      ref_id = "A"

      relative_time_range {
        from = 300
        to   = 0
      }

      datasource_uid = "36979063-5384-4eb9-8679-565a727cbc13"
      model          = "{\"datasource\":{\"type\":\"prometheus\",\"uid\":\"36979063-5384-4eb9-8679-565a727cbc13\"},\"disableTextWrap\":false,\"editorMode\":\"builder\",\"exemplar\":false,\"expr\":\"sum(sum_over_time(tiles_tile_request_errors[1m]))\",\"fullMetaSearch\":false,\"includeNullMetadata\":true,\"instant\":true,\"interval\":\"1m\",\"intervalMs\":15000,\"legendFormat\":\"Errors\",\"maxDataPoints\":43200,\"range\":false,\"refId\":\"A\",\"useBackend\":false}"
    }
    data {
      ref_id = "C"

      relative_time_range {
        from = 300
        to   = 0
      }

      datasource_uid = "__expr__"
      model          = "{\"conditions\":[{\"evaluator\":{\"params\":[0.99,0.99],\"type\":\"gt\"},\"operator\":{\"type\":\"and\"},\"query\":{\"params\":[\"C\"]},\"reducer\":{\"params\":[],\"type\":\"last\"},\"type\":\"query\"}],\"datasource\":{\"type\":\"__expr__\",\"uid\":\"__expr__\"},\"expression\":\"A\",\"intervalMs\":1000,\"maxDataPoints\":43200,\"refId\":\"C\",\"type\":\"threshold\"}"
    }

    no_data_state  = "OK"
    exec_err_state = "Error"
    annotations = {
      __dashboardUid__ = "ddu4e1545csg0a"
      __panelId__      = "10"
      description      = "Tileserver errors >0 for 1 minute"
      summary          = "Tileserver Errors"
    }
    labels = {
      severity = "1"
    }
    is_paused = false
  }
}
