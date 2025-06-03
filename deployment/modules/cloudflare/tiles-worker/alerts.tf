resource "grafana_rule_group" "rule_group_0000" {
  org_id             = 1
  name               = "tiles.immich.cloud Minutely Alerts"
  folder_uid         = "tiles-immich-cloud"
  interval_seconds   = 60
  disable_provenance = true

  rule {
    name      = "Tiles Sustained Low Error Rate"
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
    exec_err_state = "OK"
    for            = "5m"
    annotations = {
      __dashboardUid__ = "ddu4e1545csg0a"
      __panelId__      = "10"
      description      = "Tileserver errors >0 for 5 minutes"
      summary          = "Tiles Sustained Low Error Rate"
    }
    labels = {
      severity = "1"
    }
    is_paused = false
  }
  rule {
    name      = "Request Count / Origin"
    condition = "C"

    data {
      ref_id = "A"

      relative_time_range {
        from = 300
        to   = 0
      }

      datasource_uid = "36979063-5384-4eb9-8679-565a727cbc13"
      model          = "{\"datasource\":{\"type\":\"prometheus\",\"uid\":\"36979063-5384-4eb9-8679-565a727cbc13\"},\"disableTextWrap\":false,\"editorMode\":\"code\",\"expr\":\"((sum by(apexDomain) (count_over_time(tiles_tile_request_invocation{protocol='https:'}[1h])) or 0) + (sum by(apexDomain) (count_over_time(tiles_cdn_hit_count{request_type=\\\"tile\\\", protocol='https:'}[1h])))) / 60 / 60 \\u003e 0.3\",\"fullMetaSearch\":false,\"includeNullMetadata\":true,\"instant\":false,\"interval\":\"1h\",\"intervalMs\":60000,\"legendFormat\":\"{{origin}}\",\"maxDataPoints\":43200,\"range\":true,\"refId\":\"A\",\"useBackend\":false}"
    }
    data {
      ref_id = "C"

      relative_time_range {
        from = 300
        to   = 0
      }

      datasource_uid = "__expr__"
      model          = "{\"conditions\":[{\"evaluator\":{\"params\":[1,0],\"type\":\"gt\"},\"operator\":{\"type\":\"and\"},\"query\":{\"params\":[]},\"reducer\":{\"params\":[],\"type\":\"avg\"},\"type\":\"query\"}],\"datasource\":{\"name\":\"Expression\",\"type\":\"__expr__\",\"uid\":\"__expr__\"},\"expression\":\"B\",\"intervalMs\":1000,\"maxDataPoints\":43200,\"refId\":\"C\",\"type\":\"threshold\"}"
    }
    data {
      ref_id = "B"

      relative_time_range {
        from = 300
        to   = 0
      }

      datasource_uid = "__expr__"
      model          = "{\"conditions\":[{\"evaluator\":{\"params\":[0,0],\"type\":\"gt\"},\"operator\":{\"type\":\"and\"},\"query\":{\"params\":[]},\"reducer\":{\"params\":[],\"type\":\"avg\"},\"type\":\"query\"}],\"datasource\":{\"name\":\"Expression\",\"type\":\"__expr__\",\"uid\":\"__expr__\"},\"expression\":\"A\",\"intervalMs\":1000,\"maxDataPoints\":43200,\"reducer\":\"last\",\"refId\":\"B\",\"type\":\"reduce\"}"
    }

    no_data_state  = "OK"
    exec_err_state = "OK"
    for            = "1m"
    annotations = {
      __dashboardUid__ = "ddu4e1545csg0a"
      __panelId__      = "12"
      description      = "An Apex domain has been detected performing an unusually high amount of RPS to the tile server"
      summary          = "Tiles Apex Domain with High RPS"
    }
    labels = {
      severity = "3"
    }
    is_paused = false
  }
  rule {
    name      = "Tiles High Error Rate"
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
      model          = "{\"conditions\":[{\"evaluator\":{\"params\":[100,0.99],\"type\":\"gt\"},\"operator\":{\"type\":\"and\"},\"query\":{\"params\":[\"C\"]},\"reducer\":{\"params\":[],\"type\":\"last\"},\"type\":\"query\"}],\"datasource\":{\"type\":\"__expr__\",\"uid\":\"__expr__\"},\"expression\":\"A\",\"intervalMs\":1000,\"maxDataPoints\":43200,\"refId\":\"C\",\"type\":\"threshold\"}"
    }

    no_data_state  = "OK"
    exec_err_state = "OK"
    for            = "1m"
    annotations = {
      __dashboardUid__ = "ddu4e1545csg0a"
      __panelId__      = "10"
      description      = "Tileserver errors >100 for 1 minute"
      summary          = "Tileserver High Error Rate"
    }
    labels = {
      severity = "1"
    }
    is_paused = false
  }
}
