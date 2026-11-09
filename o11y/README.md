# o11y bundle

The Grafana dashboards and alert rules of the geocoder nightly and of the tiles worker (tiles.immich.cloud), shipped to FUTO's o11y stack ([immich-app/yucca-o11y](https://github.com/immich-app/yucca-o11y)) as one signed OCI artifact, `ghcr.io/immich-app/geoshenanigans/o11y-manifests:main`. This is Model A in yucca-o11y's `docs/06-dashboards-and-alerts-guide.md`.

| Source                  | Becomes                                                        |
| ----------------------- | -------------------------------------------------------------- |
| (generated)             | `GrafanaFolder` `geoshenanigans`, the project's folder         |
| `dashboards/<uid>.json` | a `GrafanaDashboard` with the JSON embedded as `spec.gzipJson` |
| `alerts/*.yaml`         | `GrafanaAlertRuleGroup`s, shipped as-is                        |

`render-manifests.sh` builds the bundle into `build/o11y` and rejects anything o11y would mis-apply. `.github/workflows/o11y.yml` runs it on every PR that touches this directory, and on `main` pushes the result with `flux push artifact` and signs it keyless with cosign.

```bash
# renders build/o11y, checks it assembles under kustomize and validates it with kubeconform
MISE_ENV=o11y mise run o11y:render
```

The tools (jq, yq, kustomize, kubeconform, flux2, cosign) and the `o11y:render` task are in `.mise/config.o11y.toml`, locked in `.mise/mise.o11y.lock`. After changing a version there, re-lock with `MISE_ENV=o11y mise lock`, using the mise version `o11y.yml` pins (or bump that pin in the same change): a lock written by a newer mise fails CI's `--locked` install.

## Where the data comes from

`geocoder-build.yml`'s report step pushes what only its runner has once per run, whatever the outcome: the build phases, peak memory and data age (`geocoder_build_*`), and the `geocoder_patch_*` and `geocoder_index_bytes` series. `geocoder-build-watchdog.yml`'s report job, every 2 hours on a GitHub-hosted runner, pushes the published `latest.json` timestamps with its heartbeat (none when it can't read `latest.json`), and the latest finished nightly's outcome, duration and steps from the GitHub API, stamped with the time that run finished, so a run whose runner died still reports and each run keeps one point. Both push straight to o11y's public gateway, `https://vmauth.futostatus.com`, as fmeet's workers do, with a bearer token each report step reads from 1Password at runtime. They stamp `project=geoshenanigans`, `cluster=geocoder`, `env=prod`, `provider=github` and `region=global` on every sample, which o11y stores in the existing immich tenant (`6:1`, beside pokedex). `cluster` names the pipeline rather than a machine, as fmeet's `cluster=serverless` does, and is the same for both pushers so everything lands in that one tenant.

The report steps push their log lines to VictoriaLogs through the same gateway, with the same five identity fields plus `app` (`geocoder-build` or `geocoder-watchdog`), `step`, `run_id`, `run_attempt`, `build_date` and `level`. `project:="geoshenanigans" app:="geocoder-build"` selects the nightly's; add `run_id:="<id>"` for one run.

### The tiles worker

The tiles worker (`tiles/`) ships from every request, through the same gateway with the same token, which Terraform binds as `O11Y_TOKEN` (`op://tf_<env>/O11Y_VICTORIAMETRICS_VMAUTH_PASSWORD`, mirrored from FUTO's vault by core-infra-tf) next to `O11Y_URL`: prod ships to the production store, dev to staging. Every series and log line carries `project=geoshenanigans`, `cluster=tiles`, `env`, `provider=cloudflare` and `region=world`, which o11y stores in tenant `6:3`.

A Worker can't keep a counter between requests, so each request sends one sample per measurement (1 for an invocation, the milliseconds for a duration, and a 1 in each duration's VictoriaMetrics histogram bucket) as InfluxDB lines to o11y's serverless ingest path, `/serverless/insert/influx/write`. The store keeps one sample per series per 20s and would collapse them; the path's aggregator writes each series' sum per minute instead, `<measurement>_<field>:1m_sum_samples`, and `<measurement>_duration_bucket:1m_sum_samples{vmrange=...}` for the distributions (yucca-o11y `docs/05`, "Shippers that send one sample per event"). Add the minutes up with `sum_over_time()` and over `aggregator`, the replica that summed them:

```promql
sum by (status) (sum_over_time(tiles_response_count:1m_sum_samples{project="geoshenanigans", cluster="tiles"}[5m]))
histogram_quantile(0.95, sum by (vmrange) (sum_over_time(tiles_handle_request_duration_bucket:1m_sum_samples{project="geoshenanigans", cluster="tiles"}[5m])))
```

Every series is `tiles_*`. `tiles/src/o11y.ts` keeps only tags with a handful of values (continent, request type, status, zoom, D1 region...), limits the ones taken from the URL (zoom, API version, style) to what the worker serves, and keeps the colo only on `tiles_handle_request`: every combination is a series written every minute while it has requests, and a duration's once per bucket. The client's site and network go in the logs instead.

The serverless path writes only while requests arrive, and o11y's fleet-wide stopped-reporting rule leaves the tiles tenant out, so the worker also sends a heartbeat: a cron trigger (`cloudflare_workers_cron_trigger.tiles`) runs its scheduled handler every minute, which pushes `tiles_heartbeat_timestamp_seconds` (with the `deployment_key` it serves) straight to `/insert/0/influx/write`, a gauge at one sample a minute. `geoshenanigans-tiles-heartbeat` fires after 10 minutes without it, and `geoshenanigans-tiles-silent` (no requests counted for 15 minutes) only while it arrives, so a broken metrics path pages once, as the heartbeat.

The worker's error lines and a 1% sample of its requests (`kind:="request"`, with `apex_domain`, `as_org`, `country`, `status` and `duration_ms`) go to VictoriaLogs. Each line carries the request's `ray` (its Cf-Ray), `path`, `colo` and `continent`. `project:="geoshenanigans" cluster:="tiles" level:="error"` selects the failures. `REQUEST_LOG_SAMPLE_RATE` changes the sample.

## Dashboards

Each `dashboards/<uid>.json` file is a Grafana export whose `uid` matches the file name, with `"id": null` and tags `app`, `metrics`, `logs` (if it has log panels) and `geoshenanigans`. Metric queries use the `$datasource` variable, limited to and defaulting to `VictoriaMetrics Fleet`, and every selector carries `project="geoshenanigans"`. Log panels use the `$logs` variable, defaulting to `VictoriaLogs`.

The nightly writes one sample per series per run, so panels read the last sample in a window: `last_over_time(...[$__interval])` with a 1h minimum interval in time series, one point per run, and `last_over_time(...[$__range])` in stats. Stats about the latest run keep only the series with the newest `geocoder_build_last_run_timestamp_seconds`, because a watchdog retry reports under its own `trigger`.

`geoshenanigans-tiles` adds up the per-minute sums per panel interval (1m minimum): `sum_over_time(...[$__interval])`, divided by `($__interval_ms / 1000)` for a rate, and `[15m]` or `[$__range]` in stats and tables.

To change a dashboard, edit it in Grafana, export it with "Export for sharing externally" off, keep the `uid`, set `"id": null` and save it over the file.

## Alerts

Each `alerts/*.yaml` file holds one `GrafanaAlertRuleGroup` with `folderRef: geoshenanigans`: `geoshenanigans.yaml` the nightly's, `geoshenanigans-tiles.yaml` the tiles worker's. Every rule is labelled `project: geoshenanigans` and with a `severity`: o11y routes `project=geoshenanigans` to the `rootly-geoshenanigans` contact point, and Rootly sets urgency from the severity (`critical` is High, `warning` is Medium). Every query uses `datasourceUid: VictoriaMetricsFleet`. Never set `heartbeat: rootly`, which is o11y's own dead man's switch route.

The threshold stage fires on a value above 0, so a condition on a zero is written `== bool 0`, which turns a match into 1; a bare `== 0` returns 0 and never fires. The rules' comments say why each window and `for` is what it is.

o11y's Flux substitutes `${VAR}` in everything it applies, and an unknown name becomes an empty string, so alerts must not contain `${`. Dashboards are safe because their JSON is embedded gzipped.

## Distribution

The artifact is a single `application/vnd.cncf.flux.content.v1.tar+gzip` layer with no `kustomization.yaml`; kustomize-controller generates one. The CRs carry no namespace, and o11y's Kustomization sets `targetNamespace: o11y`. It is signed keyless by this workflow's GitHub OIDC identity, which o11y's `OCIRepository` pins. To check a published artifact:

```bash
cosign verify ghcr.io/immich-app/geoshenanigans/o11y-manifests:main \
  --certificate-identity-regexp '^https://github\.com/immich-app/geoshenanigans/\.github/workflows/o11y\.yml@refs/heads/main$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

The GHCR package must be public, because o11y pulls it anonymously. After the first publish, check that an anonymous pull works:

```bash
tok=$(curl -s "https://ghcr.io/token?scope=repository:immich-app/geoshenanigans/o11y-manifests:pull" | jq -r .token)
curl -s -H "Authorization: Bearer $tok" https://ghcr.io/v2/immich-app/geoshenanigans/o11y-manifests/tags/list
```

If it is denied, an org admin sets the package to public in its package settings.

The consumer side is `kubernetes/apps/base/tenants/geoshenanigans/bundle.yaml` in yucca-o11y, loaded by the production o11y overlay only: staging never receives these series, so the absence alerts would fire there for good.
