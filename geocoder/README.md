# Reverse Geocoder

A fast reverse geocoder built from OpenStreetMap data. Given latitude and longitude coordinates, it returns the nearest street address including house number, street name, city, state, county, postcode, and country, resolved the way Nominatim resolves them (see [NOMINATIM_REFERENCE.md](NOMINATIM_REFERENCE.md)), and the notable places nearby.

Based on the open source [Traccar Geocoder](https://github.com/traccar/traccar-geocoder).

## Features

- Street-level reverse geocoding from OSM data
- Address point, street name, and address interpolation lookup (TIGER ranges in the US)
- Administrative boundary resolution (country, state, county, city, postcode)
- Points of interest in three tiers ([POI_OVERVIEW.md](POI_OVERVIEW.md))
- Bodies of water (seas, oceans, lakes, rivers, canals, bays, straits, wetlands, glaciers) in a `water` field beside the address, from an optional water dir
- Sub-millisecond query latency with memory-mapped index files
- Nightly indexes with daily patches ([PATCHING.md](PATCHING.md))
- Automatic HTTPS with Let's Encrypt

## Published Indexes

The nightly build (`.github/workflows/geocoder-build.yml`) builds the planet
and eight continents from the day's OSM data and publishes every index
variant, with a patch from the previous day, to Tigris:
`https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds/latest.json`.
[PATCHING.md](PATCHING.md) describes the layout, the variants (mode, boundary
quality, POI tier, water) and how a client downloads and patches them.

## Monitoring

The nightly reports every run on `main` to FUTO o11y (project
`geoshenanigans`), whatever its outcome; test dispatches, from other branches
or under a scratch `s3_prefix` (whose run title ends `[scratch: <prefix>]`),
don't report, and the watchdog passes them over. Its report,
`builder/tools/o11y_report.py nightly`, starts after Finalize whatever the
steps before it did (a failed or skipped Setup or upload included), runs
beside the cleanups and artifact uploads, and is waited for by the job's last
step. It pushes what only its runner has: each build phase's duration, peak
memory, the patch verify counts, each patch's size, the index size, the
data's age and the bytes of planet/full's address footprint stream beside the
limit they may not pass, and logs build.log, the memwatch samples and each
variant's verify result. Every 2 hours the watchdog's `report` job
(`o11y_report.py watchdog`, on a GitHub-hosted runner) pushes the dates in
the published `latest.json` with a heartbeat (none when it can't read
`latest.json`), and the latest finished nightly's outcome, duration and step
durations from the GitHub API, logging that run's steps once. A run whose
runner died or that timed out never reaches its own report, so its outcome
comes from the watchdog.

Both push straight to o11y's public gateway (`O11Y_URL` in the workflows,
`https://vmauth.futostatus.com`), metrics to VictoriaMetrics and logs to
VictoriaLogs, with every sample and log line labelled
`project=geoshenanigans`, `cluster=geocoder`, `env=prod`, `provider=github`,
`region=global`. The gateway's bearer token is read from 1Password
(`op://tf_prod/O11Y_VICTORIAMETRICS_VMAUTH_PASSWORD/password`, with
the `OP_TF_PROD_ENV` service account) inside the reporting step, which hands
it to `o11y_report.py` as `$O11Y_TOKEN`; without one, nothing is sent.

- Dashboard: `geoshenanigans-geocoder-nightly` in the Grafana folder
  `geoshenanigans`.
- Alerts, in the same folder, go to the Rootly project `geoshenanigans`:
  critical when a run fails, a patch fails verify or nothing has been
  published for 30 hours; warning when a run takes over an hour, the
  planet/full patch passes 64 MiB, the data is over two days old or the
  watchdog goes silent.
- Logs, in VictoriaLogs (add `run_id:="<run id>"` for one run, or use
  `app:="geocoder-watchdog"` for the watchdog's):
  `project:="geoshenanigans" app:="geocoder-build"`

`o11y_report.py nightly --dry-run` (or `watchdog --dry-run`) prints the
metrics and log lines instead of sending them.

## Quick Start

Serve the published indexes:

```bash
cargo build --release --manifest-path server/Cargo.toml
./server/target/release/query-server data
```

The server downloads a continent from the latest build the first time a query
lands in it: `admin` mode, `simplified` boundaries, no POIs and no water unless an
admin sets another selection with `PUT /admin/configuration` (a JSON body of
`base_url`, `mode`, `quality`, `poi_tier`, `water` and the regions to `preload`;
a body without `water` takes `none`). A PUT downloads each loaded continent
again for its selection, with the latest build of its `base_url`, unless the
`.build_date` (below) its dir held as its files loaded names that bucket and
selection (its own download's, or that of another server's download into the
dir after it, on the same data dir), and loads it once that is in. Until then,
or if the download or load fails, the running process keeps serving the old
index and retries the switch on the next PUT, of whichever selection. If the
failed download removed `.build_date`, a restart leaves that continent
unloaded until a download succeeds. A PUT writes its configuration to
`configuration.json` before its downloads run: a server that starts with one
downloads, as that PUT would, its preloads and each loaded continent whose
`.build_date` names another bucket or selection. A loaded continent whose dir
holds neither the downloader's ledger nor its stamp (`.file_sha256`,
`.build_date`, below) was laid out by other means, by hand or by the test
portal, and is never downloaded again. A value the build doesn't offer fails
each download, naming those it does, and the PUT is refused with them (400)
when the server has the build's manifest at hand. Downloads check the bucket's
certificate against the system's CA store: on Linux and the other Unix systems
but macOS, its certificate bundle (`ca-certificates` on Debian), or another
that `SSL_CERT_FILE` or `SSL_CERT_DIR` names; on macOS and Windows, the
system's trust store, which those variables don't change. On a host without one
the server starts and serves what its data dir holds, and each download fails,
naming why. The files of a component with a `dir`
(the water values') go in that sub-dir of the continent dir, `water/`, which the
server loads with it; a `dir` that is not one plain name, or that starts with a
dot as the downloader's own names do, fails the download before it fetches. A
continent dir keeps a ledger of what each of its files holds, `.file_sha256`
(a line `<name> <sha256>` each, `-` while unknown, a water file's name
`water/<name>`), and a download skips a file only where the ledger
names the build's sha256 for it. It removes the files its selection doesn't
take (another selection's or build's) that the ledger names, or whose names
files of the build have in the dir or in the sub-dir their component names, the
ledger naming them or not; files of other names stay. A `water/` that is a link
or a file fails the download before it writes dataset files: it writes nothing
through a link, as the dir's lock guards only the continent dir. A fetch writes
its file in `.fetching/` beside its place (the continent dir's, or `water/`'s),
a dir of the downloader's own, which holds a `.downloader` file of the
downloader's own text from when it is made, and renames it into place; a file
at the name of one of the build's with `.tmp` after it
(`water/admin_polygons.bin.tmp`, an operator's copy, say) is none of a
download's, and stays. The dir names its build, the bucket (`base_url`)
and the selection in `.build_date` once a whole selection of it is in and those
are gone (a download with nothing to fetch into a dir stamped otherwise, or not
at all, stamps it, and removes the `.fetching/` dirs of the downloader's
there); a download that fetches or removes anything fails where a `.fetching`
is a file, a link, or a dir holding files but no `.downloader` of the
downloader's own text (an operator's, say; a `.downloader` holding only the
start of that text, as a download killed while
it wrote it leaves, is the downloader's only where it is all the dir holds),
before it changes the ledger or the stamp; else it removes the stamp first,
once it has made the ledger where the dir has none (a dir holding neither was
laid out by hand, and a server loads it), then removes each `.fetching/`, with
what killed fetches left there, its `.downloader` last (so that a kill part way
leaves the dir the downloader's), and again once its last fetch is in. Before
it reads the dir a download finishes what a transport decode killed part way
left there and in `water/`, which the water dirs' units decode into
(PATCHING.md): such a decode would otherwise put back
the files it replaced once the download had listed the files to remove; one it
can't finish fails the download.
Downloads into a dir go one at a time, those of other servers on the data dir
too: each holds a lock on the dir's `.download.lock` to its end, and runs to
its end though the query that started it gave up. A server checks and
loads a dir, when it starts and once its own download into the dir is in,
under a shared lock on that file, which it makes where it isn't there, so
that it waits for a download into the dir (another server's) to end and loads
what it left, and the first download into a dir waits for it; after its own
download it waits on a thread of its own, and answers queries meanwhile. A
dir it can't make the file in (read-only, or not writable to it) loads
without a lock. A download changes the dir's `.build_date` or `.file_sha256`
before any other file, and writes them by appending or by renaming a new file
into place: a server notes both (device, inode, size and modification time)
before it checks a dir and again once it loaded it, and where they differ, a
download ran meanwhile, it drops what it loaded and says so, the continent
left unloaded. A server that finds a ledger but no stamp in a dir leaves that
continent unloaded until a query or a preload finishes its download. Queries
need an API key from the dashboard (see Authentication below).

Servers on Unix can share a data dir; elsewhere a download takes no lock on
the dirs it writes, so a data dir is one server's. Those that run as one user
are safe side by side: their downloads into a dir go one at a time and their
loads wait for them, by the lock on `.download.lock`. A server that can't write
the data dir
(a read-only mount, or a dir of another user) is safe beside one that
downloads into it: it takes the lock where `.download.lock` is there, else
loads the dir without it and drops what it loaded where a download ran
meanwhile, by the check of `.build_date` and `.file_sha256` above. Servers of
several users can each download into the data dir only where each can write
every file and dir the others' downloads make in it: the ledger, the temp
files of a fetch, of the ledger and of the stamp, and the dir of a decode
killed part way, which the next download finishes. Give the servers a group of
their own and, before their first download, make the data dir that group's
with group write and the setgid bit (`chgrp`, then `chmod g+ws`), so that what
is made in it is that group's too; and run each server with a umask of 002.
Under a umask of 022 a server's download into a dir whose ledger another
server made fails, naming the file. A download that can't write
`.download.lock` (one a server of umask 022 made) takes its lock on it open to
read, which a local file system allows; on NFS an exclusive lock needs the
file open to write, so there it fails, saying so.

The test portal is a map page for checking answers, served by every query
server at `/test`, where it asks that server. Its compose file runs
`query-portal`, a binary of the search crate whose sources are the server
crate's, which serves every dataset of
the latest published planet build: each selection of a mode, a boundary
precision and a POI tier the manifest's constraints allow (28), without
water, whose files it doesn't fetch. Its page
compares up to 4 of them side by side, each a preset or a pick by axis, and
its URL holds the columns, the point, the build (`b=`, its publication
ID: `<date>-<the first 12 hex digits of its manifest's sha256>`, the same on
every volume), which it flags when another build is served, one published
again under its date among them, and the search box's text (`q=`); a `b=`
of a date alone stands for any build of its date. To run it:

```bash
cd test-portal && docker compose up --build --remove-orphans
# open http://localhost:3002/test
# or http://localhost:3002/test?d=with-addresses&d=smallest&at=48.8584,2.2945
# or http://localhost:3002/test?d=with-addresses&d=smallest&q=eiffel
```

Its volume, `geocoder-portal-data`, is not the old services' volumes: at its
first start the portal makes the test account in `geocoder.json` on it. To
keep the accounts of the old `geocoder-data-full` volume, copy that volume's
`index/geocoder.json` to `geocoder.json` on the new one once, before the
first start.

Its `/reverse` and `/polygons` take `dataset=`: a preset's id (`smallest`)
or `<mode>.<quality>.<poi_tier>` (`admin.simplified.major`), and
`with-addresses` without one. They answer what a query-server serving that
selection's files answers, and name the dataset and build that answered in
`X-Geocoder-Dataset`, `X-Geocoder-Build` (its date), `X-Geocoder-Build-Id`
(its publication ID, as `/health` and `/datasets` name it) and
`X-Geocoder-Build-Dir` (the dir the volume holds it in, `<date>-<n>`, the
volume's alone).
`/datasets` lists the datasets with their labels and sizes, and the
combinations the constraints exclude with their messages; `/health` gives
the build served and how its sync goes. The page asks `/datasets` anew
once `/health` names another publication ID, and cancels a polygon
request once a newer one or a clear supersedes it.
Its `/search` takes `dataset=` too, `q=` (what to search for, 1 to 256
bytes but the spaces at its ends, given to the search as it is), `limit=`
(1 to 50, 20 without one), `key=` and `debug=1`. It answers what the node
binding's `searchPlaces` answers from that selection's files, byte for byte:
a JSON array of `{name, latitude, longitude, admin1name, admin2name, country,
country_code, type}`, best first; with `debug=1` the line `place-search
query` prints instead (the query, `us` the time it took, what it read, and
each result with how it was found). It names the dataset and build as the
lookups do, and the index it searched in `X-Geocoder-Search-Index`.
Datasets whose dirs hold the files an index is made of alike share one, a
group named by its first dataset (12 for the 28: `admin`, `full` and
`no-addresses` of a precision and a POI tier share one); `/datasets` names
each dataset's (`search.index`, and `search.shipped` where the build
publishes it), and `/health`'s `search` tells each group's datasets, its
state (`queued`, `building`, `ready` or `failed` with its `reason`) and
`since` when, and once ready whether it was `built` or found built from its
files (`current`), and for an index the build publishes how it compares
with the published files (`shipped`: `same`, the files that `differ` and
those `unpublished`). Until a dataset's index is ready its `/search` answers
503 with JSON `{status: "building", dataset, index, since}` and
`Retry-After: 5`, or `{status: "failed", dataset, index, since, reason}`.
Searches run 4 at once beside the 8 lookups, so a few `/polygons` of
exact boundaries can't hold one up, and count against the key's rate as
`/reverse` does.

The page's search box searches each column's dataset as one types, once
the text has stood 250 ms (Enter searches at once), 10 results a column. A
newer search cancels the requests of the one before. The results go into
one matrix: a row a result, its rank in each column (`–` where a column
that answered lacks it, `·` where one has no answer yet), its name, kind,
type and context (admin2 · admin1 · country). Results of two columns are
one row where their names are equal, both or neither is a POI, and their
points are at most 1 km apart, 20 km where one is a boundary without a
label node, whose point moves with the boundary's precision. A row marks the columns that lack it, a rank
below its best, how far its points are apart, another type, and each part
of the context the columns disagree on. Each row is a dot on the map,
coloured by the columns that have it, and a click on a row or a dot looks
up its point in every column, as a click on the map does. A column whose
index is queued or building says so, with how many of the build's indexes
are ready, and is asked again after the `Retry-After`; one whose index
failed says why. A server without `/datasets`, or whose `/datasets` names
no indexes, shows no search.

A column can also show what a photo at the point gets in Immich today:
"Immich today (GeoNames cities500)", `dataset=immich-cities500`, which
`/datasets` lists under `others` (`kind: "immich"`, with the release and
the date of its geodata). The portal's image copies the geodata of
`ghcr.io/immich-app/immich-server:release` as it is when the image is built
(its `/build/geodata`: GeoNames' `cities500.txt`, `admin1CodesASCII.txt`,
`admin2Codes.txt` and `countryInfo.txt`, Natural Earth's
`ne_10m_admin_0_countries.geojson`, `geodata-date.txt`) and the release's
tag and commit, and `/reverse` answers as that release's
`MapRepository.reverseGeocode` does (`server/src/portal/immich.rs`): the
cities500 place, but the PPLX outside Australia and the PPLH, nearest by
`earth_distance` among those in `earth_box` of 25 km about the point (a
cube on earthdistance's sphere of 6378168 m, so a place in its corner up to
~43 km away counts), its name the city, its admin1 name the state and its
country code's `countryInfo.txt` name the country; with none in the box,
the first Natural Earth polygon whose outer ring holds the point (Postgres'
`polygon @> point`, its edge too; holes are not read), its `ADM0_A3`'s
name the country (24 codes, Kosovo's `KOS` among them, have none); else
nothing, and at 0, 0 nothing, which Immich takes for no GPS. Of places at
one distance the first in the file wins. The answer, JSON of no build
(`X-Geocoder-Dataset` alone), gives what Immich stores as
`immich.stored` (`city`, `state`, `country`) and as `address` and
`display_name` for the page's field table, and how it found it:
`immich.how` (`cities500`, `natural_earth`, `nothing` or `no_gps`), the
place (`geonameid`, name, point, codes, `distance_m`,
`beyond_max_distance`) and `in_box`, or the Natural Earth row. The column
has no search and no polygons (`/search` and `/polygons` of it are a 400),
and its search head says so; feedback of it carries its dataset id.

Someone who disagrees with an answer can tell us what they expected. Each
column's answer in the detail and each column's search has a "Disagree?"
button, and the toolbar's Feedback sends a general report. Its form sends
the column's dataset, the build that answered and the answer the page
showed, what they expected (in words; by name, road, house number, city,
postcode and country; or that the answer should be empty), a comment and
the link's state to `POST /feedback?key=`, a JSON body of at most 16 KiB
(413 past it; the page cuts an answer of over 8 KiB to the start of its
JSON). A client IP sends 20 reports a minute (429 past them), a body that
isn't a report is a 400, and one kept a 201. `GET /feedback/count`, of no
key, gives `{"today": <n>, "total": <n>}`, which the page thanks with. The
portal appends each report as one JSON line to
`<data>/feedback/<YYYY-MM-DD>.jsonl` of the UTC day it came: on the box,
`/ssd/test-portal/data-portal/feedback/`, the volume of the portal on
`:3002`. A report keeps only its own fields: nothing of who sent it, no IP
and no email, as the form says. A line (one line in the file):

```json
{"received": "2026-11-21T20:41:07Z", "kind": "reverse", "lat": 48.8584, "lon": 2.2945,
 "q": null, "result_index": null, "dataset": "admin-minimal.simplified.none",
 "build": "2026-11-21-9bc04e7d1a25", "answer": {"display_name": "Paris, France", "address": {}},
 "answer_trimmed": false,
 "expected": {"text": "the Eiffel Tower", "name": null, "road": "Avenue Gustave Eiffel",
              "house_number": null, "city": null, "postcode": null, "country": null, "empty": false},
 "comment": "the tower is right there",
 "page": {"columns": ["with-addresses", "smallest"], "point": {"lat": 48.8584, "lon": 2.2945}, "q": null}}
```

`kind` is `reverse` (at `lat`, `lon`), `search` (of `q`, and of its
result at `result_index`, from 0, at that result's point; `null` for the
results as a whole) or `general` (at the selected point, if any, of no
dataset). `answer` is the JSON the page showed, a reverse lookup's answer
or a search's results, or the start of it as a string where
`answer_trimmed` is true. `page` is the link's `d=` (the columns), `at=`
and `q=`.

The test key the page sends allows 200 requests a second and 5,000,000 a
day (UTC), shared by every visitor. The page keeps 8 in flight and asks a
429 of `/reverse` or `/search` again after a pause (a search's 429 pauses
the searches alone), 8 tries in all, after which the 429 is its answer;
once the day's 5,000,000 are spent, `/reverse` and `/search` answer 429 until
the UTC day turns. Its `/polygons` (of no rate limit) don't wait out
those pauses, and the portal runs 8 lookups at once, the others waiting
their turn.

`query-portal` checks the bucket for a new build itself, every hour
(`PORTAL_SYNC_SECONDS`), and 10 minutes after a round that failed (or
`PORTAL_SYNC_SECONDS`, where that is less). A round that finds latest.json
and the build's manifest naming the build it serves fetches nothing else.
It fetches a new build into a dir of its own in the volume, each file
checked against the build's manifest (~11.2 GiB a build, ~9 GiB to
download): each dataset's dir holds hard links of the build's files, files
the build holds twice byte for byte are fetched once and hard-linked, and
no file is shared between the dirs of two builds. Once the build loads, and
before it switches to it, it builds the build's 12 place search indexes in
the build's dir, as a device builds its own (`search/<group>/`, ~0.9 GiB
once the files they hold alike are hard-linked, ~7 GiB at the peak while
they build): 4 at once (`PORTAL_SEARCH_JOBS`, at most the CPUs it may run
on), each on one thread at the lowest priority, the costliest first, ~156 s
and ~10 CPU-minutes for the planet build. A group that fails doesn't hold
the build back: it is switched to all the same, and the next round tries
the group again. At start it serves the build at once and opens its
indexes after, building those its dir lacks. An index is used again while
its search_meta.json names the files beside it and the index format the
image builds: an image of another index builder of the same format keeps
the indexes the build holds until the next build. It fetches only with room
free for the files still to fetch, 7 GiB for the indexes and 2 GiB besides,
so a volume that holds the build served and the one before needs ~20 GiB
more free to fetch a third, ~45 GiB in all. A sync killed part way resumes in its dir: it reads
the files of the objects its ledger names against their sha256 and removes
those that fail and the files the ledger doesn't name before it weighs the
room. It switches to a build only once every file of it holds its
manifest's bytes, each read against its sha256 again, each dataset's dir
holds hard links of the build's files and nothing else, and every dataset
loads. A build whose files fail that check is fetched again, into a new
dir, not refused; one whose files check out but whose datasets don't load
is refused, as is one whose manifest is of another build version than it
reads, each saying so on `/health`. While latest.json gives another build
version than the manifest, the sync fails and retries, refusing nothing. A
build published again under its date (a rerun of the nightly) is another
build: it fetches that one too, and a refusal of the build as first
published doesn't hold against it. At start it serves the newest build the
volume holds whose files all hold their manifest's bytes, each read against
its sha256 before the build is served (~3 s for a planet build on an NVMe
disk, cold or warm), and fetches one that doesn't again. Until it has a
build to serve, `/health`, `/datasets`, `/reverse` and `/polygons` answer
503 with the reason, `/reverse` and `/polygons` whatever key they are sent.
Besides the build it serves, it keeps the newest complete build it holds of
another date (the one it served before, but where that was of the same
date: a build published again under its date takes the place of the one
first published), and an incomplete one of the latest build, to resume.
Once latest.json names a build it keeps again, as published then (its
manifest the same byte for byte), it serves that one again once its files
check out and its datasets load, fetching only its manifest. A date in
`<data>/latest.override` stands in for the bucket's latest.json. One
`query-portal` serves a volume: it holds `<data>/portal.lock` for as long
as it runs, and a second one started on the same volume exits at once,
saying so.

## API

### GET /reverse

Query parameters:
- `lat` - latitude (required)
- `lon` - longitude (required)
- `key` - API key (required)

Example request:

```
GET /reverse?lat=43.7384&lon=7.4246&key=YOUR_API_KEY
```

Response follows [Nominatim](https://nominatim.org/release-docs/latest/api/Reverse/) format:

```json
{
  "display_name": "Avenue de la Costa 42, 98000 Monaco, Monaco",
  "address": {
    "house_number": "42",
    "road": "Avenue de la Costa",
    "city": "Monaco",
    "state": "Monaco",
    "county": "Monaco",
    "postcode": "98000",
    "country": "Monaco",
    "country_code": "MC"
  }
}
```

Fields are omitted when not available.

Where the dataset has a water dir (a `water` value other than `none`), an
answer whose point lies in a body of water carries it last, beside the
address, which it never changes (46.45, 6.55, in Lake Geneva):

```json
{
  "display_name": "Thonon-les-Bains, Upper Savoy, Auvergne-Rhône-Alpes, Metropolitan France, France",
  "address": {
    "municipality": "Thonon-les-Bains",
    "state": "Auvergne-Rhône-Alpes",
    "region": "Metropolitan France",
    "county": "Upper Savoy",
    "country": "France",
    "country_code": "FR"
  },
  "water": { "name": "Lake Geneva", "class": "water", "type": "lake" }
}
```

`class` and `type` are this dataset's feature labels. They generally follow
Nominatim's categories (`water/lake`, `water/reservoir`, `natural/bay`,
`natural/wetland`, ...), with extensions for features such as river areas
(`waterway/river`, `waterway/canal`) and Marine Regions seas and oceans
(`place/sea`, `place/ocean`). The
smallest body of water holding the point answers: a lake in a wetland, a river
in an estuary, a bay in a sea. A point at sea that no address reaches answers
`{"address": {}, "water": {...}}`. A point on an island mapped as a hole of its
lake is in no water, unless a larger one holds the island. Nominatim's default
answer has no water at all; [NOMINATIM_REFERENCE.md](NOMINATIM_REFERENCE.md#water-an-extension)
gives how this field relates to its `layer=natural` lookup, the selection rule
and how river areas are named.

Status codes:
- `200` - success
- `401` - missing or invalid API key
- `429` - rate limit exceeded

### Authentication

The server includes a web dashboard for managing API keys. On first launch, navigate to the server URL in a browser to create an admin account. Once logged in, you can generate API keys and create additional users with configurable rate limits.

## Architecture

The project consists of four components:

- **Builder** (`builder/`, C++) - `build-index` parses OSM PBF files and creates a compact binary index using S2 geometry cells for spatial lookup. `geocoder-diff` and `geocoder-patch` make and apply the daily patches ([builder/PATCHING.md](builder/PATCHING.md)), and `pbf-apply` applies OSM replication diffs to a PBF.
- **Server** (`server/`, Rust) - Memory-maps the index files and serves queries via HTTP/HTTPS with sub-millisecond latency. It is also the `query_server` library the binding wraps.
- **Node binding** (`node/`) - The native module Immich uses to geocode in-process: `Geocoder.load(dir)` (the dataset and its `water/` dir), then `reverseGeocode(lat, lng)` resolves to the response JSON as a string, `water` among it. `openPlaceSearch()` opens the place search index of the dataset (shipped with the bigger packs, else built on the device; see [builder/PATCHING.md](builder/PATCHING.md#place-search-index)), then `searchPlaces(query)` resolves to the results as a JSON string. `createTransportDecoder(dir, files)` decodes a downloading transport unit into the dir it is given, an existing one: the dataset dir for the units of the dataset's own dirs, `<dataset>/water/` for those of a water dir, whose files have the names of the boundary units' ([PATCHING.md](PATCHING.md#transport-units)).
- **Test portal** (`test-portal/`) - The map page served at `/test`, and the image and compose file of `query-portal`, which serves every dataset of the published planet build without water for the page to compare side by side.

### Index Structure

A full index holds the files below. The published variants split them
between a mode dir, a quality dir and a POI dir; the files of each are listed
in [PATCHING.md](PATCHING.md) and `builder/configurations.template.json`.
Some download as transport units, a download-only column form that decodes
to the same files ([builder/PATCHING.md](builder/PATCHING.md#transport-units)).

| Files | Description |
|------|-------------|
| `geo_cells.bin`, `geo_entries.bin` | S2 cell index for streets, addresses and interpolations: each cell's three lists, in blocks of 64 cells, each list against the one before (GXT2, `builder/src/geo_index.h`); the server reads a query's cell and its neighbours in one walk per block |
| `street_ways.bin`, `street_nodes.bin` | Street ways: a row per way (its name, where its nodes start) in groups of 16 ways, and the ways' nodes as deltas in a block per way (`builder/src/way_files.h`). A fresh build writes the ways in cell order: by the least street cell that lists a way, then by name and nodes, so the ways of one place sit together (a continent's files keep the planet's order) |
| `way_parents.bin`, `way_postcodes.bin` | Per street: smallest containing admin polygon, postcode from its postal boundary; a value per street in blocks of 64 (`builder/src/side_blocks.h`) |
| `addr_points.bin`, `addr_vertices.bin`, `addr_index.bin` | Address points (coordinates, house number, street) in blocks of 16 records, building outlines as a blob per building, and where each block and its blobs start (`builder/src/addr_files.h`); in cell order as the ways are, then by street and house number |
| `addr_postcodes.bin` | Per address point: its own `addr:postcode`, in blocks as the streets' side files |
| `interp_ways.bin`, `interp_nodes.bin` | Interpolation ways and their nodes, in the street ways' format; in cell order as the ways are, then by street and range |
| `interp_postcodes.bin` | Per interpolation way: its TIGER ZIP (TIGER regions only), in blocks as the other side files |
| `admin_cells.bin`, `admin_entries.bin` | S2 cell index for admin boundaries: lists in blocks of 8 cells, each against the one before (the cell pair layout, `builder/src/cell_pairs.h`) |
| `admin_polygons.bin`, `admin_rings.bin` | Admin polygon records (postcode areas among them, at level 11) and their rings, as refs to arcs |
| `arc_index.bin`, `arc_vertices.bin` | The boundary arcs those rings share, each border once (per boundary tier: `quality/simplified` or `quality/exact`) |
| `admin_parents.bin` | Per admin polygon: the next larger polygon containing it |
| `place_cells.bin`, `place_nodes.bin`, `place_hidden.bin` | Place nodes (city, town, village, suburb, ...): the visible ones in admin cell order, bit-packed in blocks of 64, with each cell's first row; the ones a boundary hides (its label for place search) beside them (`builder/src/place_files.h`) |
| `postcode_centroids.bin`, `postcode_centroid_cells.bin` | Postcode centroids in admin cell order, Rice-coded steps in blocks of 32 with a box per block (PCC1, `builder/src/postcode_files.h`), and each admin cell's rows of them |
| `poi_records.bin`, `poi_vertices.bin`, `poi_cells.bin`, `poi_entries.bin` | Points of interest and their cell index (a list per cell) |
| `strings_core.bin`, `strings_street.bin`, `strings_addr.bin`, `strings_postcode.bin`, `strings_poi.bin` | Deduplicated string pool, in five tiers so each variant ships only the strings it uses; each tier front-coded in buckets cut by content (FCC2, `builder/src/string_files.h`), a string's id still its offset in the tier's pool |
| `strings_layout.json` | Start and end of each string tier, which place every string offset |
| `poi_meta.json` | POI category names and scoring parameters |
| `water/` | The water layer, a set of its own in the admin files' formats (`admin_polygons.bin`, `admin_rings.bin`, `arc_index.bin`, `arc_vertices.bin`, `admin_cells.bin`, `admin_entries.bin`, `strings_core.bin`, `strings_layout.json`) and `water_meta.json`, each type code's class, type and Nominatim rank: a record per polygon carrying its OSM object's name, type code and area, and nothing pointing into the other files (`server/src/water.rs`, `builder/src/water_layer.h`) |

## Building from Source

### Prerequisites

**Builder (C++):**
- CMake 3.16+
- C++17 compiler
- protozero, s2geometry and abseil, libdeflate (`build-index`); zlib and expat (`pbf-apply`)
- the static libzstd, `libzstd.a` (`geocoder-patch`, which decompresses patches in process;
  Debian/Ubuntu `libzstd-dev`, Fedora `libzstd-static`)
- the `zstd` command, which `geocoder-diff` compresses patches with

**Server (Rust):**
- Rust toolchain

### Build

```bash
# Build the indexer
mkdir build && cd build && cmake ../builder && make

# Without libzstd: geocoder-patch runs the zstd command on PATH instead
cmake ../builder -DGEOCODER_PATCH_LIBZSTD=OFF && make

# Build the server
cargo build --release --manifest-path server/Cargo.toml
```

### Run

```bash
# Create index from PBF file, into a new or empty output-dir (it refuses any other:
# an earlier build's files left there would be published as this one's). The index
# is output-dir/planet; with --continents, each continent's dir beside it is one too
./build/build-index output-dir input.osm.pbf [input2.osm.pbf ...]

# Start the server (on output-dir instead, a --continents build serves continent by continent)
./server/target/release/query-server output-dir/planet [bind-address]

# Start with automatic HTTPS (certificates cached in --cache, default acme-cache)
./server/target/release/query-server output-dir/planet --domain geocoder.example.com
```

### Planet builds

A full planet build (`--continents --multi-output --multi-quality`, the
nightly's configuration; `--multi-quality` writes the two boundary tiers,
`quality/simplified` and `quality/exact`, which `--multi-output` needs: its
mode dirs hold no admin geometry and are read beside a tier) takes about 5 minutes on 64 cores and fits in
80 GB of RAM (6.5 minutes there): the write phase sizes how much it runs at
once to the memory limit, the smaller of MemTotal and the process's cgroup
limit. Set
`GC_MEMORY_LIMIT_MIB` to cap it further when other work shares the host,
or `GC_MEMORY_RESERVE_MIB` to keep that much of the limit for other work
under it (the nightly's fetch shares the job's cgroup).
Disk: the planet PBF (~95 GB) plus the output (~120 GB), and the previous
day's output for `--prev-output`.

The distro S2 and abseil packages keep their debug checks on. The nightly
links static copies built from the same package sources without them
(`tools/build_s2.sh <prefix>`, then `cmake -DGEOCODER_S2_PREFIX=<prefix>`).
The indexes are identical either way; the release libraries are faster.

`pbf-apply in.osm.pbf change.osc.gz... -o out.osm.pbf` applies replication
diffs with the same result as `osmium apply-changes`, in a few minutes on a
planet. `--direct-io` writes the output around the page cache. That is faster
on a fast NVMe, but the next reader of the output then finds none of it cached.

## Memory Footprint

The server is designed to run inside memory-constrained deployments
(e.g. as an Immich sidecar on small NAS/VPS hosts):

- All index files are memory-mapped with `MADV_RANDOM`; nothing is
  loaded into the heap at startup. Process anonymous memory stays
  under ~10 MiB regardless of dataset size and query volume.
- Resident file pages are clean page cache: the kernel reclaims them
  under memory pressure, and a cgroup/container limit simply bounds
  the cache working set. Measured on a full planet index: stable
  operation and ~5–15 ms warm-query latency inside a hard 32 MiB
  container limit; a 64–256 MiB limit buys better cache hit rates on
  busy instances.
- Dataset downloads stream (HTTP → zstd decode → hash → disk) with
  RAM bounded by a few network chunks, so multi-GiB index files can
  be fetched under the same limits.
- The async worker pool defaults to `min(cores, 8)` threads; override
  with `GEOCODER_WORKER_THREADS`.
- `GET /health` returns `200 {"status":"ok","regions":N}` once the
  index is serving — wire it to container health checks.

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GEOCODER_WORKER_THREADS` | Async worker pool size | `min(cores, 8)` |
| `GEOCODER_ALLOW_ANONYMOUS` | `true` accepts requests without an API key, configuration changes included, with no rate limit: only for a server on a private container network | (disabled) |

## License

    Apache License, Version 2.0

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
