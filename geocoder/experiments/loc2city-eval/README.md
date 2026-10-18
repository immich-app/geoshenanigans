# loc2city eval: loc-to-city's spot checks against our geocoder

Runs the 423 checks of `ferrybig/loc-to-city` (commit 031a58c,
`verify/src/cases.ts`: 190 spot cases, 141 fixed and 92 open capitals) on
our `grid_query` answers, with their matching rules ported to Python, to see
how our sets map onto a country / region / city picker and where they fail.
Run 2026-10-17 on the box (`/ssd/loc2city`), re-checked the same night with
an independent extractor and evaluator (`verify/`, `/ssd/transport-verify`).

## Results (build 2026-10-13, verified 2026-10-17)

Pass counts with mapper B (country = `country_code`; region = first of
state, province, region, county; city = first of city, town, village,
hamlet, municipality):

| set | B | B + dep + regfill |
|---|---|---|
| minimal (admin-minimal, q2.5) | 303 / 423 | 325 |
| recommended (admin, q2.5, POI major) | **315 / 423** | **337** |
| max (full, uncapped, POI all) | 311 / 423 | 332 |

- The two mapper changes gain 22 checks on recommended and lose none:
  - **dep** (+15 checks, +21 country tiers): a state / region / province
    name matching a Natural Earth admin-0 unit with the same sovereign takes
    that unit's ISO code (Puerto Rico, Åland, French Polynesia, ...). It
    fires on 20 of 7,064 random land points, all real dependencies. The
    proper fix is in the builder, which reads `ISO3166-1:alpha2` only on
    admin_level 2; check what Nominatim reports first.
  - **regfill** (+7): no region key → municipality, then city (city-states).
    Fires on 0.34% of land points.
  - Rejected: city falls back to county (+2, but fires on 31.5% of land
    points), city to state_district / state (−3), name cleaning (cosmetic).
- Their must-keep checks: we pass 242 / 322 on recommended with B. Their
  101 open checks: 73 with B, 78 with dep + regfill.
- Failure classes on recommended (B): naming only 27 (Sicilia / Sicily,
  gmina X / X), dependencies as their sovereign 21, their per-country region
  levels 20, district instead of the city 18 (Tokyo → Shinjuku, London →
  Westminster; needs a non-Nominatim "enclosing city" field), city only as a
  region 12, wrong place 9-10, region missing for city-states 6.
- Bugs found (measured): the max set returns the wrong country in places
  (Kolkata → BD on 12-17 of 200 points with state West Bengal, the Vatican
  → IT on 90 of 90, Nukus, Llívia, Jungholz, Marigot); in every set enclaves
  get the surrounding polygon (Rome / Lazio over the Vatican, Zhuhai over
  Macau), because the builder stored outer rings only (fixed on
  `fix/admin-holes`). Minimal loses 12 checks recommended passes (Dutch
  villages → municipality, Washington DC → Arlington, no city for Moscow /
  Astana).

## Layout

- `parse_cases.py`: extracts every check from `cases.ts` without running
  it (a tokenizer and recursive-descent parser for the literal subset),
  mirroring `evaluate.ts` `allChecks()`.
- `evaluate.py`: their `fold` / `namedFor` / `matches` rules on our answers,
  every check × set × mapper; `mappers.json`: the mapper variants.
- `categorise.py`: hand categorisation of every failing (check, tier);
  anything unmatched prints as UNCLASSIFIED.
- `fetch_preset.py`: downloads a preset the way a client does (flat dir,
  sha256-checked).
- `data/points.txt`: the 421 query points in evaluation order;
  `data/ne_names.json` (country name to ISO, NAME → ISO_A2_EH, most
  populous feature per code) and `data/ne_units.json` (admin-0 units with
  their sovereign, for dep), both derived from the Natural Earth file below.
- `verify/`: the re-check: `l2c_extract.py` (second extractor, different
  method), `l2c_eval.py` (evaluator written from `evaluate.ts`),
  `l2c_dep.py` (own dep / regfill), `l2c_run.sh`, `l2c_probe.sh` (random
  land, Kolkata and Vatican probes), `l2c_spot.py`, `l2c_naming_all.py`,
  `l2c_soft.py`.

The scripts hard-code the box paths (`/ssd/loc2city`, `/ssd/shrink`).

## Rebuild and run

With their repository checked out at 031a58c in `repo/` and the three sets
fetched (below):

```sh
python3 -I parse_cases.py repo/verify/src/cases.ts out/checks.json
cp data/*.json data/points.txt out/
for s in smallest recommended max; do grid_query /ssd/shrink/idx/$s < out/points.txt > out/answers_$s.txt; done
python3 -I evaluate.py out mappers.json variants   # writes out/results_variants.json
python3 -I categorise.py out out/results_variants.json B B+dep+regfill
```

## Data and tool versions

- Sets: build 2026-10-13 from
  `https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds/2026-10-13/`,
  fetched with `fetch_preset.py`: `planet admin-minimal q2.5 none`
  (smallest), `planet admin q2.5 major` (recommended), planet full uncapped
  all (max).
- `grid_query`: `geocoder/server/examples/grid_query.rs` at main 5caed45.
- Natural Earth 10m admin-0 countries v5.1.2
  (`https://raw.githubusercontent.com/nvkelso/natural-earth-vector/v5.1.2/geojson/ne_10m_admin_0_countries.geojson`,
  sha256 239eec57ac17f100…), the file their download script uses.
- Their checks: `https://github.com/ferrybig/loc-to-city`, commit 031a58c.
