// The Immich column (immich.rs): what Immich's reverse geocoding stores for
// a photo, over geodata files of the layout of its release image's, written
// here: cities500.txt lines of a few places, their admin1 and admin2 names,
// countryInfo.txt (its lines ended by \r\n, as GeoNames' are) and a Natural
// Earth GeoJSON of a few squares. /reverse answers it without a build.

use super::*;
use crate::portal::immich::{self, Geodata, Source};

/// The release the fixtures' immich-release.txt names.
const VERSION: &str = "v3.4.2";
const COMMIT: &str = "b6b4b80df113656bfa4872cab31c7319e89438a9";
const GEODATA_DATE: &str = "2026-10-29T14:18:17+00:00";

/// The point `north` and `east` metres from (lat, lon) on earthdistance's
/// sphere.
fn offset((lat, lon): (f64, f64), north: f64, east: f64) -> (f64, f64) {
    let lat2 = lat + (north / immich::EARTH).to_degrees();
    (lat2, lon + (east / (immich::EARTH * lat2.to_radians().cos())).to_degrees())
}

/// The great-circle metres between two points on that sphere (haversine).
fn metres((lat1, lon1): (f64, f64), (lat2, lon2): (f64, f64)) -> f64 {
    let (p1, p2) = (lat1.to_radians(), lat2.to_radians());
    let h = ((p2 - p1) / 2.0).sin().powi(2) + p1.cos() * p2.cos() * ((lon2 - lon1).to_radians() / 2.0).sin().powi(2);
    2.0 * immich::EARTH * h.sqrt().asin()
}

/// A line of cities500.txt.
fn city(id: i64, name: &str, (lat, lon): (f64, f64), feature: &str, country: &str, admin1: &str, admin2: &str) -> String {
    let fields = [
        id.to_string(), name.to_string(), name.to_string(), String::new(), lat.to_string(), lon.to_string(), "P".to_string(),
        feature.to_string(), country.to_string(), String::new(), admin1.to_string(), admin2.to_string(), String::new(), String::new(),
        "600".to_string(), String::new(), "10".to_string(), "Etc/UTC".to_string(), "2026-10-01".to_string(),
    ];
    fields.join("\t")
}

/// A square of lat `s`..`n` and lon `w`..`e`, as a GeoJSON ring.
fn square(s: f64, w: f64, n: f64, e: f64) -> serde_json::Value {
    serde_json::json!([[w, s], [e, s], [e, n], [w, n], [w, s]])
}

/// Where the fixtures' places are.
const CORNER: (f64, f64) = (1.0, 0.5);
const STRAIGHT: (f64, f64) = (5.0, 40.0);
const NEAREST: (f64, f64) = (10.0, 10.0);
const TIED: (f64, f64) = (-20.0, -60.0);
const AUSTRALIA: (f64, f64) = (-33.9, 151.2);

/// Writes the fixtures' geodata into `dir`, as the image lays it out.
fn write_geodata(dir: &Path) {
    let places = [
        // 24 km north and 24 km east of CORNER: 33.9 km away, in a corner
        // of the box.
        city(1, "Cornerville", offset(CORNER, 24_000.0, 24_000.0), "PPL", "TL", "01", "001"),
        // 26 km north of STRAIGHT: beyond the box's side.
        city(2, "Farnorth", offset(STRAIGHT, 26_000.0, 0.0), "PPL", "TL", "01", "001"),
        // About NEAREST: the nearest place, one farther, and two nearer
        // whose feature codes the import leaves out.
        city(3, "Fartown", offset(NEAREST, 8_000.0, 0.0), "PPLA", "TL", "01", "002"),
        city(4, "Neartown", offset(NEAREST, 0.0, 3_000.0), "PPL", "TL", "01", "002"),
        city(5, "Old Quarter", offset(NEAREST, 1_000.0, 0.0), "PPLX", "TL", "01", "002"),
        city(6, "Ruins", offset(NEAREST, 0.0, 500.0), "PPLH", "TL", "01", "002"),
        // Two places at one point, the larger geonameid first in the file;
        // their admin1 code isn't in admin1CodesASCII.txt.
        city(20, "First", TIED, "PPL", "TL", "99", ""),
        city(10, "Second", TIED, "PPL", "TL", "99", ""),
        // A PPLX of Australia, which the import keeps.
        city(30, "Surry Hills", offset(AUSTRALIA, 200.0, 0.0), "PPLX", "AU", "02", ""),
    ];
    let geojson = serde_json::json!({
        "type": "FeatureCollection",
        "name": "ne_10m_admin_0_countries",
        "features": [
            // A square with a hole, which the import leaves out.
            { "type": "Feature", "properties": { "ADMIN": "Bigland", "ADM0_A3": "BIG", "TYPE": "Sovereign country" },
              "geometry": { "type": "Polygon", "coordinates": [square(20.0, 20.0, 30.0, 30.0), square(24.0, 24.0, 26.0, 26.0)] } },
            // The country in that hole, after it.
            { "type": "Feature", "properties": { "ADMIN": "Enclave", "ADM0_A3": "ENC", "TYPE": "Sovereign country" },
              "geometry": { "type": "Polygon", "coordinates": [square(24.0, 24.0, 26.0, 26.0)] } },
            // Two islands whose code countryInfo.txt lacks.
            { "type": "Feature", "properties": { "ADMIN": "Islands", "ADM0_A3": "ISL", "TYPE": "Indeterminate" },
              "geometry": { "type": "MultiPolygon", "coordinates": [[square(-10.0, 50.0, -9.0, 51.0)], [square(-12.0, 50.0, -11.0, 51.0)]] } },
        ],
    });
    let country_info = [
        "# GeoNames.org Country Information",
        "#ISO\tISO3\tISO-Numeric\tfips\tCountry\tCapital",
        "TL\tTLD\t999\tTL\tTestland \tTestcity",
        "BG\tBIG\t998\tBG\tBigland\tBigcity",
        "EN\tENC\t997\tEN\tEnclave\tSmallcity",
        "AU\tAUS\t036\tAS\tAustralia\tCanberra",
        "XX\tXXX\t000\tXX\t\t",
        "",
    ]
    .join("\r\n");
    std::fs::create_dir_all(dir).unwrap();
    for (name, text) in [
        ("cities500.txt", places.join("\n") + "\n"),
        ("admin1CodesASCII.txt", "TL.01\tTestshire\tTestshire\t100\nAU.02\tNew South Wales\tNew South Wales\t2155400\n".to_string()),
        ("admin2Codes.txt", "TL.01.001\tNorth County\tNorth County\t101\nTL.01.002\tSouth County\tSouth County\t102\n".to_string()),
        ("countryInfo.txt", country_info),
        ("ne_10m_admin_0_countries.geojson", geojson.to_string()),
        (immich::DATE_FILE, GEODATA_DATE.to_string()),
        (immich::RELEASE, format!("version={VERSION}\ncommit={COMMIT}\n")),
    ] {
        std::fs::write(dir.join(name), text).unwrap();
    }
}

/// The fixtures' geodata, in a scratch dir of its own.
fn geodata() -> (ScratchDir, Source, Geodata) {
    let dir = ScratchDir::new();
    write_geodata(dir.path());
    let source = Source::at(dir.path()).unwrap();
    let geodata = Geodata::load(dir.path()).unwrap();
    (dir, source, geodata)
}

/// What Immich answers at `at`, as the column's JSON gives it.
fn answer(source: &Source, geodata: &Geodata, (lat, lon): (f64, f64)) -> serde_json::Value {
    geodata.reverse(lat, lon).json(source)
}

#[test]
fn the_geodata_loads_as_immichs_import_reads_it() {
    let (_dir, source, geodata) = geodata();
    assert_eq!((source.version.as_deref(), source.commit.as_deref(), source.geodata_date.as_str()), (Some(VERSION), Some(COMMIT), GEODATA_DATE));
    // The PPLX outside Australia and the PPLH are left out.
    assert_eq!((geodata.places(), geodata.skipped), (7, 2));
    // A ring a polygon: Bigland's outer one, Enclave's, the two islands'.
    assert_eq!(geodata.rings(), 4);
    assert!(Source::at(&source.dir.join("nowhere")).is_none());
    // The source names the countries as the geodata does, without loading
    // it.
    assert_eq!(source.country_names.get("TL").map(String::as_str), Some("Testland"));
    assert!(!source.country_names.contains_key("XX"));
}

// earth_box's cube has no second test of the distance: a place in its
// corner, 24 km north and 24 km east, is found 33.9 km away, while one 26 km
// straight north is out of it.
#[test]
fn a_place_in_a_corner_of_the_box_is_found_beyond_25_km() {
    let (_dir, source, geodata) = geodata();
    let corner = offset(CORNER, 24_000.0, 24_000.0);
    assert!(metres(CORNER, corner) > 33_900.0, "{}", metres(CORNER, corner));
    let got = answer(&source, &geodata, CORNER);
    assert_eq!(got["immich"]["how"], "cities500");
    assert_eq!(got["immich"]["stored"], serde_json::json!({ "city": "Cornerville", "state": "Testshire", "country": "Testland" }));
    let place = &got["immich"]["place"];
    assert_eq!((place["geonameid"].as_i64(), place["name"].as_str()), (Some(1), Some("Cornerville")));
    assert!((place["distance_m"].as_f64().unwrap() - metres(CORNER, corner)).abs() < 1.0, "{place}");
    assert_eq!(place["beyond_max_distance"], true);
    assert_eq!(got["immich"]["in_box"], 1);

    let straight = answer(&source, &geodata, STRAIGHT);
    assert!(metres(STRAIGHT, offset(STRAIGHT, 26_000.0, 0.0)) < 26_001.0);
    assert_eq!(straight["immich"]["how"], "nothing", "{straight}");
    assert_eq!(straight["immich"]["place"], serde_json::Value::Null);
}

// Of the places in the box the nearest by earth_distance wins: the import's
// PPLX outside Australia and PPLH, nearer, aren't there to; Australia's PPLX
// is. The state is the place's admin1 name, the country its country code's
// name in countryInfo.txt (trimmed), the admin2 name shown beside them.
#[test]
fn the_nearest_place_in_the_box_wins() {
    let (_dir, source, geodata) = geodata();
    let got = answer(&source, &geodata, NEAREST);
    assert_eq!(got["immich"]["stored"], serde_json::json!({ "city": "Neartown", "state": "Testshire", "country": "Testland" }));
    assert_eq!(got["display_name"], "Neartown, Testshire, Testland");
    assert_eq!(got["address"], serde_json::json!({ "city": "Neartown", "state": "Testshire", "country": "Testland" }));
    let place = &got["immich"]["place"];
    assert_eq!((place["geonameid"].as_i64(), place["feature_code"].as_str(), place["admin2"].as_str()), (Some(4), Some("PPL"), Some("South County")));
    assert!((place["distance_m"].as_f64().unwrap() - 3_000.0).abs() < 1.0, "{place}");
    assert_eq!(place["beyond_max_distance"], false);
    assert_eq!(got["immich"]["in_box"], 2);

    let australia = answer(&source, &geodata, AUSTRALIA);
    assert_eq!(australia["immich"]["stored"], serde_json::json!({ "city": "Surry Hills", "state": "New South Wales", "country": "Australia" }));
}

// Of places at one distance the first in cities500.txt wins, whatever its
// geonameid; an admin1 code admin1CodesASCII.txt lacks gives no state.
#[test]
fn of_places_at_one_distance_the_first_in_the_file_wins() {
    let (_dir, source, geodata) = geodata();
    let got = answer(&source, &geodata, offset(TIED, 1_000.0, 1_000.0));
    assert_eq!(got["immich"]["place"]["geonameid"], 20);
    assert_eq!(got["immich"]["stored"], serde_json::json!({ "city": "First", "state": null, "country": "Testland" }));
    assert_eq!(got["display_name"], "First, Testland");
    assert_eq!(got["immich"]["in_box"], 2);
}

// With no place in the box, the country is the first Natural Earth polygon
// whose outer ring holds the point, in the file's order, its edge too: the
// hole of a polygon is not, so an enclave in it is its surround's. Its
// ADM0_A3 names it in countryInfo.txt, where some codes have no line: no
// country. There is no city or state.
#[test]
fn without_a_place_in_the_box_the_natural_earth_country_answers() {
    let (_dir, source, geodata) = geodata();
    let got = answer(&source, &geodata, (21.0, 21.0));
    assert_eq!(got["immich"]["how"], "natural_earth");
    assert_eq!(got["immich"]["stored"], serde_json::json!({ "city": null, "state": null, "country": "Bigland" }));
    assert_eq!(got["immich"]["natural_earth"],
               serde_json::json!({ "admin": "Bigland", "adm0_a3": "BIG", "type": "Sovereign country", "feature": 0, "polygon": 0 }));
    assert_eq!(got["display_name"], "Bigland");

    let enclave = answer(&source, &geodata, (25.0, 25.0));
    assert_eq!(enclave["immich"]["natural_earth"]["admin"], "Bigland");
    let edge = answer(&source, &geodata, (25.0, 20.0));
    assert_eq!(edge["immich"]["natural_earth"]["admin"], "Bigland");

    let island = answer(&source, &geodata, (-11.5, 50.5));
    assert_eq!(island["immich"]["natural_earth"],
               serde_json::json!({ "admin": "Islands", "adm0_a3": "ISL", "type": "Indeterminate", "feature": 2, "polygon": 1 }));
    assert_eq!(island["immich"]["stored"], serde_json::json!({ "city": null, "state": null, "country": null }));
    assert_eq!(island["display_name"], serde_json::Value::Null);
}

// At sea, far from any place and out of every country, Immich stores
// nothing; at 0, 0 its metadata extraction takes the point for no GPS.
#[test]
fn at_sea_nothing_is_stored() {
    let (_dir, source, geodata) = geodata();
    for (at, how) in [((-40.0, -140.0), "nothing"), ((0.0, 0.0), "no_gps")] {
        let got = answer(&source, &geodata, at);
        assert_eq!(got["immich"]["how"], how, "{at:?}");
        assert_eq!(got["immich"]["stored"], serde_json::json!({ "city": null, "state": null, "country": null }), "{at:?}");
        assert_eq!((&got["display_name"], &got["address"]), (&serde_json::Value::Null, &serde_json::json!({})), "{at:?}");
        assert_eq!((&got["immich"]["place"], &got["immich"]["natural_earth"]), (&serde_json::Value::Null, &serde_json::Value::Null));
    }
}

/// A portal of the data dir `data` and the geodata of `geodata` (none: of
/// no geodata), serving no build, at its base URL.
async fn serving(data: &Path, geodata: Option<&Path>) -> String {
    let mut cfg = config_at("http://127.0.0.1:9", Arc::new(load_dataset));
    cfg.immich = geodata.map(|dir| Source::at(dir).unwrap());
    serve(router(portal(data, cfg), db())).await
}

// /reverse?dataset=immich-cities500 answers without a build, checked as any
// /reverse is, named by its dataset alone; it has no polygons and no search.
#[tokio::test]
async fn reverse_of_the_immich_column_answers_without_a_build() {
    let (data, dir) = (ScratchDir::new(), ScratchDir::new());
    write_geodata(dir.path());
    let url = serving(data.path(), Some(dir.path())).await;
    let got = fetch(&format!("{url}/reverse?lat=10.0&lon=10.0&key=test&dataset={}", immich::ID)).await;
    assert_eq!((got.status, got.content_type.as_deref()), (200, Some("application/json")), "{}", got.body);
    assert_eq!((got.header("x-geocoder-dataset"), got.header("x-geocoder-build")), (Some(immich::ID), None));
    let json = got.json();
    assert_eq!(json["immich"]["stored"]["city"], "Neartown");
    assert_eq!((&json["immich"]["release"], &json["immich"]["geodata_date"]), (&serde_json::json!(VERSION), &serde_json::json!(GEODATA_DATE)));
    assert_eq!((json["lat"].as_f64(), json["lon"].as_f64()), (Some(10.0), Some(10.0)));

    let reverse = |query: &str| format!("{url}/reverse?dataset={}&{query}", immich::ID);
    assert_eq!(fetch(&reverse("lat=10&lon=10")).await.status, 401);
    assert_eq!(fetch(&reverse("lat=91&lon=10&key=test")).await.status, 400);
    for route in ["polygons", "search"] {
        let got = fetch(&format!("{url}/{route}?lat=10&lon=10&q=x&key=test&dataset={}", immich::ID)).await;
        assert_eq!(got.status, 400, "{route}");
        assert!(got.body.contains("looks up points alone"), "{route}: {}", got.body);
    }
    // Without a build the build's datasets still answer 503.
    assert_eq!(fetch(&format!("{url}/reverse?lat=10&lon=10&key=test")).await.status, 503);
}

// Geodata that doesn't load answers 503 with why; a portal without geodata
// takes the id for one of a build's datasets, and answers 503 without one.
#[tokio::test]
async fn the_immich_column_says_why_it_cant_answer() {
    let (data, dir) = (ScratchDir::new(), ScratchDir::new());
    write_geodata(dir.path());
    std::fs::remove_file(dir.path().join("cities500.txt")).unwrap();
    let got = fetch(&format!("{}/reverse?lat=10&lon=10&key=test&dataset={}", serving(data.path(), Some(dir.path())).await, immich::ID)).await;
    assert_eq!(got.status, 503);
    assert!(got.body.contains("cities500.txt"), "{}", got.body);

    let none = fetch(&format!("{}/reverse?lat=10&lon=10&key=test&dataset={}", serving(data.path(), None).await, immich::ID)).await;
    assert_eq!(none.status, 503, "{}", none.body);
    assert!(none.body.starts_with("no build to serve"), "{}", none.body);
}

// /datasets lists the Immich column under "others", of kind immich, with its
// release and geodata, its country names, and no search; none where the
// portal has no geodata.
#[tokio::test]
async fn datasets_lists_the_immich_column_among_the_others() {
    let bucket = test_bucket::serve(test_bucket::template_components()).await;
    let geodata = ScratchDir::new();
    write_geodata(geodata.path());
    for (with, others) in [(true, 1), (false, 0)] {
        let data = ScratchDir::new();
        let (_fixtures, load) = fixtures();
        let mut cfg = config(&bucket, load);
        cfg.immich = with.then(|| Source::at(geodata.path()).unwrap());
        let p = portal(data.path(), cfg);
        p.round(true).await.unwrap();
        let url = serve(router(p, db())).await;
        let json = fetch(&format!("{url}/datasets")).await.json();
        assert_eq!(json["datasets"].as_array().unwrap().len(), 28);
        assert_eq!(json["others"].as_array().unwrap().len(), others);
        let reverse = fetch(&format!("{url}/reverse?lat=10&lon=10&key=test&dataset={}", immich::ID)).await;
        if !with {
            assert_eq!(reverse.status, 400, "{}", reverse.body);
            assert!(reverse.body.starts_with(&format!("unknown dataset \"{}\"", immich::ID)), "{}", reverse.body);
            continue;
        }
        assert_eq!(reverse.status, 200);
        let other = &json["others"][0];
        assert_eq!((&other["id"], &other["kind"], &other["label"]), (&serde_json::json!(immich::ID), &serde_json::json!("immich"), &serde_json::json!(immich::LABEL)));
        assert_eq!(other["immich"], serde_json::json!({ "release": VERSION, "commit": COMMIT, "geodata_date": GEODATA_DATE, "max_distance_m": 25000.0 }));
        assert_eq!(other["search"], serde_json::Value::Null);
        assert!(other["description"].as_str().unwrap().contains(VERSION), "{other}");
        // countryInfo.txt's names by alpha-2 code, trimmed, those of no
        // name left out: the page names another column's country by them.
        assert_eq!(other["country_names"], serde_json::json!({ "AU": "Australia", "BG": "Bigland", "EN": "Enclave", "TL": "Testland" }));
    }
}
