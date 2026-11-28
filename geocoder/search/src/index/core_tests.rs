use crate::build::{build_core, BuildOpts};
use crate::fixture::{Geo, TempDir};
use crate::index::entities::{Entities, EntityKind};
use crate::index::terms::{Fuzz, TermInfo, TermKind, TermSource};
use crate::index::{CoreIndex, CoreRecords};
use crate::profile::Profile;
use std::{collections::BTreeSet, fs};

#[test]
fn core_membership_counts_and_names_follow_the_dataset() {
    let tmp = TempDir::new("core");
    let set = tmp.0.join("set");
    let mut f = Geo::default();
    let country = f.boundary("United States", 2, [0., 0., 1., 1.]);
    f.country(country, *b"US");
    let county = f.boundary("West County", 6, [0., 0., 1., 0.5]);
    let east = f.boundary("East County", 6, [0., 0.5, 1., 1.]);
    let city = f.place_area("West City", 1, [0.3, 0.3, 0.7, 0.7]);
    let place = f.place("Liberty Park Drive", 1, 0.4, 0.4);
    f.place_tags(place, &[("alt_name", "Liberty Park Dr;Liberty Park Drive"), ("name", "Liberty Park Drive")]);
    let poi = f.poi("FUTO", 0.6, 0.6, 0);
    f.poi_tags(poi, &[("name:de", "Büro")]);
    let geo = f.load(&set);
    let stamps = crate::dataset::Stamps::of(&set).unwrap();
    let mut files = Vec::new();
    for threads in [1, 4, 16] {
        let out = tmp.0.join(threads.to_string());
        let opts = BuildOpts { threads, ..BuildOpts::new(out.join("tmp")) };
        build_core(&geo, &stamps, &set, &out, Profile::Immich, &opts).unwrap();
        let bytes: Vec<_> = crate::constants::CORE_FILES.iter().map(|name| fs::read(out.join(name)).unwrap()).collect();
        if files.is_empty() {
            files = bytes
        } else {
            assert_eq!(files, bytes)
        }
        let core = CoreIndex::open(&out).unwrap();
        let records = CoreRecords::open(&geo, &set).unwrap();
        assert_eq!(core.entities.len(), 6);
        assert_eq!(fs::metadata(out.join("core.ent")).unwrap().len(), 8 + 4 * 6);
        let west: BTreeSet<_> = [county, city].into_iter().flat_map(|s| core.areas.ranges(s).flatten()).collect();
        let mut brute = BTreeSet::new();
        let mut prev = None;
        for id in 0..core.entities.len() {
            let e = core.entities.get(id).unwrap();
            let row = records.read(e).unwrap();
            let (lat, lon) = row.point;
            let slots: BTreeSet<_> = geo.find_admin(lat, lon).ranked.into_iter().map(|p| p.slot).collect();
            let chain = core.chains.of_entity(id).unwrap();
            let levels: [Vec<u32>; 16] = std::array::from_fn(|level| {
                chain
                    .iter()
                    .copied()
                    .filter(|&s| usize::from(geo.admin_polygon(s).unwrap().admin_level) == level)
                    .collect()
            });
            let order = (levels, e);
            assert!(prev.as_ref().is_none_or(|p| p <= &order));
            prev = Some(order);
            for slot in [county, east, city] {
                assert_eq!(core.areas.ranges(slot).any(|r| r.contains(&id)), slots.contains(&slot));
            }
            if slots.contains(&city) || slots.contains(&county) {
                brute.insert(id);
            }
        }
        assert_eq!(west, brute);
        let full = &core.terms.exact(TermKind::Word, "west county")[0];
        assert_eq!(full.addr_count, core.areas.ranges(county).map(|r| r.end - r.start).sum::<u32>());
        let word = &core.terms.exact(TermKind::Partial, "west")[0];
        assert_eq!((word.count, word.addr_count), (2, brute.len() as u32));
        let liberty = core.terms.exact(TermKind::Word, "liberty park dr");
        assert_eq!(
            liberty.iter().map(|t| (&*t.word, &*t.lookup_word, t.count)).collect::<Vec<_>>(),
            [("liberty park dr", "liberty park dr", 1), ("liberty park drive@en", "liberty park dr", 1)]
        );
        let futo = &core.terms.exact(TermKind::Word, "futo")[0];
        let id = core.terms.posting(futo.postings[0]).unwrap().ids().next().unwrap();
        assert_eq!(core.entities.get(id).unwrap().kind(), EntityKind::Poi);
        assert!(core.prefix.get("fu").contains(&id));
        assert!(!core.terms.prefix(TermKind::Word, "b", 1).is_empty());
        assert_eq!(core.terms.fuzzy(TermKind::Word, "ftuo", Fuzz { edits: 1, anchor: 1 }, 1)[0].token, "futo");
        assert_eq!(core.terms.exact(TermKind::Country, "us")[0].info, TermInfo::Country(*b"us"));
        let meta: serde_json::Value = serde_json::from_slice(&fs::read(out.join("search_meta.json")).unwrap()).unwrap();
        assert_eq!(meta["format"], 3);
        assert!(meta["core"]["dataset"]["poi_tags.bin"].is_string());
    }
}

#[test]
fn a_frequent_term_seeks_and_walks_past_its_importance_head() {
    use crate::constants::{SKIP_EVERY, TOPK_MIN_DF, TOPK_PER_TERM};
    let tmp = TempDir::new("frequent");
    let set = tmp.0.join("set");
    let mut f = Geo::default();
    let n = TOPK_MIN_DF + 17;
    for i in 0..n {
        f.poi("Common", 1., 1., (i % 251) as u8);
    }
    let geo = f.load(&set);
    let imps: Vec<_> = (1..=n).collect();
    fs::write(set.join("poi_imp.bin"), query_server::side_blocks::encode(&imps)).unwrap();
    let opts = BuildOpts::new(tmp.0.join("runs"));
    build_core(&geo, &crate::dataset::Stamps::of(&set).unwrap(), &set, &tmp.0, Profile::Immich, &opts).unwrap();
    let core = CoreIndex::open(&tmp.0).unwrap();
    let term = &core.terms.exact(TermKind::Word, "common")[0];
    let list = core.terms.posting(term.postings[0]).unwrap();
    assert_eq!(list.head().collect::<Vec<_>>(), (n - TOPK_PER_TERM as u32..n).rev().collect::<Vec<_>>());
    for target in [0, SKIP_EVERY - 1, SKIP_EVERY, SKIP_EVERY + 1, n - 1, n, u32::MAX] {
        assert_eq!(list.seek(target).collect::<Vec<_>>(), (target..n).collect::<Vec<_>>());
    }
    assert_eq!(list.best(2, |id| (id < 8).then_some(id)), [7, 6]);
    assert_eq!(core.prefix.get("comm")[0], n - 1);
}

#[test]
fn interrupted_core_publish_is_refused_until_rebuilt() {
    use crate::constants::{BUILD_ASIDE_DIR, BUILD_STAGE_DIR};
    let dir = TempDir::new("publish-core");
    let set = dir.0.join("set");
    let mut f = Geo::default();
    f.place("One", 0, 1., 1.);
    let geo = f.load(&set);
    let opts = BuildOpts::new(dir.0.join("runs"));
    let stamp = crate::dataset::Stamps::of(&set).unwrap();
    build_core(&geo, &stamp, &set, &dir.0, Profile::Immich, &opts).unwrap();
    let aside = dir.0.join(BUILD_STAGE_DIR).join(BUILD_ASIDE_DIR);
    fs::create_dir_all(&aside).unwrap();
    fs::rename(dir.0.join("core.ent"), aside.join("core.ent")).unwrap();
    assert!(CoreIndex::open(&dir.0).is_err());
    build_core(&geo, &stamp, &set, &dir.0, Profile::Immich, &opts).unwrap();
    assert_eq!(CoreIndex::open(&dir.0).unwrap().entities.len(), 1);
}

#[test]
fn an_interrupted_generation_stays_unreadable_after_a_failed_rebuild() {
    use crate::constants::{BUILD_ASIDE_DIR, BUILD_STAGE_DIR};
    let tmp = TempDir::new("interrupted-generation");
    let set = tmp.0.join("dataset");
    let mut f = Geo::default();
    f.poi("one", 1., 1., 0);
    let geo = f.load(&set);
    let stamps = crate::dataset::Stamps::of(&set).unwrap();
    let opts = BuildOpts::new(tmp.0.join("runs"));
    build_core(&geo, &stamps, &set, &tmp.0, Profile::Immich, &opts).unwrap();
    let aside = tmp.0.join(BUILD_STAGE_DIR).join(BUILD_ASIDE_DIR);
    fs::create_dir_all(&aside).unwrap();
    fs::write(aside.join("core.fst"), b"previous version").unwrap();
    fs::write(set.join("poi_tags.bin"), b"broken").unwrap();
    assert!(build_core(&geo, &stamps, &set, &tmp.0, Profile::Immich, &opts).is_err());
    assert!(CoreIndex::open(&tmp.0).is_err());
    assert!(aside.join("core.fst").exists());
}

#[test]
fn a_shipped_chain_dictionary_is_read_in_id_order_and_partial_inputs_fail() {
    let tmp = TempDir::new("shipped-chains");
    let set = tmp.0.join("set");
    let mut f = Geo::default();
    let poly = f.boundary("Country", 2, [1., 1., 2., 2.]);
    f.country(poly, *b"MX");
    f.poi("Palace", 1.5, 1.5, 0);
    f.poi("Outside", 0., 0., 0);
    let geo = f.load(&set);
    let opts = BuildOpts::new(tmp.0.join("sort"));
    let build = |dir: &str| {
        build_core(&geo, &crate::dataset::Stamps::of(&set).unwrap(), &set, &tmp.0.join(dir), Profile::Immich, &opts)
    };
    build("geometry").unwrap();
    fs::write(set.join("admin_chains.bin"), query_server::side_blocks::encode(&[1])).unwrap();
    assert!(build("side").is_err());
    fs::write(set.join("search_chains.bin"), [0, 1, 0]).unwrap();
    assert!(build("side").is_err());
    fs::write(set.join("poi_chains.bin"), query_server::side_blocks::encode(&[1, 0])).unwrap();
    build("side").unwrap();
    for file in crate::constants::CORE_FILES {
        assert_eq!(
            fs::read(tmp.0.join("side").join(file)).unwrap(),
            fs::read(tmp.0.join("geometry").join(file)).unwrap(),
            "{file}"
        );
    }
    fs::write(set.join("search_chains.bin"), [0, 1]).unwrap();
    assert!(build("side").is_err());
}

#[test]
fn a_city_crossing_the_county_line_is_in_both_counties_in_the_full_profile() {
    let tmp = TempDir::new("crossing");
    let set = tmp.0.join("set");
    let mut f = Geo::default();
    let west = f.boundary("West County", 6, [0., 0., 1., 0.5]);
    let east = f.boundary("East County", 6, [0., 0.5, 1., 1.]);
    let city = f.place_area("Crossing", 1, [0.3, 0.3, 0.7, 0.6]);
    // The fixture lists each polygon in the cells of its records, as the builder does for vertices.
    f.poi("Corner", 0.3, 0.6, 0);
    f.poi("Corner", 0.3, 0.3, 0);
    let geo = f.load(&set);
    let stamp = crate::dataset::Stamps::of(&set).unwrap();
    for profile in Profile::ALL {
        let out = tmp.0.join(profile.as_str());
        build_core(&geo, &stamp, &set, &out, profile, &BuildOpts::new(tmp.0.join("sort"))).unwrap();
        let core = CoreIndex::open(&out).unwrap();
        let id = (0..core.entities.len())
            .find(|&id| core.entities.get(id).is_some_and(|e| e.kind() == EntityKind::Area && e.record() == city))
            .unwrap();
        let counties = [west, east].into_iter().filter(|&c| core.areas.ranges(c).any(|r| r.contains(&id))).count();
        assert_eq!(counties, if profile == Profile::Full { 2 } else { 1 });
    }
}

#[test]
fn damaged_offsets_and_rows_end_the_walk() {
    use crate::constants::TOPK_MIN_DF;
    let tmp = TempDir::new("bad-core");
    let set = tmp.0.join("set");
    let mut f = Geo::default();
    for _ in 0..TOPK_MIN_DF {
        f.poi("Same", 1., 1., 0);
    }
    let geo = f.load(&set);
    let stamp = crate::dataset::Stamps::of(&set).unwrap();
    build_core(&geo, &stamp, &set, &tmp.0, Profile::Immich, &BuildOpts::new(tmp.0.join("runs"))).unwrap();
    for file in ["core.ent", "core.fst", "core.post", "core.chn", "core.area", "core.pfx", "core.pfx.fst"] {
        let path = tmp.0.join(file);
        let original = fs::read(&path).unwrap();
        for pos in (0..original.len()).step_by((original.len() / 11).max(1)) {
            let mut b = original.clone();
            b[pos] ^= 0xff;
            fs::write(&path, b).unwrap();
            if let Ok(core) = CoreIndex::open(&tmp.0) {
                for t in core.terms.prefix(TermKind::Word, "", 3) {
                    if let Some(p) = core.terms.posting(t.postings[0]) {
                        for id in p.seek(1).chain(p.head()) {
                            let _ = core.entities.get(id);
                            let _ = core.chains.of_entity(id);
                        }
                    }
                }
                let _ = core.prefix.get("sa");
                let _ = core.areas.ranges(0).count();
            }
        }
        fs::write(path, original).unwrap();
    }
}

#[test]
fn exact_prefix_and_fuzzy_terms_order_the_source_names_before_taking_the_limit() {
    let tmp = TempDir::new("word-order");
    let set = tmp.0.join("set");
    let mut f = Geo::default();
    let i = f.poi("Airport", 0., 0., 0);
    f.poi_tags(i, &[("name:en", "Airport")]);
    f.poi("Aprt", 0., 0., 0);
    let geo = f.load(&set);
    build_core(
        &geo,
        &crate::dataset::Stamps::of(&set).unwrap(),
        &set,
        &tmp.0,
        Profile::Immich,
        &BuildOpts::new(tmp.0.join("sort")),
    )
    .unwrap();
    let core = CoreIndex::open(&tmp.0).unwrap();
    let names: Vec<_> = core.terms.exact(TermKind::Word, "aprt").into_iter().map(|t| t.word).collect();
    assert_eq!(names, ["airport@en", "aprt"]);
    assert_eq!(core.terms.prefix(TermKind::Word, "aprt", 1)[0].word, "airport@en");
    assert_eq!(core.terms.fuzzy(TermKind::Word, "aprt", Fuzz { edits: 0, anchor: 0 }, 1)[0].word, "airport@en");
}

#[test]
fn a_core_is_reused_only_for_its_dataset_and_profile() {
    use crate::dataset::{open_core, BuildOnDevice, Origin, Stamps};
    let tmp = TempDir::new("reuse");
    let set = tmp.0.join("set");
    let index = tmp.0.join("index");
    let mut f = Geo::default();
    f.poi("One", 1., 1., 0);
    let geo = f.load(&set);
    let stamps = Stamps::of(&set).unwrap();
    let open = |profile| open_core(&geo, &stamps, &set, &index, profile, &BuildOnDevice::default()).map(|(_, o)| o);
    assert_eq!(open(Profile::Immich).unwrap(), Origin::Built);
    fs::write(set.join("poi_addr.host"), "ignored").unwrap();
    assert_eq!(open(Profile::Immich).unwrap(), Origin::Current);
    assert_eq!(open(Profile::Full).unwrap(), Origin::Built);
    assert_eq!(open(Profile::Full).unwrap(), Origin::Current);
    f.poi("Two", 1., 1., 0);
    let changed = f.load(&set);
    assert!(open(Profile::Full).is_err());
    let (core, origin) =
        open_core(&changed, &Stamps::of(&set).unwrap(), &set, &index, Profile::Full, &BuildOnDevice::default())
            .unwrap();
    assert_eq!(origin, Origin::Built);
    assert_eq!(core.entities.len(), 2);
}

#[test]
fn ranked_postings_resolve_each_candidate_before_it_can_take_a_result_slot() {
    use crate::fixture::Rng;
    use crate::index::entities::{EntityRecord, EntityRef};
    struct Damaged<'a> {
        inner: CoreRecords<'a>,
        unreadable: BTreeSet<u32>,
    }
    impl Entities for Damaged<'_> {
        fn read(&self, e: EntityRef) -> Option<EntityRecord<'_>> {
            if self.unreadable.contains(&e.record()) {
                return None;
            }
            self.inner.read(e)
        }
    }
    let tmp = TempDir::new("candidate-records");
    let set = tmp.0.join("set");
    let mut f = Geo::default();
    let n = crate::constants::TOPK_MIN_DF + 1;
    for _ in 0..n {
        f.poi("Park", 1., 1., 0);
    }
    let geo = f.load(&set);
    let mut rng = Rng(7);
    let scores: Vec<u32> = (0..n).map(|_| 1 + (rng.next() % 100) as u32).collect();
    fs::write(set.join("poi_imp.bin"), query_server::side_blocks::encode(&scores)).unwrap();
    build_core(
        &geo,
        &crate::dataset::Stamps::of(&set).unwrap(),
        &set,
        &tmp.0,
        Profile::Immich,
        &BuildOpts::new(tmp.0.join("runs")),
    )
    .unwrap();
    let core = CoreIndex::open(&tmp.0).unwrap();
    let term = &core.terms.exact(TermKind::Word, "park")[0];
    for threshold in [0, 5, 75, 100] {
        let unreadable = (0..n).filter(|_| rng.next() % 100 < threshold).collect();
        let reader = Damaged { inner: CoreRecords::open(&geo, &set).unwrap(), unreadable };
        let mut expected: Vec<_> =
            (0..n).filter(|&i| i % 11 == 0 && reader.read(core.entities.get(i).unwrap()).is_some()).collect();
        expected.sort_by_key(|&i| (std::cmp::Reverse(scores[i as usize]), i));
        expected.truncate(17);
        let actual = core.best(term.postings[0], &reader, 17, |id, _| id % 11 == 0);
        assert_eq!(actual, expected);
    }
}

#[test]
fn an_empty_core_opens_and_searches() {
    let tmp = TempDir::new("empty-core");
    let set = tmp.0.join("set");
    let geo = Geo::default().load(&set);
    build_core(
        &geo,
        &crate::dataset::Stamps::of(&set).unwrap(),
        &set,
        &tmp.0,
        Profile::Immich,
        &BuildOpts::new(tmp.0.join("runs")),
    )
    .unwrap();
    let core = CoreIndex::open(&tmp.0).unwrap();
    assert!(core.entities.is_empty());
    assert_eq!(core.chains.of_entity(0), None);
    assert!(core.prefix.get("x").is_empty());
    assert!(core.terms.exact(TermKind::Word, "x").is_empty());
}

#[test]
fn full_records_read_outline_bounds_and_optional_host_object_ids() {
    use crate::index::entities::{BBox, EntityRef, OsmKind, OsmRef};
    let tmp = TempDir::new("core-record-geometry");
    let mut f = Geo::default();
    let poly = f.boundary("County", 6, [1., 2., 3., 4.]);
    f.poi("Bench", 2., 3., 0);
    let geo = f.load(&tmp.0);
    let mut bytes = Vec::new();
    for word in [0xd0510ead_u32, 1, 1] {
        bytes.extend(word.to_le_bytes());
    }
    bytes.extend([3, 0, 0, 0]);
    bytes.extend(42u64.to_le_bytes());
    fs::write(tmp.0.join("admin_polygons.osm_ids"), &bytes).unwrap();
    let e = EntityRef::new(EntityKind::Area, poly).unwrap();
    let full = CoreRecords::for_profile(&geo, &tmp.0, Profile::Full).unwrap();
    let r = full.read(e).unwrap();
    assert_eq!(r.osm, Some(OsmRef { kind: OsmKind::Relation, id: 42 }));
    assert_eq!(r.bbox, Some(BBox { min_lat: 1., min_lon: 2., max_lat: 3., max_lon: 4. }));
    assert_eq!(full.read(EntityRef::new(EntityKind::Poi, 0).unwrap()).unwrap().osm, None);
    drop(full);
    bytes[13] = 1;
    fs::write(tmp.0.join("admin_polygons.osm_ids"), &bytes).unwrap();
    assert_eq!(CoreRecords::for_profile(&geo, &tmp.0, Profile::Full).unwrap().read(e).unwrap().osm, None);
    bytes.pop();
    fs::write(tmp.0.join("admin_polygons.osm_ids"), bytes).unwrap();
    assert!(CoreRecords::for_profile(&geo, &tmp.0, Profile::Full).is_err());
    assert_eq!(CoreRecords::open(&geo, &tmp.0).unwrap().read(e).unwrap().osm, None);
}

#[test]
fn full_core_is_independent_of_worker_count_and_immich_ignores_host_files() {
    let tmp = TempDir::new("core-determinism");
    let set = tmp.0.join("set");
    let mut f = Geo::default();
    f.place("Town", 1, 0.5, 0.5);
    f.boundary("Region", 6, [0., 0., 1., 1.]);
    f.place_area("Town", 1, [0.2, 0.2, 0.8, 0.8]);
    // Sixty-four rows fill all sixteen term workers, with one posting split across them.
    for _ in 3..64 {
        f.poi("Town Park", 0.5, 0.5, 0);
    }
    let geo = f.load(&set);
    let build = |profile, threads| {
        let dir = tmp.0.join(format!("{}-{threads}", Profile::as_str(profile)));
        build_core(
            &geo,
            &crate::dataset::Stamps::of(&set).unwrap(),
            &set,
            &dir,
            profile,
            &BuildOpts { threads, ..BuildOpts::new(dir.join("tmp")) },
        )
        .unwrap();
        crate::constants::CORE_FILES.iter().map(|f| fs::read(dir.join(f)).unwrap()).collect::<Vec<_>>()
    };
    let full = build(Profile::Full, 1);
    assert_eq!(build(Profile::Full, 4), full);
    assert_eq!(build(Profile::Full, 16), full);
    let shipped = build(Profile::Immich, 1);
    fs::write(set.join("poi_rows.osm_ids"), b"only the full resolver reads this").unwrap();
    assert_eq!(build(Profile::Immich, 4), shipped);
}

#[test]
fn absent_parent_levels_keep_the_same_tuple_order_as_shipped_chain_ids() {
    let tmp = TempDir::new("missing-chain-level");
    let set = tmp.0.join("set");
    let mut f = Geo::default();
    let state = f.boundary("State", 4, [1., 1., 2., 2.]);
    let county = f.boundary("County", 6, [3., 3., 4., 4.]);
    let geo = f.load(&set);
    let build = |name: &str| {
        let out = tmp.0.join(name);
        build_core(
            &geo,
            &crate::dataset::Stamps::of(&set).unwrap(),
            &set,
            &out,
            Profile::Immich,
            &BuildOpts::new(out.join("tmp")),
        )
        .unwrap();
        crate::constants::CORE_FILES.iter().map(|f| fs::read(out.join(f)).unwrap()).collect::<Vec<_>>()
    };
    let geometry = build("geometry");
    fs::write(set.join("admin_chains.bin"), query_server::side_blocks::encode(&[1, 0])).unwrap();
    fs::write(set.join("search_chains.bin"), [1, county as u8, 1, state as u8]).unwrap();
    assert_eq!(build("shipped"), geometry);
}

#[test]
fn a_build_cleans_its_default_scratch_and_preserves_shared_scratch() {
    let tmp = TempDir::new("scratch-owner");
    let set = tmp.0.join("set");
    let out = tmp.0.join("index");
    let mut f = Geo::default();
    f.place("Town", 1, 1., 1.);
    let geo = f.load(&set);
    let run = |scratch: &std::path::Path| {
        build_core(
            &geo,
            &crate::dataset::Stamps::of(&set).unwrap(),
            &set,
            &out,
            Profile::Immich,
            &BuildOpts::new(scratch.into()),
        )
        .unwrap()
    };
    run(&out.join("tmp"));
    assert!(!out.join("tmp").exists());
    let shared = tmp.0.join("shared");
    let other = shared.join(format!("core-{}-0", std::process::id()));
    fs::create_dir_all(&other).unwrap();
    run(&shared);
    assert!(other.is_dir());
    assert_eq!(fs::read_dir(shared).unwrap().count(), 1);
}

#[test]
fn an_indexed_record_uses_its_ranked_country_and_recovers_a_missing_country() {
    let tmp = TempDir::new("chain-country");
    let set = tmp.0.join("set");
    let mut f = Geo::default();
    let country = f.boundary("Testland", 2, [0., 0., 2., 2.]);
    f.country(country, *b"DE");
    let city = f.boundary("Town", 8, [0.2, 0.2, 1.5, 1.5]);
    let other = f.boundary("Neighbor", 2, [2., 2., 3., 3.]);
    f.country(other, *b"FR");
    let unranked = f.boundary("Country way", 2, [2., 2., 3., 3.]);
    f.country(unranked, *b"FR");
    f.area_rank(unranked, 0);
    f.poi("Inside", 1., 1., 0);
    f.poi("Outside", 4., 4., 0);
    let geo = f.load(&set);
    let out = tmp.0.join("core");
    let stamps = crate::dataset::Stamps::of(&set).unwrap();
    build_core(&geo, &stamps, &set, &out, Profile::Immich, &BuildOpts::new(tmp.0.join("runs"))).unwrap();
    let core = CoreIndex::open(&out).unwrap();
    let records = CoreRecords::open(&geo, &set).unwrap();
    for id in 0..core.entities.len() {
        let entity = core.entities.get(id).unwrap();
        let direct = records.read(entity).unwrap();
        assert_eq!(core.record(id, &records), Some(direct.clone()));
        for outer in [None, Some(city), Some(unranked), Some(u32::MAX)] {
            assert_eq!(records.read_in_chain(entity, outer), Some(direct.clone()));
        }
        if entity.kind() == EntityKind::Area && entity.record() == city {
            // An area's parent chain may cross a border away from its label point.
            assert_eq!(records.read_in_chain(entity, Some(other)), Some(direct.clone()));
        }
        if &*direct.name == "Inside" {
            assert_eq!(core.chains.outer_slot(id), Some(country));
            assert_eq!(direct.country_code, Some(*b"de"));
        }
    }
    assert!(core.record(core.entities.len(), &records).is_none());
}
