use super::*;
use crate::test_rng::Rng;

const US: u16 = (b'U' as u16) << 8 | b'S' as u16;
const PR: u16 = (b'P' as u16) << 8 | b'R' as u16;

fn row(country_code: u16, code: &str, name: &str) -> LogicalRow {
    LogicalRow { country_code, code: code.to_string(), name: name.to_string() }
}

fn table(rows: &[LogicalRow]) -> PostalCities {
    PostalCities::open(FileBytes::Owned(encode(rows))).unwrap()
}

#[test]
fn postal_cities_read_as_the_shared_vectors_say_and_the_test_encoder_writes_them() {
    let rows = crate::exact_pip::tests::testdata_rows("postal_cities.tsv");
    let hex = |s: &str| (0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap()).collect::<Vec<u8>>();
    let mut zero = false;
    for r in &rows {
        assert_eq!(r.len(), 3, "{r:?}");
        let logical: Vec<LogicalRow> = r[1]
            .split(';')
            .filter(|s| !s.is_empty() && *s != "-")
            .map(|s| {
                let f: Vec<&str> = s.splitn(3, ',').collect();
                row(u16::from_str_radix(f[0], 16).unwrap(), f[1], f[2])
            })
            .collect();
        let want = hex(&r[2]);
        assert_eq!(encode(&logical), want, "{}", r[0]);
        let t = PostalCities::open(FileBytes::Owned(want)).unwrap_or_else(|e| panic!("{}: {e}", r[0]));
        let mut got = Vec::new();
        assert!(t.for_rows(|row| got.push(row)), "{}", r[0]);
        assert_eq!(got, logical, "{}", r[0]);
        assert_eq!(t.rows() as usize, logical.len(), "{}", r[0]);
        // Each row's name by its country and code.
        for l in &logical {
            assert_eq!(t.name(l.country_code.to_be_bytes(), &l.code), Some(l.name.as_str()), "{}: {}", r[0], l.code);
        }
        zero |= logical.is_empty();
    }
    assert!(zero);
}

#[test]
fn a_postcode_reads_its_rows_name_and_none_without_a_row() {
    let t = table(&[row(PR, "00601", "Adjuntas"), row(US, "19018", "Clifton Heights"), row(US, "19026", "Drexel Hill"),
                    row(US, "19073", "Newtown Square")]);
    assert_eq!(t.name(*b"US", "19026"), Some("Drexel Hill"));
    assert_eq!(t.name(*b"US", "19073"), Some("Newtown Square"));
    assert_eq!(t.name(*b"US", "19018"), Some("Clifton Heights"));
    assert_eq!(t.name(*b"PR", "00601"), Some("Adjuntas"));
    // A code between rows, before the first, past the last, of another country.
    for (country, code) in [(*b"US", "19020"), (*b"US", "00001"), (*b"US", "99999"), (*b"US", "00601"), (*b"PR", "19026"),
                            (*b"CA", "19026"), (*b"US", "1902"), (*b"US", "190260"), (*b"US", "")] {
        assert_eq!(t.name(country, code), None, "{country:?} {code}");
    }
    // A table of no rows names nothing.
    let empty = table(&[]);
    assert_eq!(empty.rows(), 0);
    assert_eq!(empty.name(*b"US", "19026"), None);
}

#[test]
fn every_row_reads_its_name_across_blocks() {
    let mut rng = Rng::new(3);
    let names = ["Louisville", "Jeffersontown", "Lyndon", "St. Matthews", "Middletown"];
    let mut rows = Vec::new();
    let mut code = 40000u64;
    for _ in 0..3 * BLOCK_ROWS + 11 {
        code += 1 + rng.next() % 4;
        rows.push(row(US, &code.to_string(), names[(rng.next() % 5) as usize]));
    }
    let t = table(&rows);
    for r in &rows {
        assert_eq!(t.name(*b"US", &r.code), Some(r.name.as_str()), "{}", r.code);
    }
    for c in 40000..code + 5 {
        let want = rows.iter().find(|r| r.code == c.to_string()).map(|r| r.name.as_str());
        assert_eq!(t.name(*b"US", &c.to_string()), want, "{c}");
    }
}

#[test]
fn a_us_postcode_is_a_row_by_its_five_digits() {
    for (postcode, want) in [("19026", Some("19026")), ("19026-1234", Some("19026")), (" 19026 ", Some("19026")),
                             ("US 19026", Some("19026")), ("US-19026", Some("19026")), ("us19026", Some("19026")),
                             ("1902", None), ("190261", None), ("19026-12", None), ("00000", None), ("00000-0000", None),
                             ("1902A", None), ("PA 19026", None), ("", None)] {
        assert_eq!(postal_key(*b"US", postcode), want, "{postcode}");
    }
    assert_eq!(postal_key(*b"CA", "19026"), None);
    assert_eq!(postal_key(*b"PR", "00601"), None);
}

#[test]
fn a_table_no_builder_writes_is_refused_at_open_and_a_garbled_block_reads_without_a_panic() {
    let rows: Vec<LogicalRow> =
        (0..200).map(|i| row(US, &format!("{}", 19000 + 3 * i), ["Media", "Drexel Hill", "Wayne"][i % 3])).collect();
    let file = encode(&rows);
    let n = file.len();
    let open = |b: Vec<u8>| PostalCities::open(FileBytes::Owned(b));
    assert!(open(file[..n - 13].to_vec()).is_err());
    assert!(open(file[..6].to_vec()).err().unwrap().contains("no PCY1 footer"));
    let mut blocks = file.clone();
    blocks[n - 8] = 0;
    assert!(open(blocks).is_err());
    let mut reserved = file.clone();
    reserved[n - FOOTER_BYTES - ENTRY_BYTES + 3] = 1;
    assert!(open(reserved).is_err());
    let mut count = file.clone();
    count[n - FOOTER_BYTES - ENTRY_BYTES + 2] = 0;
    assert!(open(count).is_err());
    // Every byte flipped but the footer's: whatever it reads, it reads without a panic.
    let mut rng = Rng::new(4);
    for at_byte in 0..n - FOOTER_BYTES {
        let mut b = file.clone();
        b[at_byte] ^= 1 << (rng.next() % 8);
        let Ok(t) = open(b) else { continue };
        for r in rows.iter().step_by(7) {
            let _ = t.name(*b"US", &r.code);
        }
        let _ = t.for_rows(|_| {});
    }
}

#[test]
fn a_name_its_block_does_not_end_and_a_block_past_its_rows_room_read_as_no_name() {
    // Two rows of one block; the block's last name without its 0 byte.
    let file = encode(&[row(US, "19026", "Drexel Hill"), row(US, "19073", "Wayne")]);
    let table_at = file.len() - FOOTER_BYTES - ENTRY_BYTES;
    assert_eq!(file[table_at - 1], 0);
    let mut unended = file.clone();
    unended[table_at - 1] = b'X';
    let t = PostalCities::open(FileBytes::Owned(unended)).unwrap();
    assert_eq!(t.name(*b"US", "19026"), Some("Drexel Hill"));
    assert_eq!(t.name(*b"US", "19073"), None);
    assert!(!t.for_rows(|_| {}));
    // A block of more bytes than its rows can hold, its rows and names as
    // they were: no row of it reads.
    let file = encode(&[row(US, "19026", "Media")]);
    let table_at = file.len() - FOOTER_BYTES - ENTRY_BYTES;
    let mut long = file[..table_at].to_vec();
    long.extend(std::iter::repeat(b'x').take(4000));
    long.push(0);
    long.extend_from_slice(&file[table_at..]);
    let t = PostalCities::open(FileBytes::Owned(long)).unwrap();
    assert_eq!(t.name(*b"US", "19026"), None);
    assert!(!t.for_rows(|_| {}));
    // As long as its rows can hold: it reads.
    let mut most = file[..table_at].to_vec();
    let room = 2 + CODE_BYTES + NAME_BYTES + 1;
    most.extend(std::iter::repeat(b'x').take(room - table_at - 1));
    most.push(0);
    most.extend_from_slice(&file[table_at..]);
    let t = PostalCities::open(FileBytes::Owned(most)).unwrap();
    assert_eq!(t.name(*b"US", "19026"), Some("Media"));
}
