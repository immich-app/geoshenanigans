use super::*;
use crate::test_rng::Rng;

const US: u16 = (b'U' as u16) << 8 | b'S' as u16;
const STEP: u64 = 1 << 29;

/// The level-16 cell of position `pos` along the curve.
fn cell(pos: u64) -> u64 {
    pos << 29 | 1 << 28
}

fn run(pos: u64, code: &str) -> LogicalRow {
    LogicalRow { cell: cell(pos), country_code: if code.is_empty() { 0 } else { US }, code: code.to_string() }
}

fn grid(rows: &[LogicalRow]) -> PostcodeGrid {
    PostcodeGrid::open(FileBytes::Owned(encode(rows))).unwrap()
}

fn at(g: &PostcodeGrid, pos: u64) -> Option<String> {
    g.postcode_at(cell(pos)).map(|(country, code)| {
        assert_eq!(country, *b"US");
        code.to_string()
    })
}

#[test]
fn postcode_grid_reads_as_the_shared_vectors_say_and_the_test_encoder_writes_them() {
    let rows = crate::exact_pip::tests::testdata_rows("postcode_grid.tsv");
    let hex = |s: &str| (0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap()).collect::<Vec<u8>>();
    let mut zero = false;
    for r in &rows {
        assert_eq!(r.len(), 3, "{r:?}");
        let logical: Vec<LogicalRow> = r[1]
            .split(';')
            .filter(|s| !s.is_empty() && *s != "-")
            .map(|s| {
                let f: Vec<&str> = s.splitn(3, ',').collect();
                LogicalRow {
                    cell: u64::from_str_radix(f[0], 16).unwrap(),
                    country_code: u16::from_str_radix(f[1], 16).unwrap(),
                    code: f[2].to_string(),
                }
            })
            .collect();
        let want = hex(&r[2]);
        assert_eq!(encode(&logical), want, "{}", r[0]);
        let g = PostcodeGrid::open(FileBytes::Owned(want)).unwrap_or_else(|e| panic!("{}: {e}", r[0]));
        let mut got = Vec::new();
        assert!(g.for_rows(|row| got.push(row)), "{}", r[0]);
        assert_eq!(got, logical, "{}", r[0]);
        assert_eq!(g.rows() as usize, logical.len(), "{}", r[0]);
        // Each row's cell has its postcode, and the cell before it the row before it's.
        for (k, row) in logical.iter().enumerate() {
            let want = (row.country_code != 0).then(|| (row.country_code.to_be_bytes(), row.code.as_str()));
            assert_eq!(g.postcode_at(row.cell), want, "{} row {k}", r[0]);
            let before = match k {
                0 => None,
                _ => (logical[k - 1].country_code != 0).then(|| (logical[k - 1].country_code.to_be_bytes(), logical[k - 1].code.as_str())),
            };
            let step = 1u64 << (row.cell.trailing_zeros() + 1);
            if row.cell > step {
                assert_eq!(g.postcode_at(row.cell - step), before, "{} before row {k}", r[0]);
            }
        }
        zero |= logical.is_empty();
    }
    assert!(rows.len() >= 8 && zero);
}

#[test]
fn a_cell_has_its_runs_postcode_and_none_before_the_first_or_in_a_run_of_none() {
    let g = grid(&[run(1000, "19026"), run(1003, ""), run(1010, "19073"), run(1011, "19026")]);
    assert_eq!(g.level(), 16);
    assert_eq!(at(&g, 999), None);
    assert_eq!(at(&g, 0), None);
    assert_eq!(at(&g, 1000).as_deref(), Some("19026"));
    assert_eq!(at(&g, 1002).as_deref(), Some("19026"));
    assert_eq!(at(&g, 1003), None);
    assert_eq!(at(&g, 1009), None);
    assert_eq!(at(&g, 1010).as_deref(), Some("19073"));
    assert_eq!(at(&g, 1011).as_deref(), Some("19026"));
    // The last run reaches the end of the curve.
    assert_eq!(at(&g, 6 << 32).as_deref(), Some("19026"));
    // A finer cell inside one of the grid's is read as the grid's cell would be, as a query
    // gives the grid's own level.
    assert_eq!(g.postcode_at(cell(1001) - 1).map(|(_, c)| c), Some("19026"));
    // A grid of no rows names no cell.
    let empty = grid(&[]);
    assert_eq!((empty.rows(), empty.level()), (0, 0));
    assert_eq!(at(&empty, 1000), None);
}

#[test]
fn every_cell_reads_its_run_across_blocks() {
    // Runs over three blocks and a few rows: every cell from before the first row to past the
    // last reads its run's postcode.
    let mut rng = Rng::new(9);
    let mut rows = Vec::new();
    let mut pos = 500;
    for i in 0..3 * BLOCK_ROWS + 7 {
        let code = if i > 0 && rng.next() % 4 == 0 { String::new() } else { format!("{}", 19000 + i % 5) };
        rows.push(run(pos, &code));
        pos += 1 + rng.next() % 6;
    }
    rows.dedup_by(|b, a| a.code == b.code);
    let g = grid(&rows);
    let mut k = 0;
    for p in 400..pos + 20 {
        while k < rows.len() && cell(p) >= rows[k].cell {
            k += 1;
        }
        let want = if k == 0 || rows[k - 1].code.is_empty() { None } else { Some(rows[k - 1].code.clone()) };
        assert_eq!(at(&g, p), want, "cell {p}");
    }
}

#[test]
fn a_grid_no_builder_writes_is_refused_at_open_and_a_garbled_block_reads_without_a_panic() {
    let rows: Vec<LogicalRow> = (0..300).map(|i| run(100 + 3 * i, if i % 4 == 3 { "" } else { ["19026", "19073", "19008"][(i % 3) as usize] })).collect();
    let file = encode(&rows);
    let open = |b: Vec<u8>| PostcodeGrid::open(FileBytes::Owned(b));
    assert!(open(file[..file.len() - 13].to_vec()).is_err());
    assert!(open(file[..6].to_vec()).err().unwrap().contains("no PGR1 footer"));
    let mut level = file.clone();
    let n = level.len();
    level[n - 8] = 21;
    assert!(open(level).is_err());
    let mut reserved = file.clone();
    reserved[n - 7] = 1;
    assert!(open(reserved).is_err());
    // Every byte flipped but the footer's: whatever it reads, it reads without a panic.
    let mut rng = Rng::new(4);
    for at_byte in 0..n - FOOTER_BYTES {
        let mut b = file.clone();
        b[at_byte] ^= 1 << (rng.next() % 8);
        let Ok(g) = open(b) else { continue };
        for p in (90..1100).step_by(7) {
            let _ = g.postcode_at(cell(p));
        }
        let _ = g.for_rows(|_| {});
    }
    assert_eq!(STEP, cell(1) - cell(0));
}
