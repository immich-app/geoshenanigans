// find_admin answers for many points, cheap to diff between two readers: one line per point
// with every AdminResult field ('|'-joined, '-' for none).
//
//   admin_dump <dir> < "lat lng" lines > answers.txt
use query_server::{Index, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL};
use std::io::{BufRead, BufWriter, Write};

fn main() {
    let dir = std::env::args().nth(1).expect("usage: admin_dump <dir>");
    let idx = Index::load(&dir, DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).expect("load");
    let stdout = std::io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for line in std::io::stdin().lock().lines() {
        let line = line.unwrap();
        let mut it = line.split_whitespace();
        let (Some(Ok(lat)), Some(Ok(lng))) = (it.next().map(str::parse::<f64>), it.next().map(str::parse::<f64>)) else { continue };
        let a = idx.find_admin(lat, lng);
        let cc = a.country_code.map(|c| String::from_utf8_lossy(&c).into_owned()).unwrap_or_else(|| "-".into());
        let f = [a.country, a.state, a.province, a.region, a.state_district, a.county, a.district, a.municipality, a.city, a.town,
                 a.village, a.hamlet, a.city_district, a.borough, a.suburb, a.quarter, a.neighbourhood, a.city_block];
        let fields: Vec<&str> = f.iter().map(|x| x.unwrap_or("-")).collect();
        writeln!(out, "{cc}|{}", fields.join("|")).unwrap();
    }
}
