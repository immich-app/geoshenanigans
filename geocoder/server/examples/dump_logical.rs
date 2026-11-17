// Writes the logical files of a build dir's street, interpolation, address,
// side and postcode centroid files, each record read through the readers the
// queries read it through (way_files.rs, addr_files.rs, side_blocks.rs,
// postcode_files.rs). They are the files `geocoder-diff --decode-logical`
// writes for the dir through the builder's decoders, byte for byte, so a dir
// whose dump differs from that, or from the records the dir was built from,
// holds a record the server reads as another.
//
//   cargo build --release --example dump_logical
//   ./target/release/examples/dump_logical <build>/planet/full <out-dir>
//
// <out-dir> is made, or is empty. For each family the dir holds it gets the
// records as builder/src/types.h lays them out:
//   street_ways.bin, street_nodes.bin   a WayHeader a street, and the ways'
//                                       nodes back to back, lat then lng (f32)
//   interp_ways.bin, interp_nodes.bin   an InterpWay a way, and their nodes
//   addr_points.bin, addr_vertices.bin  an AddrPoint a record, and the
//                                       footprints' logical blobs back to back
//   way_parents.bin, way_postcodes.bin, addr_postcodes.bin,
//   interp_postcodes.bin                a u32 a record
//   postcode_centroids.bin              a LogicalCentroid a centroid
//                                       (builder/src/postcode_files.h), in
//                                       their order, with its cell's key
// A way's node_offset is the nodes of the ways before it (0 of a way without
// nodes), a footprint's vertex_offset the bytes of the blobs before it in
// units of addr_files::VERTEX_OFFSET_UNIT (NO_DATA of a point).
//
// Every way is read as the scans read one: its row (WayFiles::row), its nodes
// (way) and its first node (first_node). Every address is read in a run of
// ids, as a cell's list is (AddrFiles::for_ids), and alone (record); a
// building's footprint as its vertices (footprint), as its logical blob
// (logical_blob) and as its count (vertex_count). Every side value is read by
// its record's id (SideBlocks::get). Every postcode centroid is read in a run
// of its cell's rows, as a query reads a cell's (PostcodeFiles::for_rows), and
// in its block read whole (rows_of); every cell's rows by the walk of the
// cells file (for_each_cell) and by the cell (cell_rows).
//
// A line a family says what was read, how many reads gave nothing, and how
// many disagreed: a row that counts other nodes than the way has, a first
// node that is not the way's, a record that reads otherwise alone than in its
// run, a footprint whose vertices or count are not its logical blob's, a
// centroid that reads otherwise in its block than in its run, a cell whose
// rows read otherwise by the cell than in the walk. What a read does not give
// is written as zeros, or as no nodes and no footprint.
// The exit status is 0 where every read gave what the others did, 1 where one
// did not, and 2 where nothing was dumped: no such dir, a dir with part of a
// family (one file of a pair, some of the three address files, a side file
// without the file its records are in or of another count), files no builder
// writes, an <out-dir> that holds something.
//
// A family is read on a thread of its own, a record at a time from the mapped
// files into buffered ones, so the memory it takes does not grow with the dir.
use query_server::postcode_files::{self, Centroid, BLOCK_ROWS};
use query_server::way_files::Kind;
use query_server::{
    addr_files, mmap_file, vertex_scale, AddrFiles, AddrRecord, FileBytes, NodeCoord, PostcodeFiles, SideBlocks, WayFiles, WayRow,
    DEFAULT_ADMIN_CELL_LEVEL, NO_DATA,
};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::process::ExitCode;

const USAGE: &str = "usage: dump_logical <build-dir> <out-dir>";

/// The side files, each with the file of the records its values are of.
const SIDE_FILES: [(&str, &str); 4] = [
    ("way_parents.bin", "street_ways.bin"),
    ("way_postcodes.bin", "street_ways.bin"),
    ("addr_postcodes.bin", addr_files::POINTS_FILE),
    ("interp_postcodes.bin", "interp_ways.bin"),
];

/// The ids of a run of address reads: runs start at any record of a block, as a cell's list
/// does.
const RUN: u32 = 1000;

/// The most nodes before a way with nodes, whose node_offset is a u32.
const MAX_NODES_BEFORE: u64 = u32::MAX as u64;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    ExitCode::from(run(&args, &mut std::io::stdout(), &mut std::io::stderr()))
}

/// Dumps the dir `args` name first into the one they name second, with a line a family and
/// the failed reads of them all on `report`: the exit status, with what kept the dump from
/// running on `errors`.
pub fn run(args: &[String], report: &mut impl Write, errors: &mut impl Write) -> u8 {
    let [dir, out] = args else {
        let _ = writeln!(errors, "{USAGE}");
        return 2;
    };
    let families = match dump(dir, out) {
        Ok(families) => families,
        Err(e) => {
            let _ = writeln!(errors, "dump_logical: {e}");
            return 2;
        }
    };
    let mut failed = Tally::default();
    for family in &families {
        let _ = match &family.read {
            Some((what, tally)) => {
                failed.add(*tally);
                writeln!(report, "{}: {what}, {tally}", family.file)
            }
            None => writeln!(report, "{}: none", family.file),
        };
    }
    let _ = writeln!(report, "{failed}");
    u8::from(failed != Tally::default())
}

/// The reads that failed: the ones that gave nothing, and the ones that gave something else
/// than another read of the same record.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Tally {
    nothing: u64,
    disagreed: u64,
}

impl Tally {
    fn add(&mut self, other: Tally) {
        self.nothing += other.nothing;
        self.disagreed += other.disagreed;
    }
}

impl std::fmt::Display for Tally {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} reads gave nothing, {} disagreed", self.nothing, self.disagreed)
    }
}

/// A family by its first file, and what its dump read with the reads that failed; nothing of a
/// family the dir does not hold.
struct Family {
    file: &'static str,
    read: Option<(String, Tally)>,
}

/// The file `name` of `dir`, mapped for a read from its first byte to its last; None where the
/// dir has no such file.
fn map(dir: &str, name: &str) -> Result<Option<FileBytes>, String> {
    let path = format!("{dir}/{name}");
    if !std::path::Path::new(&path).try_exists().map_err(|e| format!("{path}: {e}"))? {
        return Ok(None);
    }
    let bytes = mmap_file(&path)?;
    if let FileBytes::Mmap(map) = &bytes {
        let _ = map.advise(memmap2::Advice::Sequential);
    }
    Ok(Some(bytes))
}

/// The way files of `kind` in `dir`; None where it has neither.
fn ways(dir: &str, kind: Kind) -> Result<Option<WayFiles>, String> {
    match (map(dir, kind.ways_file())?, map(dir, kind.nodes_file())?) {
        (Some(ways), Some(nodes)) => WayFiles::open(kind, ways, nodes).map(Some).map_err(|e| format!("{dir}: {e}")),
        (None, None) => Ok(None),
        (Some(_), None) => Err(format!("{dir}: {} without {}", kind.ways_file(), kind.nodes_file())),
        (None, Some(_)) => Err(format!("{dir}: {} without {}", kind.nodes_file(), kind.ways_file())),
    }
}

/// The postcode centroid files of `dir`, at the cell level their cells file says (as the
/// builder's decoder reads them); None where it has neither.
fn centroids(dir: &str) -> Result<Option<PostcodeFiles>, String> {
    let (centroids, cells) = (postcode_files::CENTROIDS_FILE, postcode_files::CELLS_FILE);
    match (map(dir, centroids)?, map(dir, cells)?) {
        (Some(c), Some(k)) => {
            let level = k.as_slice().get(6).map_or(DEFAULT_ADMIN_CELL_LEVEL, |&level| u64::from(level));
            PostcodeFiles::open(c, k, level).map(Some).map_err(|e| format!("{dir}: {e}"))
        }
        (None, None) => Ok(None),
        (Some(_), None) => Err(format!("{dir}: {centroids} without {cells}")),
        (None, Some(_)) => Err(format!("{dir}: {cells} without {centroids}")),
    }
}

/// The side file `name` of `dir`, which holds a value for each of the `records` of `parent`;
/// None where the dir has no such file.
fn side(dir: &str, name: &str, parent: &str, records: Option<u32>) -> Result<Option<SideBlocks>, String> {
    let Some(bytes) = map(dir, name)? else { return Ok(None) };
    let side = SideBlocks::open(bytes, name).map_err(|e| format!("{dir}: {e}"))?;
    match records {
        Some(records) if records == side.count() => Ok(Some(side)),
        Some(records) => Err(format!("{dir}: {name} holds {} values for the {records} records of {parent}", side.count())),
        None => Err(format!("{dir}: {name} without {parent}")),
    }
}

/// Dumps the families of `dir` into `out`, each on a thread of its own: what each read, the
/// streets, the interpolations, the addresses, each side file and then the postcode centroids.
fn dump(dir: &str, out: &str) -> Result<Vec<Family>, String> {
    if !std::path::Path::new(dir).is_dir() {
        return Err(format!("{dir} is not a dir"));
    }
    let (streets, interps) = (ways(dir, Kind::Streets)?, ways(dir, Kind::Interps)?);
    let file = |name: &str| map(dir, name);
    let addrs = AddrFiles::open(file(addr_files::POINTS_FILE)?, file(addr_files::VERTICES_FILE)?, file(addr_files::INDEX_FILE)?)
        .map_err(|e| format!("{dir}: {e}"))?;
    let (street_ways, interp_ways) = (streets.as_ref().map(WayFiles::count), interps.as_ref().map(WayFiles::count));
    let records = [street_ways, street_ways, addrs.as_ref().map(AddrFiles::count), interp_ways];
    let mut sides = Vec::new();
    for ((name, parent), records) in SIDE_FILES.into_iter().zip(records) {
        sides.push((name, side(dir, name, parent, records)?));
    }
    let postcodes = centroids(dir)?;
    std::fs::create_dir_all(out).map_err(|e| format!("{out}: {e}"))?;
    if std::fs::read_dir(out).map_err(|e| format!("{out}: {e}"))?.next().is_some() {
        return Err(format!("{out} is not empty"));
    }
    std::thread::scope(|scope| {
        let mut threads = vec![
            scope.spawn(|| dump_ways(Kind::Streets, streets.as_ref(), out)),
            scope.spawn(|| dump_ways(Kind::Interps, interps.as_ref(), out)),
            scope.spawn(|| dump_addrs(addrs.as_ref(), out)),
        ];
        for &(name, ref side) in &sides {
            threads.push(scope.spawn(move || dump_side(name, side.as_ref(), out)));
        }
        threads.push(scope.spawn(|| dump_centroids(postcodes.as_ref(), out)));
        threads.into_iter().map(|thread| thread.join().unwrap_or_else(|_| Err("a family's read panicked".to_owned()))).collect()
    })
}

/// A file of the dump, written through a buffer.
struct Out {
    file: BufWriter<File>,
    path: String,
}

impl Out {
    fn create(dir: &str, name: &str) -> Result<Out, String> {
        let path = format!("{dir}/{name}");
        let file = File::create(&path).map_err(|e| format!("{path}: {e}"))?;
        Ok(Out { file: BufWriter::with_capacity(1 << 20, file), path })
    }

    fn put(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.file.write_all(bytes).map_err(|e| format!("{}: {e}", self.path))
    }

    fn finish(mut self) -> Result<(), String> {
        self.file.flush().map_err(|e| format!("{}: {e}", self.path))
    }
}

/// `before`, the nodes or the bytes before a record's own `own` in `file`, as the record's
/// offset holds them, in units of `unit`; an error where the record's end is past the `most`
/// its offset counts.
fn offset(file: &str, before: u64, own: u64, most: u64, unit: u64) -> Result<u32, String> {
    if before + own > most {
        return Err(format!("{file} holds more than the {most} a logical record's offset counts"));
    }
    Ok((before / unit) as u32)
}

fn bits(node: &NodeCoord) -> (u32, u32) {
    (node.lat.to_bits(), node.lng.to_bits())
}

/// The reads of a way that failed: its row, its nodes (`read`: whether they were), and its
/// first node read alone.
fn way_tally(row: Option<&WayRow>, read: bool, nodes: &[NodeCoord], first: Option<NodeCoord>) -> Tally {
    let other_count = read && row.is_some_and(|row| usize::from(row.node_count) != nodes.len());
    let other_first = first.as_ref().map(bits) != nodes.first().map(bits);
    Tally { nothing: u64::from(row.is_none()) + u64::from(!read), disagreed: u64::from(other_count) + u64::from(other_first) }
}

/// Dumps the ways of `kind` into its two files of `out`.
fn dump_ways(kind: Kind, files: Option<&WayFiles>, out: &str) -> Result<Family, String> {
    let Some(files) = files else { return Ok(Family { file: kind.ways_file(), read: None }) };
    let (mut ways_out, mut nodes_out) = (Out::create(out, kind.ways_file())?, Out::create(out, kind.nodes_file())?);
    // An InterpWay, of which a WayHeader is the first bytes.
    let stride = if kind == Kind::Interps { 24 } else { 12 };
    let mut nodes = Vec::new();
    let (mut nodes_before, mut failed) = (0u64, Tally::default());
    for id in 0..files.count() {
        let row = files.row(id);
        let read = files.way(id, &mut nodes);
        failed.add(way_tally(row.as_ref(), read, &nodes, files.first_node(id)));
        let mut record = [0u8; 24];
        if let Some(row) = row {
            let node_offset = if row.node_count == 0 { 0 } else { offset(kind.nodes_file(), nodes_before, 0, MAX_NODES_BEFORE, 1)? };
            record[..4].copy_from_slice(&node_offset.to_le_bytes());
            record[4..6].copy_from_slice(&row.node_count.to_le_bytes());
            record[8..12].copy_from_slice(&row.name.to_le_bytes());
            if kind == Kind::Interps {
                record[12..16].copy_from_slice(&row.start_number.to_le_bytes());
                record[16..20].copy_from_slice(&row.end_number.to_le_bytes());
                record[20] = row.interpolation;
            }
        }
        ways_out.put(&record[..stride])?;
        for node in &nodes {
            nodes_out.put(&node.lat.to_bits().to_le_bytes())?;
            nodes_out.put(&node.lng.to_bits().to_le_bytes())?;
        }
        nodes_before += nodes.len() as u64;
    }
    ways_out.finish()?;
    nodes_out.finish()?;
    Ok(Family { file: kind.ways_file(), read: Some((format!("{} ways, {nodes_before} nodes", files.count()), failed)) })
}

/// A record's fields, a float as its bits.
fn fields(r: &AddrRecord) -> (u32, u32, u32, u32, u32, u64, u32) {
    (r.lat.to_bits(), r.lng.to_bits(), r.housenumber_id, r.street_id, r.parent_way_id, r.blob_at, r.blob_len)
}

/// The reads of an address record that failed: the one in its run, and the one alone.
fn record_tally(in_run: Option<&AddrRecord>, alone: Option<&AddrRecord>) -> Tally {
    Tally {
        nothing: u64::from(in_run.is_none()) + u64::from(alone.is_none()),
        disagreed: u64::from(in_run.zip(alone).is_some_and(|(in_run, alone)| fields(in_run) != fields(alone))),
    }
}

/// The vertices of a logical blob, a point's empty one included.
fn vertices_of(logical: &[u8]) -> usize {
    logical.len().saturating_sub(10) / if logical.first() == Some(&3) { 8 } else { 4 }
}

/// Whether `vertices` are the ones of the logical blob: each the bbox minimum plus its steps
/// on the encoding's grid.
fn are_vertices_of(vertices: &[NodeCoord], logical: &[u8]) -> bool {
    let (Some(header), Some(steps)) = (logical.first_chunk::<10>(), logical.get(10..)) else { return false };
    let scale = vertex_scale(header[0]);
    let bmin_lat = f64::from(f32::from_le_bytes([header[2], header[3], header[4], header[5]]));
    let bmin_lng = f64::from(f32::from_le_bytes([header[6], header[7], header[8], header[9]]));
    let at = |lat: f64, lng: f64| ((bmin_lat + lat * scale) as f32, (bmin_lng + lng * scale) as f32);
    let is = |vertex: &NodeCoord, (lat, lng): (f32, f32)| bits(vertex) == (lat.to_bits(), lng.to_bits());
    if header[0] == 3 {
        let step = |q: &[u8]| f64::from(u32::from_le_bytes([q[0], q[1], q[2], q[3]]));
        steps.len() == 8 * vertices.len() && steps.chunks_exact(8).zip(vertices).all(|(q, vertex)| is(vertex, at(step(q), step(&q[4..]))))
    } else {
        let step = |q: &[u8]| f64::from(u16::from_le_bytes([q[0], q[1]]));
        steps.len() == 4 * vertices.len() && steps.chunks_exact(4).zip(vertices).all(|(q, vertex)| is(vertex, at(step(q), step(&q[2..]))))
    }
}

/// The reads of a building's footprint that failed: its vertices (`drawn`: whether they were
/// read), its logical blob (`has_logical`) and its count.
fn footprint_tally(drawn: bool, vertices: &[NodeCoord], has_logical: bool, logical: &[u8], count: u32) -> Tally {
    let other_vertices = drawn && has_logical && !are_vertices_of(vertices, logical);
    let other_count = has_logical && count != 0 && count as usize != vertices_of(logical);
    Tally {
        nothing: u64::from(!drawn) + u64::from(!has_logical) + u64::from(count == 0),
        disagreed: u64::from(other_vertices) + u64::from(other_count),
    }
}

/// Dumps the address records into addr_points.bin and addr_vertices.bin of `out`.
fn dump_addrs(addrs: Option<&AddrFiles>, out: &str) -> Result<Family, String> {
    let Some(addrs) = addrs else { return Ok(Family { file: addr_files::POINTS_FILE, read: None }) };
    let (mut points, mut blobs) = (Out::create(out, addr_files::POINTS_FILE)?, Out::create(out, addr_files::VERTICES_FILE)?);
    let mut run: Vec<Option<AddrRecord>> = Vec::new();
    let (mut vertices, mut logical) = (Vec::new(), Vec::new());
    let (mut footprints, mut logical_bytes, mut failed) = (0u64, 0u64, Tally::default());
    let mut first = 0;
    while first < addrs.count() {
        let end = first + RUN.min(addrs.count() - first);
        run.clear();
        run.resize((end - first) as usize, None);
        addrs.for_ids(first..end, |id, record| run[(id - first) as usize] = Some(record));
        for (id, in_run) in (first..end).zip(&run) {
            let alone = addrs.record(id);
            failed.add(record_tally(in_run.as_ref(), alone.as_ref()));
            let mut logical_record = [0u32; 7];
            if let Some(record) = in_run.or(alone) {
                let (mut vertex_offset, mut vertex_count) = (NO_DATA, 0);
                if record.is_building() {
                    let drawn = addrs.footprint(&record, &mut vertices);
                    let has_logical = addrs.logical_blob(&record, &mut logical);
                    failed.add(footprint_tally(drawn, &vertices, has_logical, &logical, addrs.vertex_count(&record)));
                    if has_logical {
                        let (most, unit) = (addr_files::MAX_LOGICAL_VERTEX_BYTES, addr_files::VERTEX_OFFSET_UNIT);
                        vertex_offset = offset(addr_files::VERTICES_FILE, logical_bytes, logical.len() as u64, most, unit)?;
                        vertex_count = vertices_of(&logical) as u32;
                        blobs.put(&logical)?;
                        logical_bytes += logical.len() as u64;
                        footprints += 1;
                    }
                }
                let (lat, lng) = (record.lat.to_bits(), record.lng.to_bits());
                logical_record = [lat, lng, record.housenumber_id, record.street_id, record.parent_way_id, vertex_offset, vertex_count];
            }
            for field in logical_record {
                points.put(&field.to_le_bytes())?;
            }
        }
        first = end;
    }
    points.finish()?;
    blobs.finish()?;
    Ok(Family { file: addr_files::POINTS_FILE, read: Some((format!("{} records, {footprints} footprints", addrs.count()), failed)) })
}

/// Dumps the values of the side file `name` into its file of `out`.
fn dump_side(name: &'static str, side: Option<&SideBlocks>, out: &str) -> Result<Family, String> {
    let Some(side) = side else { return Ok(Family { file: name, read: None }) };
    let mut values = Out::create(out, name)?;
    let mut failed = Tally::default();
    for id in 0..side.count() {
        let value = side.get(id);
        failed.nothing += u64::from(value.is_none());
        values.put(&value.unwrap_or(0).to_le_bytes())?;
    }
    values.finish()?;
    Ok(Family { file: name, read: Some((format!("{} values", side.count()), failed)) })
}

/// A centroid's fields, a float as its bits.
fn centroid_fields(c: &Centroid) -> (u32, u32, u32, u16) {
    (c.lat.to_bits(), c.lng.to_bits(), c.postcode_id, c.country_code)
}

/// The reads of a centroid that failed: the one in its cell's run, and the one in its block.
fn centroid_tally(in_run: Option<&Centroid>, in_block: Option<&Centroid>) -> Tally {
    Tally {
        nothing: u64::from(in_run.is_none()) + u64::from(in_block.is_none()),
        disagreed: u64::from(in_run.zip(in_block).is_some_and(|(a, b)| centroid_fields(a) != centroid_fields(b))),
    }
}

/// A centroid's logical row with its cell's key.
fn logical_centroid(c: &Centroid, cell_key: u32) -> [u8; 20] {
    let mut row = [0u8; 20];
    row[..4].copy_from_slice(&c.lat.to_bits().to_le_bytes());
    row[4..8].copy_from_slice(&c.lng.to_bits().to_le_bytes());
    row[8..12].copy_from_slice(&c.postcode_id.to_le_bytes());
    row[12..14].copy_from_slice(&c.country_code.to_le_bytes());
    row[16..].copy_from_slice(&cell_key.to_le_bytes());
    row
}

/// Dumps the postcode centroids into postcode_centroids.bin of `out`, cell by cell.
fn dump_centroids(files: Option<&PostcodeFiles>, out: &str) -> Result<Family, String> {
    let name = postcode_files::CENTROIDS_FILE;
    let Some(files) = files else { return Ok(Family { file: name, read: None }) };
    let mut rows = Out::create(out, name)?;
    let (mut cells, mut failed, mut written) = (0u64, Tally::default(), Ok(()));
    let mut run: Vec<Option<Centroid>> = Vec::new();
    // The block read whole last, and its rows.
    let mut block: (u32, [Option<Centroid>; BLOCK_ROWS]) = (u32::MAX, [None; BLOCK_ROWS]);
    files.for_each_cell(|cell, first, end| {
        cells += 1;
        failed.disagreed += u64::from(files.cell_rows(cell) != (first, end));
        let cell_key = (cell >> 32) as u32;
        let mut at = first;
        while at < end && written.is_ok() {
            let stop = at + RUN.min(end - at);
            run.clear();
            run.resize((stop - at) as usize, None);
            files.for_rows(at, stop, |row, c| run[(row - at) as usize] = Some(c));
            for (row, in_run) in (at..stop).zip(&run) {
                let b = row / BLOCK_ROWS as u32;
                if block.0 != b {
                    block = (b, [None; BLOCK_ROWS]);
                    files.rows_of(b, u32::MAX, |r, c| block.1[r as usize % BLOCK_ROWS] = Some(c));
                }
                let in_block = block.1[row as usize % BLOCK_ROWS];
                failed.add(centroid_tally(in_run.as_ref(), in_block.as_ref()));
                let logical = in_run.or(in_block).map_or([0; 20], |c| logical_centroid(&c, cell_key));
                if written.is_ok() {
                    written = rows.put(&logical);
                }
            }
            at = stop;
        }
    });
    written?;
    rows.finish()?;
    Ok(Family { file: name, read: Some((format!("{} centroids in {cells} cells", files.rows()), failed)) })
}

// The server's tests take this file in as a module (lib.rs): these run with them, and the
// ones that dump a dataset's dir are there, beside the dataset.
#[cfg(test)]
mod tests {
    use super::*;

    fn node(lat: f32, lng: f32) -> NodeCoord {
        NodeCoord { lat, lng }
    }

    fn tally(nothing: u64, disagreed: u64) -> Tally {
        Tally { nothing, disagreed }
    }

    #[test]
    fn a_ways_reads_fail_that_give_nothing_or_what_another_does_not() {
        let row = |node_count: u16| WayRow { node_count, name: 7, start_number: 0, end_number: 0, interpolation: 0 };
        let nodes = [node(1.0, 2.0), node(3.0, 4.0)];
        // A way with nodes and one without, each read alike by every read.
        assert_eq!(way_tally(Some(&row(2)), true, &nodes, Some(nodes[0])), tally(0, 0));
        assert_eq!(way_tally(Some(&row(0)), true, &[], None), tally(0, 0));
        // No row; no nodes, which leaves no first node to differ; neither.
        assert_eq!(way_tally(None, true, &nodes, Some(nodes[0])), tally(1, 0));
        assert_eq!(way_tally(Some(&row(2)), false, &[], None), tally(1, 0));
        assert_eq!(way_tally(None, false, &[], None), tally(2, 0));
        // A row that counts other nodes than were read.
        assert_eq!(way_tally(Some(&row(3)), true, &nodes, Some(nodes[0])), tally(0, 1));
        assert_eq!(way_tally(Some(&row(2)), true, &[], None), tally(0, 1));
        // A first node that is another node, by either coordinate; none of a way with nodes;
        // one of a way without.
        assert_eq!(way_tally(Some(&row(2)), true, &nodes, Some(nodes[1])), tally(0, 1));
        assert_eq!(way_tally(Some(&row(2)), true, &nodes, Some(node(1.0, 4.0))), tally(0, 1));
        assert_eq!(way_tally(Some(&row(2)), true, &nodes, Some(node(3.0, 2.0))), tally(0, 1));
        assert_eq!(way_tally(Some(&row(2)), true, &nodes, None), tally(0, 1));
        assert_eq!(way_tally(Some(&row(0)), true, &[], Some(nodes[0])), tally(0, 1));
        // Nodes are compared by their bits: -0 is not 0, and a NaN is itself.
        assert_eq!(way_tally(Some(&row(1)), true, &[node(0.0, 0.0)], Some(node(-0.0, 0.0))), tally(0, 1));
        assert_eq!(way_tally(Some(&row(1)), true, &[node(f32::NAN, 0.0)], Some(node(f32::NAN, 0.0))), tally(0, 0));
        // Both at once.
        assert_eq!(way_tally(Some(&row(5)), true, &nodes, None), tally(0, 2));
    }

    #[test]
    fn an_address_records_reads_fail_that_give_nothing_or_differ_in_any_field() {
        let record = AddrRecord { lat: 1.0, lng: 2.0, housenumber_id: 3, street_id: 4, parent_way_id: 5, blob_at: 6, blob_len: 7 };
        assert_eq!(record_tally(Some(&record), Some(&record)), tally(0, 0));
        assert_eq!(record_tally(None, Some(&record)), tally(1, 0));
        assert_eq!(record_tally(Some(&record), None), tally(1, 0));
        assert_eq!(record_tally(None, None), tally(2, 0));
        let others = [
            AddrRecord { lat: -1.0, ..record },
            AddrRecord { lng: 2.5, ..record },
            AddrRecord { housenumber_id: 0, ..record },
            AddrRecord { street_id: 0, ..record },
            AddrRecord { parent_way_id: 0, ..record },
            AddrRecord { blob_at: 0, ..record },
            AddrRecord { blob_len: 0, ..record },
        ];
        for other in &others {
            assert_eq!(record_tally(Some(&record), Some(other)), tally(0, 1), "{other:?}");
            assert_eq!(record_tally(Some(other), Some(&record)), tally(0, 1), "{other:?}");
        }
        // Floats are compared by their bits.
        let (zero, nan) = (AddrRecord { lat: 0.0, ..record }, AddrRecord { lng: f32::NAN, ..record });
        assert_eq!(record_tally(Some(&zero), Some(&AddrRecord { lat: -0.0, ..record })), tally(0, 1));
        assert_eq!(record_tally(Some(&nan), Some(&nan)), tally(0, 0));
    }

    #[test]
    fn a_centroids_reads_fail_that_give_nothing_or_differ_in_any_field_and_its_row_holds_its_cells_key() {
        let centroid = Centroid { lat: 51.5, lng: -0.12, postcode_id: 7, country_code: u16::from_be_bytes(*b"GB") };
        assert_eq!(centroid_tally(Some(&centroid), Some(&centroid)), tally(0, 0));
        assert_eq!(centroid_tally(None, Some(&centroid)), tally(1, 0));
        assert_eq!(centroid_tally(Some(&centroid), None), tally(1, 0));
        assert_eq!(centroid_tally(None, None), tally(2, 0));
        for other in [
            Centroid { lat: -0.0, ..centroid },
            Centroid { lng: 0.12, ..centroid },
            Centroid { postcode_id: 8, ..centroid },
            Centroid { country_code: u16::from_be_bytes(*b"IE"), ..centroid },
        ] {
            assert_eq!(centroid_tally(Some(&centroid), Some(&other)), tally(0, 1), "{other:?}");
        }
        let nan = Centroid { lat: f32::NAN, ..centroid };
        assert_eq!(centroid_tally(Some(&nan), Some(&nan)), tally(0, 0));
        // lat, lng, postcode_id, the country code, two zero bytes and the cell's key.
        let row = logical_centroid(&centroid, 0x4876_0000);
        assert_eq!(&row[..4], &51.5f32.to_bits().to_le_bytes());
        assert_eq!(&row[4..8], &(-0.12f32).to_bits().to_le_bytes());
        assert_eq!(&row[8..], &[7, 0, 0, 0, b'B', b'G', 0, 0, 0, 0, 0x76, 0x48]);
    }

    /// A logical blob in `enc`: its bbox minimum and its vertices' steps from it.
    fn logical_blob(enc: u8, (bmin_lat, bmin_lng): (f32, f32), steps: &[(u32, u32)]) -> Vec<u8> {
        let mut blob = vec![enc, 0];
        blob.extend(bmin_lat.to_le_bytes());
        blob.extend(bmin_lng.to_le_bytes());
        for &(lat, lng) in steps {
            if enc == 3 {
                blob.extend(lat.to_le_bytes());
                blob.extend(lng.to_le_bytes());
            } else {
                blob.extend((lat as u16).to_le_bytes());
                blob.extend((lng as u16).to_le_bytes());
            }
        }
        blob
    }

    /// The vertices of steps from a bbox minimum on a grid of `scale` degrees.
    fn stepped((bmin_lat, bmin_lng): (f32, f32), scale: f64, steps: &[(u32, u32)]) -> Vec<NodeCoord> {
        let to = |bmin: f32, step: u32| (f64::from(bmin) + f64::from(step) * scale) as f32;
        steps.iter().map(|&(lat, lng)| node(to(bmin_lat, lat), to(bmin_lng, lng))).collect()
    }

    #[test]
    fn a_footprints_vertices_are_its_logical_blobs_on_the_grid_of_its_encoding() {
        let bmin = (52.37, -4.89);
        let steps = [(0, 40), (120, 0), (65_535, 65_535), (7, 9)];
        for (enc, scale) in [(0, 1e-5), (1, 1e-4), (2, 1e-6), (3, 1e-7)] {
            // Encoding 3 holds steps of 32 bits.
            let steps: Vec<(u32, u32)> = steps.iter().map(|&(lat, lng)| if enc == 3 { (lat << 12, lng << 9) } else { (lat, lng) }).collect();
            let logical = logical_blob(enc, bmin, &steps);
            let vertices = stepped(bmin, scale, &steps);
            assert_eq!(vertices_of(&logical), 4, "encoding {enc}");
            assert!(are_vertices_of(&vertices, &logical), "encoding {enc}");
            // Not a vertex more or fewer, a vertex a float off on either axis, vertices in
            // another order, or the ones of another grid.
            assert!(!are_vertices_of(&vertices[..3], &logical), "encoding {enc}");
            assert!(!are_vertices_of(&[&vertices[..], &vertices[..1]].concat(), &logical), "encoding {enc}");
            for k in 0..4 {
                let mut off = vertices.clone();
                off[k].lat = f32::from_bits(off[k].lat.to_bits() + 1);
                assert!(!are_vertices_of(&off, &logical), "encoding {enc}, vertex {k}");
                let mut off = vertices.clone();
                off[k].lng = f32::from_bits(off[k].lng.to_bits() + 1);
                assert!(!are_vertices_of(&off, &logical), "encoding {enc}, vertex {k}");
            }
            let mut swapped = vertices.clone();
            swapped.swap(0, 1);
            assert!(!are_vertices_of(&swapped, &logical), "encoding {enc}");
            assert!(!are_vertices_of(&stepped(bmin, scale * 10.0, &steps), &logical), "encoding {enc}");
            // Nor of a blob cut inside a vertex or its header.
            assert!(!are_vertices_of(&vertices, &logical[..logical.len() - 1]), "encoding {enc}");
            assert!(!are_vertices_of(&[], &logical[..9]), "encoding {enc}");
        }
        // No vertices are those of a header alone, and a point's empty blob has none.
        assert!(are_vertices_of(&[], &logical_blob(2, bmin, &[])));
        assert_eq!((vertices_of(&logical_blob(2, bmin, &[])), vertices_of(&[])), (0, 0));
        assert!(!are_vertices_of(&[], &[]));
    }

    #[test]
    fn a_footprints_reads_fail_that_give_nothing_or_are_not_its_logical_blobs() {
        let bmin = (10.0, 20.0);
        let steps = [(0, 0), (30, 0), (30, 30), (0, 0)];
        let logical = logical_blob(2, bmin, &steps);
        let vertices = stepped(bmin, 1e-6, &steps);
        assert_eq!(footprint_tally(true, &vertices, true, &logical, 4), tally(0, 0));
        // Each read that gives nothing, and all three.
        assert_eq!(footprint_tally(false, &[], true, &logical, 4), tally(1, 0));
        assert_eq!(footprint_tally(true, &vertices, false, &[], 4), tally(1, 0));
        assert_eq!(footprint_tally(true, &vertices, true, &logical, 0), tally(1, 0));
        assert_eq!(footprint_tally(false, &[], false, &[], 0), tally(3, 0));
        // Vertices that are not the logical blob's, a count that is not, and both.
        assert_eq!(footprint_tally(true, &vertices[..3], true, &logical, 4), tally(0, 1));
        assert_eq!(footprint_tally(true, &vertices, true, &logical, 5), tally(0, 1));
        assert_eq!(footprint_tally(true, &vertices[..3], true, &logical, 3), tally(0, 2));
    }

    #[test]
    fn an_offset_is_what_is_before_a_record_up_to_the_most_it_counts() {
        assert_eq!(offset("street_nodes.bin", 0, 0, MAX_NODES_BEFORE, 1), Ok(0));
        assert_eq!(offset("street_nodes.bin", 4_000_000_000, 0, MAX_NODES_BEFORE, 1), Ok(4_000_000_000));
        assert_eq!(offset("street_nodes.bin", u64::from(u32::MAX), 0, MAX_NODES_BEFORE, 1), Ok(u32::MAX));
        assert_eq!(
            offset("street_nodes.bin", u64::from(u32::MAX) + 1, 0, MAX_NODES_BEFORE, 1),
            Err("street_nodes.bin holds more than the 4294967295 a logical record's offset counts".to_owned())
        );
        // A footprint's bytes end no later than the most a patch indexes, and its offset counts
        // the bytes before it in units of 2.
        let (most, unit) = (addr_files::MAX_LOGICAL_VERTEX_BYTES, addr_files::VERTEX_OFFSET_UNIT);
        assert_eq!(offset("addr_vertices.bin", 8_589_934_000, 588, most, unit), Ok(4_294_967_000));
        assert_eq!(
            offset("addr_vertices.bin", 8_589_934_000, 590, most, unit),
            Err("addr_vertices.bin holds more than the 8589934588 a logical record's offset counts".to_owned())
        );
    }

    #[test]
    fn a_tally_is_printed_as_its_two_counts_and_adds_up() {
        let mut all = tally(1, 2);
        all.add(tally(10, 20));
        assert_eq!(all, tally(11, 22));
        assert_eq!(all.to_string(), "11 reads gave nothing, 22 disagreed");
    }
}
