"""One-off source patch: the prefix table streams to one sorted temp file per prefix length (each
length flushes in key order) and is merged at the end, instead of being held in memory."""
import sys

D = sys.argv[1]
p = D + "/build.rs"
s = open(p).read()


def rep(old, new):
    global s
    assert s.count(old) == 1, old[:100]
    s = s.replace(old, new)


rep('''struct PrefixSlot {
    prefix: String,
    chars: usize,
    keys: usize,
    hits: Vec<(f32, PrefixHit)>,
}''', '''struct PrefixSlot {
    prefix: String,
    chars: usize,
    keys: usize,
    hits: Vec<(f32, PrefixHit)>,
    /// flushed prefixes of this length, in key order: u16 length, prefix, u32 length, hit blob
    out: BufWriter<File>,
    path: PathBuf,
    flushed: u64,
}''')
rep('''    slots: Vec<PrefixSlot>,
    table: Vec<(String, Vec<PrefixHit>)>,''', '''    slots: Vec<PrefixSlot>,''')
rep('''                let slot = &mut self.slots[n - 1];
                flush_slot(&mut self.table, slot);
                slot.prefix = pfx.clone();''', '''                let slot = &mut self.slots[n - 1];
                flush_slot(slot, self.with_names).expect("prefix temp file");
                slot.prefix = pfx.clone();''')
i = s.index("fn flush_slot(")
j = s.index("fn vm_hwm_kb()")
s = s[:i] + '''fn flush_slot(slot: &mut PrefixSlot, with_names: bool) -> std::io::Result<()> {
    let mut v = std::mem::take(&mut slot.hits);
    let keys = std::mem::take(&mut slot.keys);
    if slot.prefix.is_empty() || (slot.chars > PREFIX_TABLE_ALL_CHARS && keys < HEAVY_PREFIX_MIN_KEYS) {
        return Ok(());
    }
    v.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.entity.cmp(&b.1.entity)));
    let mut seen = HashSet::new();
    let hits: Vec<PrefixHit> = v.into_iter().filter(|(_, h)| seen.insert(h.entity)).take(PREFIX_TABLE_TOP).map(|x| x.1).collect();
    let mut blob = Vec::new();
    encode_prefix_hits(&mut blob, &hits, with_names);
    slot.out.write_all(&(slot.prefix.len() as u16).to_le_bytes())?;
    slot.out.write_all(slot.prefix.as_bytes())?;
    slot.out.write_all(&(blob.len() as u32).to_le_bytes())?;
    slot.out.write_all(&blob)?;
    slot.flushed += 1;
    Ok(())
}

/// Next (prefix, blob) of one per-length prefix file.
fn read_prefix(r: &mut BufReader<File>) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut l2 = [0u8; 2];
    r.read_exact(&mut l2).ok()?;
    let mut k = vec![0u8; u16::from_le_bytes(l2) as usize];
    r.read_exact(&mut k).ok()?;
    let mut l4 = [0u8; 4];
    r.read_exact(&mut l4).ok()?;
    let mut b = vec![0u8; u32::from_le_bytes(l4) as usize];
    r.read_exact(&mut b).ok()?;
    Some((k, b))
}

''' + s[j:]
rep('''        slots: (1..=PREFIX_TABLE_MAX_CHARS).map(|chars| PrefixSlot { prefix: String::new(), chars, keys: 0, hits: Vec::new() }).collect(),
        table: Vec::new(),''', '''        slots: (1..=PREFIX_TABLE_MAX_CHARS)
            .map(|chars| -> Result<PrefixSlot, String> {
                let path = opts.tmp.join(format!("{name}-pfx{chars}.tmp"));
                let out = BufWriter::new(File::create(&path).map_err(|e| e.to_string())?);
                Ok(PrefixSlot { prefix: String::new(), chars, keys: 0, hits: Vec::new(), out, path, flushed: 0 })
            })
            .collect::<Result<Vec<_>, String>>()?,''')
i = s.index("    let Writer { fst, mut post, mut slots, mut table, keys, postings, inline_keys, longest, .. } = w;")
j = s.index("    pf.finish().map_err(|e| e.to_string())?;")
s = s[:i] + '''    let Writer { fst, mut post, mut slots, keys, postings, inline_keys, longest, with_names, .. } = w;
    fst.finish().map_err(|e| e.to_string())?;
    post.flush().map_err(|e| e.to_string())?;
    let mut n_prefixes = 0u64;
    let mut pfx_readers = Vec::new();
    for sl in slots.iter_mut() {
        flush_slot(sl, with_names).map_err(|e| e.to_string())?;
        sl.out.flush().map_err(|e| e.to_string())?;
        n_prefixes += sl.flushed;
        pfx_readers.push(BufReader::new(File::open(&sl.path).map_err(|e| e.to_string())?));
    }
    // Merge the per-length files: each is sorted, and a prefix occurs in one length only.
    let mut heads: Vec<Option<(Vec<u8>, Vec<u8>)>> = pfx_readers.iter_mut().map(read_prefix).collect();
    let mut pf = fst::MapBuilder::new(BufWriter::new(mk("pfx.fst")?)).map_err(|e| e.to_string())?;
    let mut pb = BufWriter::new(mk("pfx")?);
    let mut off = 0u64;
    loop {
        let Some(i) = (0..heads.len()).filter(|&i| heads[i].is_some()).min_by(|&a, &b| heads[a].as_ref().unwrap().0.cmp(&heads[b].as_ref().unwrap().0)) else {
            break;
        };
        let (k, blob) = heads[i].take().unwrap();
        pf.insert(&k, off).map_err(|e| e.to_string())?;
        pb.write_all(&blob).map_err(|e| e.to_string())?;
        off += blob.len() as u64;
        heads[i] = read_prefix(&mut pfx_readers[i]);
    }
    for sl in &slots {
        let _ = std::fs::remove_file(&sl.path);
    }
''' + s[j:]
rep('''    st.add("prefix table prefixes", table.len());''', '''    st.add("prefix table prefixes", n_prefixes);''')
open(p, "w").write(s)
print("ok")
