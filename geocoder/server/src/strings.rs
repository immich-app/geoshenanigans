//! The string tiers as they sit on disk: FCC2, front-coded buckets cut by content
//! (builder/src/string_files.h writes them and documents the format). A string id stays a byte
//! offset into its tier's pool, the strings back to back, each ended by a NUL. [`Tier::get`] finds
//! the bucket that holds an offset through the pool and body offsets of every 16th bucket, summed
//! from the bucket table once at open, then walks that bucket. A string the bucket stores whole (its
//! first, or one sharing nothing with the string before) is borrowed from the file; any other is
//! rebuilt into a `String`.

use std::borrow::Cow;

use crate::FileBytes;

const MAGIC: &[u8; 4] = b"FCC2";
const FOOTER: usize = 12;
const ENTRY: usize = 4;
/// Buckets per superblock: the reader keeps the offsets of every SB-th bucket.
const SB: usize = 16;
/// A bucket of two strings or more spans at most this, so each of its strings fits.
const MAX_SPAN: usize = 512;

/// One tier's file, read in place.
pub struct Tier {
    file: FileBytes,
    raw_size: u32,
    /// Where the bucket table starts: the body's length.
    body: usize,
    /// Per superblock, the pool offset of its first bucket and that bucket's offset in the body.
    raw_starts: Vec<u32>,
    coded_starts: Vec<u32>,
}

fn u16_at(f: &[u8], at: usize) -> usize {
    u16::from_le_bytes([f[at], f[at + 1]]) as usize
}

fn u32_at(f: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([f[at], f[at + 1], f[at + 2], f[at + 3]])
}

#[inline]
fn varint(d: &[u8], at: &mut usize) -> Option<usize> {
    let mut v = 0usize;
    for s in [0, 7, 14] {
        let b = *d.get(*at)?;
        *at += 1;
        v |= ((b & 0x7F) as usize) << s;
        if b & 0x80 == 0 {
            return Some(v);
        }
    }
    None
}

fn borrowed(b: &[u8]) -> Option<Cow<'_, str>> {
    std::str::from_utf8(b).ok().map(Cow::Borrowed)
}

impl Tier {
    /// Checks the footer and sums the bucket table: refuses a file whose table doesn't add up to
    /// its body and pool.
    pub fn open(file: FileBytes) -> Result<Tier, String> {
        let f = file.as_slice();
        if f.len() < FOOTER {
            return Err(format!("a file of {} bytes, shorter than its footer", f.len()));
        }
        let ft = &f[f.len() - FOOTER..];
        if &ft[8..12] != MAGIC {
            return Err("no FCC2 footer".into());
        }
        let buckets = u32_at(ft, 0) as u64;
        let raw_size = u32_at(ft, 4);
        let table = buckets * ENTRY as u64;
        if table > (f.len() - FOOTER) as u64 {
            return Err(format!("a table of {buckets} buckets past the file's start"));
        }
        let body = f.len() - FOOTER - table as usize;
        if body > u32::MAX as usize {
            return Err(format!("a body of {body} bytes"));
        }
        let n = (buckets as usize).div_ceil(SB);
        let (mut raw_starts, mut coded_starts) = (Vec::with_capacity(n), Vec::with_capacity(n));
        let (mut raw, mut coded) = (0u64, 0u64);
        for (b, e) in f[body..body + table as usize].chunks_exact(ENTRY).enumerate() {
            if b % SB == 0 {
                raw_starts.push(raw as u32);
                coded_starts.push(coded as u32);
            }
            let (span, size) = (u16_at(e, 0) as u64, u16_at(e, 2) as u64);
            if span == 0 || size == 0 {
                return Err(format!("bucket {b} is empty"));
            }
            raw += span;
            coded += size;
        }
        if raw != raw_size as u64 || coded != body as u64 {
            return Err(format!(
                "buckets of {raw} bytes in {coded} where the footer gives {raw_size} in a body of {body}"
            ));
        }
        Ok(Tier { file, raw_size, body, raw_starts, coded_starts })
    }

    /// The pool's bytes.
    pub fn len(&self) -> u32 {
        self.raw_size
    }

    pub fn is_empty(&self) -> bool {
        self.raw_size == 0
    }

    /// The string at `local` in the pool, from there to its NUL; "" past the pool, and for a bucket
    /// that doesn't decode or a string that isn't UTF-8.
    #[inline]
    pub fn get(&self, local: u32) -> Cow<'_, str> {
        self.find(local).unwrap_or(Cow::Borrowed(""))
    }

    fn find(&self, local: u32) -> Option<Cow<'_, str>> {
        if local >= self.raw_size {
            return None;
        }
        let f = self.file.as_slice();
        let s = self.raw_starts.partition_point(|&r| r <= local) - 1;
        let (mut raw, mut coded) = (self.raw_starts[s] as usize, self.coded_starts[s] as usize);
        let mut e = self.body + s * SB * ENTRY;
        let local = local as usize;
        loop {
            let span = u16_at(f, e);
            if local < raw + span {
                break;
            }
            raw += span;
            coded += u16_at(f, e + 2);
            e += ENTRY;
        }
        let d = f.get(coded..coded + u16_at(f, e + 2))?;
        walk(d, local - raw)
    }
}

/// The string at `target` in a bucket's strings (`d` its coded bytes).
fn walk(d: &[u8], target: usize) -> Option<Cow<'_, str>> {
    let mut at = 0;
    let len = varint(d, &mut at)?;
    let head = d.get(at..at + len)?;
    if target <= len {
        return borrowed(&head[target..]);
    }
    // The string the walk is at: whole in d at (start, len), or built in buf's first len bytes.
    enum Cur {
        Whole(usize, usize),
        Built(usize),
    }
    let mut cur = Cur::Whole(at, len);
    at += len;
    let mut pos = len + 1;
    let mut buf = [0u8; MAX_SPAN];
    loop {
        let lcp = varint(d, &mut at)?;
        let sl = varint(d, &mut at)?;
        let suffix = d.get(at..at + sl)?;
        let l = lcp + sl;
        if lcp == 0 {
            cur = Cur::Whole(at, sl);
        } else {
            if l > MAX_SPAN {
                return None;
            }
            match cur {
                Cur::Whole(s, n) if lcp <= n => buf[..lcp].copy_from_slice(&d[s..s + lcp]),
                Cur::Built(n) if lcp <= n => {}
                _ => return None,
            }
            buf[lcp..l].copy_from_slice(suffix);
            cur = Cur::Built(l);
        }
        at += sl;
        if target < pos + l + 1 {
            let k = target - pos;
            return match cur {
                Cur::Whole(s, n) => borrowed(&d[s + k..s + n]),
                Cur::Built(n) => std::str::from_utf8(&buf[k..n]).ok().map(|s| Cow::Owned(s.to_owned())),
            };
        }
        pos += l + 1;
    }
}

/// A pool's FCC2 file, as the builder writes it (string_files.h's Encoder), for fixtures. Panics on
/// a pool the format can't hold.
#[cfg(any(test, feature = "fixtures"))]
pub fn encode(pool: &[u8]) -> Vec<u8> {
    fn fnv1a(s: &[u8]) -> u64 {
        s.iter().fold(1469598103934665603u64, |h, &b| (h ^ b as u64).wrapping_mul(1099511628211))
    }
    fn put_varint(o: &mut Vec<u8>, mut v: usize) {
        while v >= 0x80 {
            o.push(v as u8 | 0x80);
            v >>= 7;
        }
        o.push(v as u8);
    }
    assert!(pool.is_empty() || pool.last() == Some(&0), "a pool whose last string has no NUL");
    let (mut body, mut table) = (Vec::new(), Vec::new());
    let (mut bucket, mut span, mut buckets) = (Vec::new(), 0usize, 0u32);
    let mut prev: &[u8] = &[];
    let mut flush = |bucket: &mut Vec<u8>, span: &mut usize, body: &mut Vec<u8>, table: &mut Vec<u8>| {
        if *span == 0 {
            return;
        }
        table.extend_from_slice(&(*span as u16).to_le_bytes());
        table.extend_from_slice(&(bucket.len() as u16).to_le_bytes());
        body.append(bucket);
        *span = 0;
        buckets += 1;
    };
    for s in pool.split(|&b| b == 0).take(pool.iter().filter(|&&b| b == 0).count()) {
        assert!(s.len() <= 65532, "a string of {} bytes", s.len());
        let cut = span + s.len() + 1 > MAX_SPAN || (span >= 128 && (fnv1a(s) >> 32) % 8 == 0);
        if span > 0 && cut {
            flush(&mut bucket, &mut span, &mut body, &mut table);
        }
        let mut lcp = 0;
        if span > 0 {
            while lcp < s.len().min(prev.len()) && s[lcp] == prev[lcp] {
                lcp += 1;
            }
            put_varint(&mut bucket, lcp);
        }
        put_varint(&mut bucket, s.len() - lcp);
        bucket.extend_from_slice(&s[lcp..]);
        prev = s;
        span += s.len() + 1;
    }
    flush(&mut bucket, &mut span, &mut body, &mut table);
    body.extend_from_slice(&table);
    body.extend_from_slice(&buckets.to_le_bytes());
    body.extend_from_slice(&(pool.len() as u32).to_le_bytes());
    body.extend_from_slice(MAGIC);
    body
}

#[cfg(any(test, feature = "fixtures"))]
impl Tier {
    /// A pool's tier, encoded in memory (fixtures).
    pub fn of_pool(pool: &[u8]) -> Tier {
        Tier::open(FileBytes::Owned(encode(pool))).expect("an encoded pool opens")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool_of(strings: &[&[u8]]) -> Vec<u8> {
        strings.iter().flat_map(|s| s.iter().copied().chain([0])).collect()
    }

    fn hex(h: &str) -> Vec<u8> {
        (0..h.len()).step_by(2).map(|i| u8::from_str_radix(&h[i..i + 2], 16).unwrap()).collect()
    }

    /// builder/tests/test_string_files.cpp's small pool and the bytes the format gives it.
    fn small_pool() -> (Vec<u8>, Vec<u8>) {
        let mut strings: Vec<Vec<u8>> = vec![Vec::new()];
        strings.extend((0..32).map(|i| format!("name {i:02}").into_bytes()));
        strings.extend([b"Berlin".to_vec(), b"Paris".to_vec(), vec![b'x'; 520], b"b".to_vec()]);
        let pool = pool_of(&strings.iter().map(|s| s.as_slice()).collect::<Vec<_>>());
        let mut file = hex(concat!(
            "0000076e616d6520303006013106013206013306013406013506013606013706013806013905023130060131060132060133060134060135",
            "076e616d65203136060137060138060139050232300601310601320601330601340601350601360601370601380601390502333006013100",
            "064265726c696e00055061726973",
            "8804"
        ));
        file.extend([b'x'; 520]);
        file.extend(hex("0162810038008d00460009020a0202000200040000001903000046434332"));
        (pool, file)
    }

    /// Every offset of the pool reads as today's raw reader read it: the bytes from there to the
    /// next NUL ("" for invalid UTF-8), and "" past the pool.
    fn raw_get(pool: &[u8], off: usize) -> &str {
        if off >= pool.len() {
            return "";
        }
        let b = &pool[off..];
        std::str::from_utf8(&b[..b.iter().position(|&c| c == 0).unwrap_or(b.len())]).unwrap_or("")
    }

    #[test]
    fn reads_the_small_pool_the_builder_writes() {
        let (pool, file) = small_pool();
        assert_eq!(encode(&pool), file);
        let t = Tier::open(FileBytes::Owned(file)).unwrap();
        assert_eq!(t.len() as usize, pool.len());
        for off in 0..pool.len() + 3 {
            assert_eq!(t.get(off as u32), raw_get(&pool, off), "offset {off}");
        }
        assert_eq!(t.get(1), "name 00");
        assert_eq!(t.get(1 + 8 * 16), "name 16");
        assert!(matches!(t.get(1 + 8 * 16), Cow::Borrowed(_)), "a bucket's first string is borrowed");
        assert!(matches!(t.get(1 + 8 * 17), Cow::Owned(_)), "a front-coded string is built");
    }

    #[test]
    fn every_string_id_reads_as_the_raw_tier() {
        // Random pools of names sharing prefixes, empty strings, UTF-8 and invalid bytes, long
        // strings alone in their buckets, sorted or not: every string start, and offsets inside the
        // strings and at their NULs, read as the raw tier read them.
        let mut x = 0x2545F4914F6CDD1Du64;
        let mut rng = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for round in 0..40 {
            let n = if round < 3 { round } else { 1 + rng() as usize % 3000 };
            let mut strings: Vec<Vec<u8>> = Vec::new();
            let mut stem = b"Rue".to_vec();
            for _ in 0..n {
                let r = rng() % 100;
                let s = if r < 5 {
                    Vec::new()
                } else if r < 7 {
                    vec![b'a' + (rng() % 26) as u8; if r == 5 { 65532 - rng() as usize % 3 } else { 400 + rng() as usize % 700 }]
                } else {
                    if rng() % 8 == 0 {
                        stem = format!("{}{}", (b'A' + (rng() % 26) as u8) as char, rng() % 1000).into_bytes();
                    }
                    let mut s = stem.clone();
                    s.extend(format!(" {}", rng() % if r < 50 { 10 } else { 100000 }).bytes());
                    match rng() % 32 {
                        0 => s.extend("é".bytes()),
                        1 => s.push(0xFF),
                        _ => {}
                    }
                    s
                };
                strings.push(s);
            }
            if round % 3 == 1 {
                strings.sort();
            }
            let pool = pool_of(&strings.iter().map(|s| s.as_slice()).collect::<Vec<_>>());
            let t = Tier::of_pool(&pool);
            // Each string's every offset up to its NUL; in a long one (each of whose offsets
            // reads to its end) its first two, middle and last two.
            let mut offsets = vec![pool.len(), pool.len() + 1];
            let mut start = 0;
            for (end, _) in pool.iter().enumerate().filter(|(_, &c)| c == 0) {
                if end - start < 1024 {
                    offsets.extend(start..=end);
                } else {
                    offsets.extend([start, start + 1, (start + end) / 2, end - 1, end]);
                }
                start = end + 1;
            }
            let mut wrong = 0;
            for &off in &offsets {
                wrong += (t.get(off as u32) != raw_get(&pool, off)) as usize;
            }
            assert_eq!(wrong, 0, "round {round}: {wrong} offsets of {} read wrong", pool.len());
        }
    }

    #[test]
    fn refuses_files_whose_table_does_not_add_up() {
        let open = |f: Vec<u8>| Tier::open(FileBytes::Owned(f)).err().unwrap_or_default();
        let good = encode(&pool_of(&[b"ab", b"abc"]));
        assert_eq!(open(good.clone()), "");
        assert_eq!(open(good[..11].to_vec()), "a file of 11 bytes, shorter than its footer");
        let mut magic = good.clone();
        *magic.last_mut().unwrap() = b'3';
        assert_eq!(open(magic), "no FCC2 footer");
        let at = good.len() - FOOTER;
        let with = |k: usize, v: u32| {
            let mut f = good.clone();
            f[k..k + 4].copy_from_slice(&v.to_le_bytes());
            f
        };
        assert_eq!(open(with(at, 9)), "a table of 9 buckets past the file's start");
        assert_eq!(open(with(at + 4, 8)), "buckets of 7 bytes in 6 where the footer gives 8 in a body of 6");
        let mut empty = good.clone();
        empty[6..8].copy_from_slice(&[0, 0]);
        assert_eq!(open(empty), "bucket 0 is empty");
        let mut longer = good.clone();
        longer.insert(0, 1);
        assert_eq!(open(longer), "buckets of 7 bytes in 6 where the footer gives 7 in a body of 7");
        // An empty pool is its footer alone.
        let none = encode(b"");
        assert_eq!(none, hex("000000000000000046434332"));
        let t = Tier::open(FileBytes::Owned(none)).unwrap();
        assert_eq!((t.len(), t.get(0)), (0, Cow::Borrowed("")));
    }

    #[test]
    fn a_bucket_that_does_not_decode_reads_as_empty() {
        // The table adds up, but the bucket's bytes don't: a varint past its end, a suffix past it,
        // and an lcp past the string before. The bucket's first string reads; every offset past
        // its NUL, which the walk reaches only through the bad bytes, reads as "".
        for (bucket, first) in [(&[0x02, b'a', b'b', 0x80][..], "ab"), (&[0x01, b'a', 0x00, 0x05], "a"), (&[0x01, b'a', 0x03, 0x00], "a")] {
            let mut f = bucket.to_vec();
            f.extend_from_slice(&[6, 0, bucket.len() as u8, 0]);
            f.extend_from_slice(&1u32.to_le_bytes());
            f.extend_from_slice(&6u32.to_le_bytes());
            f.extend_from_slice(MAGIC);
            let t = Tier::open(FileBytes::Owned(f)).unwrap();
            assert_eq!(t.get(0), first);
            for off in first.len() as u32 + 1..6 {
                assert_eq!(t.get(off), "", "bucket {bucket:02x?}, offset {off}");
            }
        }
    }
}
