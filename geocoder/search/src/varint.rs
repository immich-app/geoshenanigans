//! LEB128 unsigned varints (postings, prefix table, extra names).

pub fn put(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

pub fn get(buf: &[u8], pos: &mut usize) -> u64 {
    let mut v = 0u64;
    let mut shift = 0;
    loop {
        let b = buf[*pos];
        *pos += 1;
        v |= ((b & 0x7F) as u64) << shift;
        if b < 0x80 {
            return v;
        }
        shift += 7;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let mut b = Vec::new();
        for v in [0u64, 1, 127, 128, 300, 1 << 40, u64::MAX] {
            put(&mut b, v);
        }
        let mut p = 0;
        for v in [0u64, 1, 127, 128, 300, 1 << 40, u64::MAX] {
            assert_eq!(get(&b, &mut p), v);
        }
        assert_eq!(p, b.len());
    }
}
