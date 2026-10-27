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

/// `get` within `buf`: None for a varint cut short by the end of `buf` or over 64 bits.
pub fn try_get(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    let mut shift = 0;
    loop {
        let b = *buf.get(*pos)?;
        *pos += 1;
        v |= ((b & 0x7F) as u64).checked_shl(shift)?;
        if b < 0x80 {
            return Some(v);
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

    #[test]
    fn try_get_stops_at_the_end_of_its_buffer() {
        let mut b = Vec::new();
        put(&mut b, 300);
        let (mut at, mut cut) = (0, 0);
        assert_eq!(try_get(&b, &mut at), Some(300));
        assert_eq!(try_get(&b[..1], &mut cut), None);
        assert_eq!(try_get(&[0x80; 11], &mut 0), None);
    }
}
