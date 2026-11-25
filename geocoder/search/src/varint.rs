//! LEB128 unsigned varints (postings, prefix table, extra names).

pub fn put(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// The varint at `*pos`, read within `buf`, moving past it: None for one cut short by the end of
/// `buf` or over 64 bits (a tenth byte of more than the 64th bit).
pub fn try_get(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    let mut shift = 0;
    loop {
        let b = *buf.get(*pos)?;
        *pos += 1;
        if shift == 63 && b > 1 {
            return None;
        }
        // The shift stays below 64: a tenth byte that gets here is 0 or 1, the varint's last.
        v |= ((b & 0x7F) as u64) << shift;
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
            assert_eq!(try_get(&b, &mut p), Some(v));
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

    #[test]
    fn try_get_refuses_a_value_past_64_bits() {
        let tenth = |b: u8| [[0xFF; 9].as_slice(), &[b]].concat();
        // The tenth byte holds the 64th bit alone.
        assert_eq!(try_get(&tenth(0x01), &mut 0), Some(u64::MAX));
        assert_eq!(try_get(&[[0x80; 9].as_slice(), &[0x01]].concat(), &mut 0), Some(1 << 63));
        assert_eq!(try_get(&tenth(0x00), &mut 0), Some(u64::MAX >> 1));
        // More than that is no value, whatever its low bits.
        for b in [0x02, 0x03, 0x7F, 0x81] {
            assert_eq!(try_get(&[tenth(b), vec![0x00]].concat(), &mut 0), None, "{b:#x}");
        }
    }
}
