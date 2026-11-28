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

/// Reads one unsigned 32-bit LEB128 from a buffered stream without reading ahead into the next value.
pub(crate) fn read_u32(r: &mut impl std::io::Read) -> std::io::Result<u32> {
    let mut value = 0;
    for shift in (0..u32::BITS).step_by(7) {
        let mut byte = [0];
        r.read_exact(&mut byte)?;
        let byte = u32::from(byte[0]);
        if byte > u32::MAX >> shift {
            break;
        }
        value |= (byte & 0x7f) << shift;
        if byte < 0x80 {
            return Ok(value);
        }
    }
    Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "varint exceeds u32"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streamed_values_stop_at_the_next_value_and_reject_overflow() {
        let mut bytes = Vec::new();
        put(&mut bytes, u64::from(u32::MAX));
        put(&mut bytes, 0);
        let mut reader = bytes.as_slice();
        assert_eq!(read_u32(&mut reader).unwrap(), u32::MAX);
        assert_eq!(read_u32(&mut reader).unwrap(), 0);
        assert_eq!(read_u32(&mut &[0x80][..]).unwrap_err().kind(), std::io::ErrorKind::UnexpectedEof);
        assert_eq!(read_u32(&mut &[0xff; 5][..]).unwrap_err().kind(), std::io::ErrorKind::InvalidData);
    }

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
