//! The keys an f32 coordinate is stored under and the bit fields, zigzags and varints record
//! files are packed from, as the builder's functions give them (builder/src/coord_keys.h, which
//! says what each is). geocoder/testdata/coord_keys.tsv holds values both must give.
//!
//! The hybrid and decimal keys are one multiply and one rounding to an integer, their inverses
//! one divide and one narrowing to f32, each rounded once, to nearest even. Rust rounds so on
//! every target but x86 without SSE2, where the x87 rounds twice.

#[cfg(all(target_arch = "x86", not(target_feature = "sse2")))]
compile_error!("coord_keys: an x87-only build rounds the hybrid key twice");

/// An f32 bit pattern's place in the floats' order (-0 before +0), and back.
#[inline]
pub fn order_key(bits: u32) -> u32 {
    if bits & 0x8000_0000 != 0 { !bits } else { bits | 0x8000_0000 }
}
#[inline]
pub fn from_order_key(key: u32) -> u32 {
    if key & 0x8000_0000 != 0 { key & 0x7FFF_FFFF } else { !key }
}

/// 1.0f32's bits, and the hybrid key's magnitude there.
pub const ONE_BITS: u32 = 0x3F80_0000;
pub const TEN_MILLION: u32 = 10_000_000;
/// The largest magnitude of a hybrid key: 0x7FFFFFFF's.
pub const HYBRID_KEY_MAX: u32 = 0x7FFF_FFFF - ONE_BITS + TEN_MILLION;

/// Whether some pattern has this hybrid key.
#[inline]
pub fn is_hybrid_key(key: u32) -> bool {
    (if key >> 31 != 0 { key.wrapping_neg() } else { key }) <= HYBRID_KEY_MAX
}

/// The pattern of a hybrid key. Any 32 bits may come in: what is no key (`is_hybrid_key`) gives
/// some pattern that has another key or none.
#[inline]
pub fn from_hybrid_key(key: u32) -> u32 {
    let sign = key & 0x8000_0000;
    let mag = if sign != 0 { key.wrapping_neg() } else { key };
    if mag >= TEN_MILLION {
        return mag.wrapping_sub(TEN_MILLION).wrapping_add(ONE_BITS) | sign;
    }
    ((mag as f64 / 1e7) as f32).to_bits() | sign
}

/// The hybrid key of a bit pattern; None when it has none.
#[inline]
pub fn hybrid_key(bits: u32) -> Option<u32> {
    let sign = bits & 0x8000_0000;
    let a = bits ^ sign;
    let mag = if a >= ONE_BITS {
        a - ONE_BITS + TEN_MILLION
    } else {
        let n = (f32::from_bits(a) as f64 * 1e7).round_ties_even();
        if n >= TEN_MILLION as f64 {
            return None;
        }
        n as u32
    };
    if sign != 0 && mag == 0 {
        return None;
    }
    let key = if sign != 0 { mag.wrapping_neg() } else { mag };
    (from_hybrid_key(key) == bits).then_some(key)
}

/// A hybrid key, a signed number, as the unsigned number that sorts the same way; and back, by
/// the same flip.
#[inline]
pub fn biased(key: u32) -> u32 {
    key ^ 0x8000_0000
}

/// The units of a decimal key: 1e-4.
pub const DEC4_SCALE: f64 = 1e4;

/// The pattern of a decimal key. Any i32 may come in: what is no key gives some pattern that has
/// another key or none.
#[inline]
pub fn from_dec4_key(key: i32) -> u32 {
    ((key as f64 / DEC4_SCALE) as f32).to_bits()
}

/// A pattern's decimal round: its float in units of 1e-4, rounded half to even, saturated to
/// i32; 0 for a NaN (as `as` converts).
#[inline]
pub fn dec4_round(bits: u32) -> i32 {
    (f32::from_bits(bits) as f64 * DEC4_SCALE).round_ties_even() as i32
}

/// The decimal key of a bit pattern; None when it has none.
#[inline]
pub fn dec4_key(bits: u32) -> Option<i32> {
    let n = (f32::from_bits(bits) as f64 * DEC4_SCALE).round_ties_even();
    if !(n.abs() < 2_147_483_648.0) {
        return None;
    }
    let key = n as i32;
    (from_dec4_key(key) == bits).then_some(key)
}

/// A difference mod 2^32 with its sign moved to bit 0, and back.
#[inline]
pub fn zigzag32(d: u32) -> u32 {
    (d << 1) ^ (d >> 31).wrapping_neg()
}
#[inline]
pub fn unzigzag32(z: u32) -> u32 {
    (z >> 1) ^ (z & 1).wrapping_neg()
}
/// The same mod 2^16.
#[inline]
pub fn zigzag16(d: u16) -> u16 {
    (d << 1) ^ (d >> 15).wrapping_neg()
}
#[inline]
pub fn unzigzag16(z: u16) -> u16 {
    (z >> 1) ^ (z & 1).wrapping_neg()
}

/// The bits, and the bytes, of `v` without its leading zeros: 0 for 0.
#[inline]
pub fn bit_length(v: u32) -> u32 {
    32 - v.leading_zeros()
}
#[inline]
pub fn byte_length(v: u32) -> u32 {
    bit_length(v).div_ceil(8)
}

/// The `w` bits at bit `bit` of `bytes`, LSB first, with one 8-byte load at the field's first
/// byte. None for a `w` above 32 and where `bytes` does not hold 8 bytes from there.
#[inline(always)]
pub fn bits_at(bytes: &[u8], bit: usize, w: u32) -> Option<u32> {
    if w == 0 {
        return Some(0);
    }
    if w > 32 {
        return None;
    }
    let word = u64::from_le_bytes(*bytes.get(bit >> 3..)?.first_chunk::<8>()?);
    Some(((word >> (bit & 7)) & ((1u64 << w) - 1)) as u32)
}

pub const RICE_ESCAPE: u32 = 12;
pub const RICE_MAX_BITS: u32 = RICE_ESCAPE + 32;

/// The bits of z's Rice code with parameter `k` (at most 31).
#[inline]
pub fn rice_bits(z: u32, k: u32) -> u32 {
    let q = z >> k;
    if q < RICE_ESCAPE { q + 1 + k } else { RICE_MAX_BITS }
}

/// The Rice code with parameter `k` at bit `*bit` of `bytes`, which it moves past it: one 8-byte
/// load at the code's first byte. None for a `k` above 31 and where `bytes` does not hold 8 bytes
/// from there. Bits that are no z's code (a q past 32 - k bits) give some z whose code is other
/// bits.
#[inline(always)]
pub fn get_rice(bytes: &[u8], bit: &mut usize, k: u32) -> Option<u32> {
    if k > 31 {
        return None;
    }
    let word = u64::from_le_bytes(*bytes.get(*bit >> 3..)?.first_chunk::<8>()?) >> (*bit & 7);
    let low = word as u32 & ((1 << RICE_ESCAPE) - 1);
    if low == 0 {
        *bit += RICE_MAX_BITS as usize;
        return Some((word >> RICE_ESCAPE) as u32);
    }
    let q = low.trailing_zeros();
    *bit += (q + 1 + k) as usize;
    Some(q.wrapping_shl(k) | ((word >> (q + 1)) & ((1u64 << k) - 1)) as u32)
}

/// The least k (0 to 31) whose Rice codes of `z` take the fewest bits, each k tried; 0 for no z.
pub fn best_rice_k(z: &[u32]) -> u32 {
    (0..32).min_by_key(|&k| (z.iter().map(|&v| rice_bits(v, k) as u64).sum::<u64>(), k)).unwrap()
}

pub const VARINT_MAX_BYTES: usize = 10;

/// The LEB128 varint at `*at` of `bytes`, which it moves past it. None for one that is cut
/// short, longer than `max_bytes`, past 64 bits or not in its fewest bytes.
#[inline]
pub fn varint(bytes: &[u8], at: &mut usize, max_bytes: usize) -> Option<u64> {
    let mut v = 0u64;
    for i in 0..max_bytes.min(VARINT_MAX_BYTES) {
        let c = *bytes.get(*at)?;
        *at += 1;
        if i == VARINT_MAX_BYTES - 1 && c > 1 {
            return None;
        }
        v |= u64::from(c & 0x7F) << (7 * i);
        if c & 0x80 == 0 {
            return (i == 0 || c != 0).then_some(v);
        }
    }
    None
}

/// Bit fields packed LSB first, back to back (fixtures and tests).
#[cfg(any(test, feature = "fixtures"))]
#[derive(Default)]
pub struct BitWriter {
    bytes: Vec<u8>,
    acc: u64,
    n: u32,
}

#[cfg(any(test, feature = "fixtures"))]
impl BitWriter {
    /// Appends `v`, of `w` bits (at most 32; `v` below 2^w).
    pub fn put(&mut self, v: u32, w: u32) {
        assert!(w <= 32 && u64::from(v) >> w == 0, "{v:#x} is not {w} bits");
        self.acc |= u64::from(v) << self.n;
        self.n += w;
        while self.n >= 8 {
            self.bytes.push(self.acc as u8);
            self.acc >>= 8;
            self.n -= 8;
        }
    }

    /// The bytes, the last filled with zero bits.
    pub fn finish(mut self) -> Vec<u8> {
        if self.n > 0 {
            self.bytes.push(self.acc as u8);
        }
        self.bytes
    }
}

/// z's Rice code with parameter `k` (at most 31) appended to `w` (fixtures and tests).
#[cfg(any(test, feature = "fixtures"))]
pub fn put_rice(w: &mut BitWriter, z: u32, k: u32) {
    let q = z >> k;
    if q >= RICE_ESCAPE {
        w.put(0, RICE_ESCAPE);
        w.put(z, 32);
        return;
    }
    w.put(0, q);
    w.put(1, 1);
    w.put((u64::from(z) & ((1u64 << k) - 1)) as u32, k);
}

/// `v`'s varint appended to `out` (fixtures and tests).
#[cfg(any(test, feature = "fixtures"))]
pub fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_rng::Rng;

    const SIGN: u32 = 0x8000_0000;

    fn hex(bytes: &[u8]) -> String {
        if bytes.is_empty() {
            return "-".to_string();
        }
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn bytes_of(s: &str) -> Vec<u8> {
        if s == "-" {
            return Vec::new();
        }
        (0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap()).collect()
    }

    fn hex32(s: &str) -> u32 {
        u32::from_str_radix(s, 16).unwrap_or_else(|e| panic!("{s}: {e}"))
    }

    /// The fields (value, width) through a BitWriter.
    fn packed(fields: &[(u32, u32)]) -> Vec<u8> {
        let mut w = BitWriter::default();
        for &(v, width) in fields {
            w.put(v, width);
        }
        w.finish()
    }

    /// The fields a bit at a time, LSB first, the last byte's spare bits zero.
    fn packed_bit_by_bit(fields: &[(u32, u32)]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut bit = 0usize;
        for &(v, w) in fields {
            for k in 0..w {
                if bit % 8 == 0 {
                    out.push(0u8);
                }
                out[bit / 8] |= ((v >> k & 1) as u8) << (bit % 8);
                bit += 1;
            }
        }
        out
    }

    /// The varint that is all of `bytes`, read with a limit of `max_bytes`.
    fn whole_varint(bytes: &[u8], max_bytes: usize) -> Option<u64> {
        let mut at = 0;
        varint(bytes, &mut at, max_bytes).filter(|_| at == bytes.len())
    }

    #[test]
    fn order_keys_are_the_place_files_and_come_back() {
        let mut rng = Rng::new(1);
        let edges = [0, SIGN, 1, SIGN | 1, ONE_BITS, ONE_BITS | SIGN, 0x7F80_0000, 0xFF80_0000, 0x7FC0_0000, 0x7FFF_FFFF, u32::MAX];
        for t in 0..1_000_000 {
            let x = if t < edges.len() { edges[t] } else { rng.next() as u32 };
            assert_eq!(order_key(x), crate::place_files::order_key(x), "{x:#x}");
            assert_eq!(from_order_key(x), crate::place_files::from_order_key(x), "{x:#x}");
            assert_eq!(from_order_key(order_key(x)), x);
            assert_eq!(order_key(from_order_key(x)), x);
        }
        let (tiny, inf) = (f32::MIN_POSITIVE, f32::INFINITY);
        let floats = [-inf, f32::MIN, -180.0, -1.5, -tiny, -0.0, 0.0, tiny, 1.5, 180.0, f32::MAX, inf];
        for w in floats.windows(2) {
            assert!(order_key(w[0].to_bits()) < order_key(w[1].to_bits()), "{} < {}", w[0], w[1]);
        }
    }

    #[test]
    fn every_hybrid_magnitude_below_ten_million_is_one_floats_key_under_either_sign() {
        let mut last = 0;
        for mag in 0..TEN_MILLION {
            let bits = from_hybrid_key(mag);
            assert!(bits < ONE_BITS && (mag == 0 || bits > last), "{mag}");
            assert_eq!(hybrid_key(bits), Some(mag));
            last = bits;
            if mag != 0 {
                assert_eq!(from_hybrid_key(mag.wrapping_neg()), bits | SIGN);
                assert_eq!(hybrid_key(bits | SIGN), Some(mag.wrapping_neg()));
            }
        }
        assert_eq!(from_hybrid_key(1_234_567), 0.1234567f32.to_bits());
        assert_eq!(from_hybrid_key(5_000_000), 0.5f32.to_bits());
    }

    #[test]
    fn the_hybrid_key_exists_where_it_decodes_back() {
        assert_eq!(hybrid_key(0), Some(0));
        // -0 would share +0's key, and a float that rounds to 0 is not 0.
        assert_eq!(hybrid_key(SIGN), None);
        assert_eq!(hybrid_key(1), None);
        assert_eq!(hybrid_key((-1e-9f32).to_bits()), None);
        // 0.1f32 is the float nearest 1,000,000 x 1e-7; its neighbours are nearest no multiple.
        assert_eq!(hybrid_key(0.1f32.to_bits()), Some(1_000_000));
        assert_eq!(hybrid_key(0.1f32.to_bits() + 1), None);
        assert_eq!(hybrid_key((-0.1f32).to_bits()), Some(1_000_000u32.wrapping_neg()));
        // Halfway between two multiples.
        assert_eq!(hybrid_key((1.0f32 / 256.0).to_bits()), None);
        assert_eq!(hybrid_key((3.0f32 / 256.0).to_bits()), None);
        // The last float below 1.0 with a key, the one after it, and 1.0, from where the key
        // counts floats.
        assert_eq!(hybrid_key(ONE_BITS - 2), Some(TEN_MILLION - 1));
        assert_eq!(hybrid_key(ONE_BITS - 1), None);
        assert_eq!(hybrid_key(ONE_BITS), Some(TEN_MILLION));
        assert_eq!(hybrid_key(52.37f32.to_bits()), Some(52.37f32.to_bits() - ONE_BITS + TEN_MILLION));
        assert_eq!(hybrid_key((-1.0f32).to_bits()), Some(TEN_MILLION.wrapping_neg()));
        // Infinities and NaNs are patterns from 1.0 up, the largest 0x7FFFFFFF.
        for bits in [0x7F80_0000, 0xFF80_0000, 0x7FC0_0000, 0xFFC0_0000, 0x7F80_0001, 0x7FFF_FFFF, u32::MAX] {
            let key = hybrid_key(bits).unwrap();
            assert!(is_hybrid_key(key));
            assert_eq!(from_hybrid_key(key), bits);
        }
        assert_eq!(HYBRID_KEY_MAX, 1_092_130_431);
        assert_eq!(hybrid_key(0x7FFF_FFFF), Some(HYBRID_KEY_MAX));
        assert_eq!(hybrid_key(u32::MAX), Some(HYBRID_KEY_MAX.wrapping_neg()));
    }

    #[test]
    fn a_key_past_the_largest_magnitude_is_no_hybrid_key() {
        for key in [0, 1, u32::MAX, TEN_MILLION, TEN_MILLION.wrapping_neg(), HYBRID_KEY_MAX, HYBRID_KEY_MAX.wrapping_neg()] {
            assert!(is_hybrid_key(key), "{key:#x}");
        }
        for key in [HYBRID_KEY_MAX + 1, (HYBRID_KEY_MAX + 1).wrapping_neg(), 0x7FFF_FFFF, SIGN, SIGN + 1, 0x6000_0000, 0xA000_0000] {
            assert!(!is_hybrid_key(key), "{key:#x}");
            // Whatever pattern it decodes to has another key, or none.
            assert_ne!(hybrid_key(from_hybrid_key(key)), Some(key));
        }
        // No pattern's key is past it, and any 32 bits decode to some pattern.
        let mut rng = Rng::new(4);
        for _ in 0..1_000_000 {
            let x = rng.next() as u32;
            if let Some(key) = hybrid_key(x) {
                assert!(is_hybrid_key(key) && from_hybrid_key(key) == x, "{x:#x}");
            }
            assert_eq!(is_hybrid_key(x), hybrid_key(from_hybrid_key(x)) == Some(x), "{x:#x}");
        }
    }

    #[test]
    fn a_biased_hybrid_key_sorts_unsigned_as_the_key_does_signed() {
        let keys = [HYBRID_KEY_MAX.wrapping_neg(), TEN_MILLION.wrapping_neg(), u32::MAX, 0, 1, TEN_MILLION, HYBRID_KEY_MAX];
        for w in keys.windows(2) {
            assert!(biased(w[0]) < biased(w[1]));
        }
        for key in keys {
            assert_eq!(biased(biased(key)), key);
        }
    }

    #[test]
    fn zigzags_and_lengths_come_back_and_are_the_fewest_bits() {
        let mut rng = Rng::new(5);
        for _ in 0..1_000_000 {
            let d = rng.next() as u32 >> (1 + rng.next() % 31);
            let any = rng.next() as u32;
            assert_eq!(unzigzag32(zigzag32(any)), any);
            assert_eq!(zigzag32(unzigzag32(any)), any);
            if d != 0 {
                // A step of d up takes one bit more than d, a step of d down one more than d - 1.
                assert_eq!(bit_length(zigzag32(d)), bit_length(d) + 1);
                assert_eq!(bit_length(zigzag32(d.wrapping_neg())), bit_length(d - 1) + 1);
            }
        }
        for d in 0..=u16::MAX {
            let z = zigzag16(d);
            assert_eq!(z as u32, zigzag32(d as i16 as i32 as u32), "{d:#x}");
            assert_eq!(unzigzag16(z), d);
        }
        assert_eq!((bit_length(0), byte_length(0)), (0, 0));
        for w in 1..=32u32 {
            let top = 1u32 << (w - 1);
            assert_eq!((bit_length(top), bit_length(top | (top - 1))), (w, w));
            assert_eq!(byte_length(top), w.div_ceil(8));
        }
    }

    #[test]
    fn bit_fields_read_back_from_the_bytes_a_writer_packs() {
        let mut rng = Rng::new(6);
        for t in 0..3000 {
            let n = rng.next() % 80;
            let fields: Vec<(u32, u32)> = (0..n)
                .map(|_| {
                    let w = if t % 7 == 0 { 32 } else { (rng.next() % 33) as u32 };
                    let v = rng.next() as u32;
                    (if w == 32 { v } else { v & ((1 << w) - 1) }, w)
                })
                .collect();
            let bytes = packed(&fields);
            assert_eq!(bytes, packed_bit_by_bit(&fields));
            // In place, 8 bytes follow each field's first: what they hold does not show.
            let mut roomy = bytes.clone();
            roomy.extend([0xFF; 8]);
            let mut bit = 0;
            for &(v, w) in &fields {
                assert_eq!(bits_at(&roomy, bit, w), Some(v));
                bit += w as usize;
            }
        }
        // No load past the bytes, and no field wider than 32 bits.
        let nine = [0xFFu8; 9];
        assert_eq!(bits_at(&nine, 0, 32), Some(u32::MAX));
        assert_eq!(bits_at(&nine, 15, 9), Some(0x1FF));
        assert_eq!(bits_at(&nine, 16, 9), None);
        assert_eq!(bits_at(&nine, 16, 0), Some(0));
        assert_eq!(bits_at(&nine, usize::MAX, 1), None);
        assert_eq!(bits_at(&nine, 0, 33), None);
        assert_eq!(bits_at(&nine, 0, 64), None);
        assert_eq!(bits_at(&[], 0, 0), Some(0));
    }

    #[test]
    fn a_varint_is_read_only_in_its_fewest_bytes() {
        let mut rng = Rng::new(7);
        for t in 0..200_000u32 {
            let v = match t {
                0..=63 => 1u64 << t,
                64..=127 => (1u64 << (t - 64)) - 1,
                _ => rng.next() >> (rng.next() % 64),
            };
            let mut b = Vec::new();
            put_varint(&mut b, v);
            let n = b.len();
            assert_eq!(whole_varint(&b, VARINT_MAX_BYTES), Some(v));
            assert_eq!(whole_varint(&b, n), Some(v));
            // More bytes than the reader allows, a cut one, and one spelled with a zero byte more.
            assert_eq!(whole_varint(&b, n - 1), None);
            assert_eq!(varint(&b[..n - 1], &mut 0, VARINT_MAX_BYTES), None);
            let mut longer = b.clone();
            longer[n - 1] |= 0x80;
            longer.push(0);
            assert_eq!(whole_varint(&longer, VARINT_MAX_BYTES + 1), None);
            // It is read from where it starts and the bytes after it are left.
            let mut among = vec![0xFF, 0x80];
            among.extend(&b);
            among.push(0x80);
            let mut at = 2;
            assert_eq!(varint(&among, &mut at, VARINT_MAX_BYTES), Some(v));
            assert_eq!(at, 2 + n);
        }
        // 2^64 - 1 fills ten bytes; one bit more does not fit.
        assert_eq!(whole_varint(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01], 10), Some(u64::MAX));
        assert_eq!(whole_varint(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02], 10), None);
        assert_eq!(whole_varint(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01], 11), None);
        assert_eq!(whole_varint(&[], 10), None);
    }

    /// The codes (z, k) through a BitWriter.
    fn rice_packed(codes: &[(u32, u32)]) -> Vec<u8> {
        let mut w = BitWriter::default();
        for &(z, k) in codes {
            put_rice(&mut w, z, k);
        }
        w.finish()
    }

    /// The codes a bit at a time: q zero bits, a one bit and k bits of z, or RICE_ESCAPE zero
    /// bits and z in 32 bits.
    fn rice_bit_by_bit(codes: &[(u32, u32)]) -> Vec<u8> {
        let mut fields = Vec::new();
        for &(z, k) in codes {
            let q = z >> k;
            if q >= RICE_ESCAPE {
                fields.extend((0..RICE_ESCAPE).map(|_| (0, 1)));
                fields.push((z, 32));
                continue;
            }
            fields.extend((0..q).map(|_| (0, 1)));
            fields.push((1, 1));
            fields.push((if k == 0 { 0 } else { z & ((1 << k) - 1) }, k));
        }
        packed_bit_by_bit(&fields)
    }

    #[test]
    fn every_four_decimal_coordinate_to_180_is_one_floats_decimal_key() {
        let mut last = from_dec4_key(-1_800_001);
        for key in -1_800_000..=1_800_000 {
            let bits = from_dec4_key(key);
            assert_eq!(dec4_key(bits), Some(key));
            assert_eq!(dec4_round(bits), key);
            assert!(order_key(bits) > order_key(last), "{key}");
            last = bits;
        }
        assert_eq!(dec4_key(52.3702f32.to_bits()), Some(523_702));
        assert_eq!(dec4_key((-0.1278f32).to_bits()), Some(-1278));
        assert_eq!(dec4_key(180f32.to_bits()), Some(1_800_000));
        assert_eq!(dec4_key((-180f32).to_bits()), Some(-1_800_000));
        assert_eq!(dec4_key(1e-4f32.to_bits()), Some(1));
        assert_eq!(dec4_key((-1e-4f32).to_bits()), Some(-1));
        assert_eq!(dec4_key(0), Some(0));
    }

    #[test]
    fn a_float_that_is_no_four_decimal_coordinate_has_no_decimal_key() {
        for v in [52.3702f32, -0.1278, 1e-4, 180.0] {
            assert_eq!(dec4_key(v.to_bits() + 1), None, "{v}");
            assert_eq!(dec4_key(v.to_bits() - 1), None, "{v}");
        }
        assert_eq!(dec4_key(SIGN), None);
        assert_eq!(dec4_round(SIGN), 0);
        for bits in [0x7F80_0000, 0xFF80_0000, 0x7FC0_0000] {
            assert_eq!(dec4_key(bits), None);
        }
        assert_eq!(dec4_key(0.00005f32.to_bits()), None);
        assert_eq!(dec4_key(52.37025f32.to_bits()), None);
        // Halves of 1e-4 round to even.
        assert_eq!(dec4_key(0.03125f32.to_bits()), None);
        assert_eq!(dec4_round(0.03125f32.to_bits()), 312);
        assert_eq!(dec4_round((-0.03125f32).to_bits()), -312);
        assert_eq!(dec4_round(0.09375f32.to_bits()), 938);
        // Past 2^31 units there is no key, and the round saturates; a NaN's is 0.
        assert_eq!(dec4_key(214_748.36f32.to_bits()), Some(2_147_483_594));
        assert_eq!(dec4_key(214_748.375f32.to_bits()), None);
        assert_eq!(dec4_key((-214_748.375f32).to_bits()), None);
        assert_eq!(dec4_round(214_748.375f32.to_bits()), i32::MAX);
        assert_eq!(dec4_round((-1e30f32).to_bits()), i32::MIN);
        assert_eq!(dec4_round(0x7F80_0000), i32::MAX);
        assert_eq!((dec4_round(0x7FC0_0000), dec4_round(0xFFC0_0000)), (0, 0));
        let mut rng = Rng::new(8);
        let mut keys = 0;
        for _ in 0..1_000_000 {
            let bits = rng.next() as u32;
            if let Some(key) = dec4_key(bits) {
                keys += 1;
                assert_eq!((dec4_round(bits), from_dec4_key(key)), (key, bits), "{bits:#x}");
            }
        }
        assert!(keys > 0);
    }

    #[test]
    fn rice_codes_read_back_from_the_bytes_a_writer_packs() {
        assert_eq!((rice_bits(0, 0), rice_bits(11, 0), rice_bits(12, 0)), (1, 12, RICE_ESCAPE + 32));
        assert_eq!((rice_bits(11 << 5 | 31, 5), rice_bits(12 << 5, 5)), (17, RICE_ESCAPE + 32));
        assert_eq!((rice_bits(u32::MAX, 31), rice_bits(u32::MAX, 0)), (33, RICE_ESCAPE + 32));
        assert_eq!(rice_packed(&[(0, 0)]), [0x01]);
        assert_eq!(rice_packed(&[(3, 0)]), [0x08]);
        assert_eq!(rice_packed(&[(5, 1)]), [0x0C]);
        assert_eq!(rice_packed(&[(12, 0)]), [0x00, 0xC0, 0x00, 0x00, 0x00, 0x00]);
        let mut rng = Rng::new(9);
        for _ in 0..3000 {
            let codes: Vec<(u32, u32)> = (0..rng.next() % 80)
                .map(|_| {
                    let k = (rng.next() % 32) as u32;
                    let shift = rng.next() % 33;
                    (if shift == 32 { 0 } else { (rng.next() as u32) >> shift }, k)
                })
                .collect();
            let bytes = rice_packed(&codes);
            assert_eq!(bytes, rice_bit_by_bit(&codes));
            let mut roomy = bytes.clone();
            roomy.extend([0xFF; 8]);
            let mut bit = 0;
            for &(z, k) in &codes {
                let at = bit;
                assert_eq!(get_rice(&roomy, &mut bit, k), Some(z));
                assert_eq!(bit - at, rice_bits(z, k) as usize);
            }
        }
    }

    #[test]
    fn a_rice_escape_at_the_end_of_a_stream_reads_its_32_bits_from_the_spare_bytes() {
        let mut bytes = rice_packed(&[(2, 1), (0xFEDC_BA98, 3)]);
        assert_eq!(bytes.len(), 6);
        // Without 8 bytes from a code's first byte there is no load, so no code.
        assert_eq!(get_rice(&bytes, &mut 0, 1), None);
        bytes.extend([0; 8]);
        let mut bit = 0;
        assert_eq!(get_rice(&bytes, &mut bit, 1), Some(2));
        assert_eq!(get_rice(&bytes, &mut bit, 3), Some(0xFEDC_BA98));
        assert_eq!(bit, 3 + RICE_ESCAPE as usize + 32);
        assert_eq!(get_rice(&bytes, &mut 0, 32), None);
        let mut far = usize::MAX;
        assert_eq!(get_rice(&bytes, &mut far, 0), None);
    }

    #[test]
    fn the_best_rice_parameter_is_the_least_of_the_fewest_bits() {
        assert_eq!(best_rice_k(&[]), 0);
        assert_eq!(best_rice_k(&[0, 0, 0]), 0);
        assert_eq!(best_rice_k(&[u32::MAX]), 31);
        assert_eq!(best_rice_k(&[12]), 3);
        assert_eq!(rice_bits(2, 1) + rice_bits(3, 1), rice_bits(2, 2) + rice_bits(3, 2));
        assert_eq!(best_rice_k(&[2, 3]), 1);
    }

    #[test]
    fn keys_fields_and_varints_are_the_shared_vectors_read_and_written() {
        let rows = crate::exact_pip::tests::testdata_rows("coord_keys.tsv");
        let mut seen = std::collections::HashMap::new();
        for r in &rows {
            assert_eq!(r.len(), 3, "{r:?}");
            let (kind, given, want) = (r[0].as_str(), r[1].as_str(), r[2].as_str());
            *seen.entry(kind).or_insert(0usize) += 1;
            match kind {
                "key" => {
                    let bits = hex32(given);
                    let (order, hybrid) = want.split_once(',').unwrap();
                    assert_eq!(order_key(bits), hex32(order), "{r:?}");
                    assert_eq!(from_order_key(hex32(order)), bits, "{r:?}");
                    assert_eq!(hybrid_key(bits), (hybrid != "-").then(|| hex32(hybrid)), "{r:?}");
                    if hybrid != "-" {
                        assert!(is_hybrid_key(hex32(hybrid)), "{r:?}");
                        assert_eq!(from_hybrid_key(hex32(hybrid)), bits, "{r:?}");
                    }
                }
                "hybrid" => {
                    let key = hex32(given);
                    assert_eq!(is_hybrid_key(key), want != "-", "{r:?}");
                    if want != "-" {
                        assert_eq!(from_hybrid_key(key), hex32(want), "{r:?}");
                        assert_eq!(hybrid_key(hex32(want)), Some(key), "{r:?}");
                    }
                }
                "zigzag32" => {
                    assert_eq!(zigzag32(hex32(given)), hex32(want), "{r:?}");
                    assert_eq!(unzigzag32(hex32(want)), hex32(given), "{r:?}");
                }
                "zigzag16" => {
                    assert_eq!(zigzag16(hex32(given) as u16) as u32, hex32(want), "{r:?}");
                    assert_eq!(unzigzag16(hex32(want) as u16) as u32, hex32(given), "{r:?}");
                }
                "length" => {
                    let v = hex32(given);
                    assert_eq!(format!("{},{}", bit_length(v), byte_length(v)), want, "{r:?}");
                }
                "bits" => {
                    let fields: Vec<(u32, u32)> = given
                        .split(',')
                        .filter(|f| *f != "-")
                        .map(|f| {
                            let (v, w) = f.split_once(':').unwrap();
                            (hex32(v), w.parse().unwrap())
                        })
                        .collect();
                    assert_eq!(hex(&packed(&fields)), want, "{r:?}");
                    let mut bytes = bytes_of(want);
                    bytes.extend([0; 8]);
                    let mut bit = 0;
                    for &(v, w) in &fields {
                        assert_eq!(bits_at(&bytes, bit, w), Some(v), "{r:?}");
                        bit += w as usize;
                    }
                }
                "varint" => {
                    let v: u64 = given.parse().unwrap();
                    let mut b = Vec::new();
                    put_varint(&mut b, v);
                    assert_eq!(hex(&b), want, "{r:?}");
                    assert_eq!(whole_varint(&bytes_of(want), VARINT_MAX_BYTES), Some(v), "{r:?}");
                }
                "novarint" => {
                    // No varint of at most `want` bytes starts the bytes.
                    assert_eq!(varint(&bytes_of(given), &mut 0, want.parse().unwrap()), None, "{r:?}");
                }
                "dec4" => {
                    let bits = hex32(given);
                    let (key, round) = want.split_once(',').unwrap();
                    assert_eq!(dec4_key(bits), (key != "-").then(|| key.parse::<i32>().unwrap()), "{r:?}");
                    assert_eq!(dec4_round(bits), round.parse::<i32>().unwrap(), "{r:?}");
                    if key != "-" {
                        assert_eq!(from_dec4_key(key.parse().unwrap()), bits, "{r:?}");
                    }
                }
                "undec4" => assert_eq!(from_dec4_key(given.parse().unwrap()), hex32(want), "{r:?}"),
                "rice" => {
                    let codes: Vec<(u32, u32)> = given
                        .split(',')
                        .map(|c| {
                            let (z, k) = c.split_once(':').unwrap();
                            (hex32(z), k.parse().unwrap())
                        })
                        .collect();
                    assert_eq!(hex(&rice_packed(&codes)), want, "{r:?}");
                    let mut bytes = bytes_of(want);
                    bytes.extend([0; 8]);
                    let mut bit = 0;
                    for &(z, k) in &codes {
                        assert_eq!(get_rice(&bytes, &mut bit, k), Some(z), "{r:?}");
                    }
                }
                "ricek" => {
                    let z: Vec<u32> = given.split(',').filter(|v| *v != "-").map(hex32).collect();
                    let k = best_rice_k(&z);
                    let total: u64 = z.iter().map(|&v| rice_bits(v, k) as u64).sum();
                    assert_eq!(format!("{k},{total}"), want, "{r:?}");
                }
                _ => panic!("a row of a kind this test does not read: {r:?}"),
            }
        }
        let least = [("key", 150), ("hybrid", 30), ("zigzag32", 16), ("zigzag16", 12), ("dec4", 60), ("undec4", 8)];
        for (kind, rows) in least.into_iter().chain([("length", 16), ("bits", 16), ("varint", 20), ("novarint", 8), ("rice", 16), ("ricek", 12)]) {
            assert!(seen.get(kind).copied().unwrap_or(0) >= rows, "{kind}");
        }
    }
}
