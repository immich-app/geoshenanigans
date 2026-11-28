use super::{invalid, map, small};
use crate::{constants::PREFIX_TABLE_TOP, keys::Keys};
use memmap2::Mmap;
use std::{io, path::Path};
pub struct PrefixTable {
    fst: Keys<Mmap>,
    bytes: Mmap,
    entities: u32,
}
impl PrefixTable {
    pub(crate) fn open(dir: &Path, entities: u32) -> io::Result<Self> {
        Ok(Self {
            fst: Keys::new(map(&dir.join("core.pfx.fst"))?).map_err(invalid)?,
            bytes: map(&dir.join("core.pfx"))?,
            entities,
        })
    }
    pub fn get(&self, prefix: &str) -> Vec<u32> {
        let read = || {
            let mut at = usize::try_from(self.fst.get(prefix)?).ok()?;
            let n = small(&self.bytes, &mut at)? as usize;
            if n > PREFIX_TABLE_TOP {
                return None;
            }
            (0..n).map(|_| small(&self.bytes, &mut at).filter(|id| *id < self.entities)).collect()
        };
        read().unwrap_or_default()
    }
}
