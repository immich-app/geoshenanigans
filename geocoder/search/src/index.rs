//! What a format-3 index holds, as the search reads it: its entities and the records they are, its
//! terms, and its streets.
pub mod entities;
pub mod streets;
pub mod terms;

/// The sub-indexes of an index, and the posting set over the core's entities that only the full
/// profile reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Sub {
    /// Places, areas and POIs: what the shipped index holds.
    Core,
    /// Streets, named addresses and postcodes.
    Addr,
    /// The address words and categories of the core's entities.
    AddrX,
}

impl Sub {
    /// Its files' stem: `<stem>.fst`, `<stem>.post`, ...
    pub fn as_str(self) -> &'static str {
        match self {
            Sub::Core => "core",
            Sub::Addr => "addr",
            Sub::AddrX => "addr.x",
        }
    }
}

#[cfg(test)]
mod core_tests;

pub mod areas;
pub mod chains;
mod dictionary;
mod posting;
pub mod prefix;
pub use crate::build::sources::CoreRecords;
pub use dictionary::Dictionary;
use memmap2::Mmap;
pub use posting::PostingList;
use std::{fs::File, io, path::Path};

pub(crate) fn map(path: &Path) -> io::Result<Mmap> {
    // Published files are replaced by rename. Their backing inodes stay immutable.
    unsafe { Mmap::map(&File::open(path)?) }
}
pub(crate) fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}
pub(crate) fn number(bytes: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(bytes.get(at..)?.get(..4)?.try_into().ok()?))
}
pub(crate) fn small(bytes: &[u8], at: &mut usize) -> Option<u32> {
    crate::varint::try_get(bytes, at)?.try_into().ok()
}

/// The mapped core of a published index. The directory lock spans every map.
pub struct CoreIndex {
    pub entities: entities::EntityTable,
    pub chains: chains::ChainTable,
    pub areas: areas::AreaRanges,
    pub terms: Dictionary,
    pub prefix: prefix::PrefixTable,
}
impl CoreIndex {
    pub fn open(dir: &Path) -> io::Result<Self> {
        use crate::build::{Contended, HeldIndexDir};
        let held = HeldIndexDir::take(dir, Contended::Wait).map_err(invalid)?;
        Self::open_held(dir, &held)
    }

    pub(crate) fn open_held(dir: &Path, _held: &crate::build::HeldIndexDir) -> io::Result<Self> {
        use crate::{build::has_unfinished_publish, constants::*};
        if has_unfinished_publish(dir) {
            return Err(invalid("unfinished index publish"));
        }
        let meta: serde_json::Value = serde_json::from_reader(File::open(dir.join(META_FILE))?)?;
        if meta[META_FORMAT_KEY] != INDEX_FORMAT {
            return Err(invalid("expected a format 3 index"));
        }
        let entities = entities::EntityTable::open(dir)?;
        Ok(Self {
            chains: chains::ChainTable::open(dir, entities.len())?,
            areas: areas::AreaRanges::open(dir, entities.len())?,
            terms: Dictionary::open(dir, entities.len())?,
            prefix: prefix::PrefixTable::open(dir, entities.len())?,
            entities,
        })
    }

    pub fn record<'a>(&self, id: u32, records: &'a impl entities::Entities) -> Option<entities::EntityRecord<'a>> {
        records.read_in_chain(self.entities.get(id)?, self.chains.outer_slot(id))
    }

    /// The most important readable matches of a posting after the caller's filters.
    pub fn best(
        &self,
        posting: terms::PostingRef,
        records: &impl entities::Entities,
        limit: usize,
        mut accept: impl FnMut(u32, &entities::EntityRecord<'_>) -> bool,
    ) -> Vec<u32> {
        let Some(list) = self.terms.posting(posting) else { return Vec::new() };
        list.best(limit, |id| {
            let row = self.record(id, records)?;
            accept(id, &row).then(|| (row.importance / crate::constants::IMP_UNIT).round() as u32)
        })
    }
}

/// A counted table of offsets followed by their blob. The terminal row bounds the last slice.
pub(crate) struct BlobTable {
    bytes: Mmap,
    rows: usize,
    stride: usize,
    blob: usize,
}
impl BlobTable {
    fn open(path: &Path, stride: usize) -> io::Result<Self> {
        let bytes = map(path)?;
        let rows = number(&bytes, 0).ok_or_else(|| invalid("missing row count"))? as usize;
        let blob = rows
            .checked_add(1)
            .and_then(|n| n.checked_mul(stride))
            .and_then(|n| n.checked_add(4))
            .ok_or_else(|| invalid("table size overflow"))?;
        if blob > bytes.len() || number(&bytes, blob - 4).map(|n| n as usize) != Some(bytes.len() - blob) {
            return Err(invalid("bad blob length"));
        }
        Ok(Self { bytes, rows, stride, blob })
    }
    fn field(&self, row: usize, col: usize) -> Option<u32> {
        if row > self.rows {
            return None;
        }
        number(&self.bytes, 4 + row * self.stride + col)
    }
    fn slice(&self, row: usize) -> Option<&[u8]> {
        if row >= self.rows {
            return None;
        }
        let col = self.stride - 4;
        let blob = &self.bytes[self.blob..];
        blob.get(self.field(row, col)? as usize..self.field(row + 1, col)? as usize)
    }
}
