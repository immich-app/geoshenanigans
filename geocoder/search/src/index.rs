//! What a format-3 index holds, as the search reads it: its entities, and the records they are.
pub mod entities;

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
