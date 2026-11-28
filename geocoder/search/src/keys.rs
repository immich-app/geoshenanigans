//! The fst files of a sub-index (its keys and its prefix table), read without trusting their
//! bytes. Both builders write the fst crate's map format (VERSION); that crate's
//! reader decodes each node as if the file were whole, so a damaged one indexes past its end,
//! overflows, or follows a transition back up to a node it came from and walks without end. Here
//! a node is read only within the file and below the node whose transition leads to it, so every
//! walk goes down and ends; a node or a transition that does not read is one the map does not
//! hold. Each node is written after the nodes its transitions lead to and ends in its state byte,
//! its address; below that byte it holds its transition count, the byte sizes of its transitions'
//! steps and outputs, its inputs, the steps (how far below its first byte each transition's node
//! ends) and the outputs (fst 0.4, `raw/node.rs`).
use crate::constants::{MAX_KEY_BYTES, MAX_KEY_STEPS};
use fst::automaton::AlwaysMatch;
use fst::{Automaton, Streamer};

/// The fst format the build writes: the only one read.
pub const VERSION: u64 = 3;
/// The u64 format and u64 type before the nodes.
const HEADER_BYTES: usize = 16;
/// The u64 key count, u64 root address and u32 checksum after them.
const FOOTER_BYTES: usize = 20;
/// The address of the final node of no transition and no output, which takes no bytes.
const EMPTY: usize = 0;
/// A node of more transitions holds a 256-byte table of them by input.
pub(crate) const TRANS_INDEX_THRESHOLD: usize = 32;
/// The inputs a state byte holds itself, by its low six bits less one (fst's common inputs).
pub(crate) const COMMON_INPUTS: &[u8; 63] = b"te/oasripcnw.hlm-du012g=:bf3y5&_4v9678k%?xCDASFIBEjPTzRNM+LOqHG";

/// An fst map: a value for each key.
pub struct Keys<D> {
    data: D,
    root: usize,
}

#[derive(Clone, Copy)]
enum Shape {
    /// The final node of no transition, at EMPTY.
    Empty,
    /// One transition without an output, to the node that ends just below this one.
    Next,
    /// One transition.
    One,
    /// Any number of transitions, final or not.
    Any,
}

/// A node as read: where its parts are. Every one lies within [end, its address].
#[derive(Clone, Copy)]
struct Node {
    shape: Shape,
    /// Its first byte, which the steps of its transitions count down from.
    end: usize,
    ntrans: usize,
    is_final: bool,
    final_output: u64,
    /// The bytes of each step and each output.
    tsize: usize,
    osize: usize,
    /// The input of a node of one transition.
    input: u8,
    /// Any: the byte past its inputs, which are in reverse (transition 0 at one below), and the
    /// start of its table of transitions by input.
    inputs_end: usize,
}

/// A transition: its input, its output and the address of the node it leads to.
#[derive(Clone, Copy)]
struct Trans {
    input: u8,
    out: u64,
    addr: usize,
}

/// The little-endian integer of `b` (at most 8 bytes).
#[inline]
fn uint(b: &[u8]) -> u64 {
    let mut v = 0;
    for (i, &x) in b.iter().enumerate() {
        v |= (x as u64) << (8 * i);
    }
    v
}

impl Node {
    const EMPTY: Node =
        Node { shape: Shape::Empty, end: EMPTY, ntrans: 0, is_final: true, final_output: 0, tsize: 0, osize: 0, input: 0, inputs_end: 0 };

    /// The node of the fst `b` whose state byte is at `addr`; None for one that does not lie
    /// between the header and the footer, or of sizes no build writes.
    #[inline]
    fn read(b: &[u8], addr: usize) -> Option<Node> {
        if addr == EMPTY {
            return Some(Node::EMPTY);
        }
        if !(HEADER_BYTES..b.len().saturating_sub(FOOTER_BYTES)).contains(&addr) {
            return None;
        }
        let state = b[addr];
        // The address `n` bytes below the state byte, within the nodes.
        let down = |n: usize| (addr >= HEADER_BYTES + n).then(|| addr - n);
        let sizes = |at: usize| ((b[at] >> 4) as usize, (b[at] & 0x0F) as usize);
        if state >> 7 == 1 {
            // One transition: its input in the state byte, else in the byte below it.
            let (input, input_len) = match state & 0x3F {
                0 => (b[down(1)?], 1),
                c => (COMMON_INPUTS[c as usize - 1], 0),
            };
            if state >> 6 == 0b11 {
                return Some(Node { shape: Shape::Next, end: down(input_len)?, ntrans: 1, is_final: false, input, ..Node::EMPTY });
            }
            let (tsize, osize) = sizes(down(input_len + 1)?);
            if !(1..=8).contains(&tsize) || osize > 8 {
                return None;
            }
            let end = down(input_len + 1 + tsize + osize)?;
            return Some(Node { shape: Shape::One, end, ntrans: 1, is_final: false, tsize, osize, input, ..Node::EMPTY });
        }
        let is_final = state & 0x40 != 0;
        // The count in the state byte, else in the byte below it (where 1 stands for 256).
        let (ntrans, ntrans_len) = match state & 0x3F {
            0 => match b[down(1)?] {
                1 => (256, 1),
                n => (n as usize, 1),
            },
            n => (n as usize, 0),
        };
        let (tsize, osize) = sizes(down(ntrans_len + 1)?);
        if tsize > 8 || osize > 8 || (ntrans > 0 && tsize == 0) {
            return None;
        }
        let index = if ntrans > TRANS_INDEX_THRESHOLD { 256 } else { 0 };
        let final_osize = if is_final { osize } else { 0 };
        let end = down(ntrans_len + 1 + index + ntrans * (1 + tsize + osize) + final_osize)?;
        let inputs_end = addr - ntrans_len - 1 - index;
        let final_output = uint(&b[end..end + final_osize]);
        Some(Node { shape: Shape::Any, end, ntrans, is_final, final_output, tsize, osize, input: 0, inputs_end })
    }

    /// The node `step` bytes below this one's first byte (EMPTY for a step of 0); None for an
    /// address below the nodes, EMPTY's among them (only a step of 0 leads to EMPTY).
    #[inline]
    fn below(&self, step: u64) -> Option<usize> {
        if step == 0 {
            return Some(EMPTY);
        }
        self.end.checked_sub(usize::try_from(step).ok()?).filter(|&addr| addr >= HEADER_BYTES)
    }

    /// Transition `i` (below `ntrans`) of the node, whose bytes are in `b`; None for one that
    /// leads below the start of the file.
    #[inline(always)]
    fn trans(&self, b: &[u8], i: usize) -> Option<Trans> {
        let (input, out, step) = match self.shape {
            Shape::Empty => return None,
            Shape::Next => return Some(Trans { input: self.input, out: 0, addr: self.end - 1 }),
            Shape::One => {
                let at = self.end + self.osize;
                (self.input, uint(&b[self.end..at]), uint(&b[at..at + self.tsize]))
            }
            Shape::Any => {
                let steps_end = self.inputs_end - self.ntrans;
                let at = steps_end - (i + 1) * self.tsize;
                let out_at = steps_end - self.ntrans * self.tsize - (i + 1) * self.osize;
                (b[self.inputs_end - 1 - i], uint(&b[out_at..out_at + self.osize]), uint(&b[at..at + self.tsize]))
            }
        };
        Some(Trans { input, out, addr: self.below(step)? })
    }

    /// The transition of input `input`, by its number.
    #[inline]
    fn find(&self, b: &[u8], input: u8) -> Option<usize> {
        match self.shape {
            Shape::Empty => None,
            Shape::Next | Shape::One => (self.input == input).then_some(0),
            // The table's entry is a transition of that input, or of none (at or past ntrans).
            Shape::Any if self.ntrans > TRANS_INDEX_THRESHOLD => {
                let i = b[self.inputs_end + input as usize] as usize;
                (i < self.ntrans && b[self.inputs_end - 1 - i] == input).then_some(i)
            }
            Shape::Any => {
                let inputs = &b[self.inputs_end - self.ntrans..self.inputs_end];
                inputs.iter().position(|&x| x == input).map(|at| self.ntrans - 1 - at)
            }
        }
    }
}

impl<D: AsRef<[u8]>> Keys<D> {
    /// The map in `data`; an error for bytes no build writes: too few, of another format, a root
    /// anywhere but just before the footer, or a root node that does not read. Reads its header,
    /// its footer and its root node only.
    pub fn new(data: D) -> Result<Self, String> {
        let b = data.as_ref();
        let footer = b.len().checked_sub(FOOTER_BYTES).filter(|&f| f >= HEADER_BYTES);
        let footer = footer.ok_or_else(|| format!("{} bytes, too few for an fst", b.len()))?;
        let version = uint(&b[..8]);
        if version != VERSION {
            return Err(format!("an fst of format {version}, not {VERSION}"));
        }
        // The root is the last node written, so it ends where the footer starts; the empty final
        // node, which takes no bytes, only when it is the only node.
        let root = uint(&b[footer + 8..footer + 16]);
        let want = if footer == HEADER_BYTES { EMPTY } else { footer - 1 };
        if root != want as u64 {
            return Err(format!("an fst root at {root}, not {want}"));
        }
        let keys = Keys { data, root: want };
        Node::read(keys.data.as_ref(), keys.root).ok_or("an fst root that does not read")?;
        Ok(keys)
    }

    /// The value of `key`; None for a key the map does not hold, one below a node or a
    /// transition that does not read, or one whose outputs add up past 64 bits.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<u64> {
        let b = self.data.as_ref();
        let mut node = Node::read(b, self.root)?;
        let mut out = 0u64;
        for &x in key.as_ref() {
            let t = node.trans(b, node.find(b, x)?)?;
            out = out.checked_add(t.out)?;
            node = Node::read(b, t.addr)?;
        }
        if !node.is_final {
            return None;
        }
        out.checked_add(node.final_output)
    }

    /// Whether the map's bytes are those its build wrote, by the checksum after its footer (the
    /// fst crate's `verify`): a read of every byte, for a tool that walks every key (a damaged
    /// file's walk can be as long as its paths are many); no query takes it. An error too for a
    /// key count past this system's addresses, which the fst crate's reader panics on.
    pub fn verify(&self) -> Result<(), String> {
        let b = self.data.as_ref();
        key_count::<usize>(b)?;
        let checked = fst::raw::Fst::new(b).and_then(|f| f.verify());
        // The crate's error says only "FST error"; the one it holds says which.
        checked.map_err(|e| match e {
            fst::Error::Fst(e) => e.to_string(),
            e => e.to_string(),
        })
    }

    /// Every key and its value, in key order: a tool's walk, as long as the map.
    pub fn stream(&self) -> Pairs<'_, AlwaysMatch> {
        Pairs(self.walk(AlwaysMatch, usize::MAX))
    }

    /// The keys `aut` matches and their values, in key order: a query's walk, which ends after
    /// MAX_KEY_STEPS transitions.
    pub fn search<A: Automaton>(&self, aut: A) -> Pairs<'_, A> {
        Pairs(self.walk(aut, MAX_KEY_STEPS))
    }

    /// `search`, each key with the automaton's state after it.
    pub fn search_with_state<A: Automaton>(&self, aut: A) -> Stream<'_, A> {
        self.walk(aut, MAX_KEY_STEPS)
    }

    fn walk<A: Automaton>(&self, aut: A, max_steps: usize) -> Stream<'_, A> {
        let b = self.data.as_ref();
        let root = Node::read(b, self.root);
        let state = aut.start();
        Stream {
            b,
            empty: root.filter(|n| n.is_final).map(|n| n.final_output),
            stack: root.map(|node| vec![Frame { node, trans: 0, out: 0, state }]).unwrap_or_default(),
            aut,
            key: Vec::new(),
            steps_left: max_steps,
        }
    }
}

/// The key count in the footer of the fst `b` (one `Keys::new` took) as a `T`; an error for one
/// past what a `T` holds.
fn key_count<T: TryFrom<u64>>(b: &[u8]) -> Result<T, String> {
    let footer = b.len() - FOOTER_BYTES;
    let n = uint(&b[footer..footer + 8]);
    T::try_from(n).map_err(|_| format!("an fst of {n} keys, more than a {}-bit system counts", std::mem::size_of::<T>() * 8))
}

/// A walk of a map in key order, as fst's own streams walk one: each key the automaton matches,
/// its value and the automaton's state after it.
pub struct Stream<'k, A: Automaton> {
    /// The fst's bytes.
    b: &'k [u8],
    aut: A,
    /// The key of the node on top of the stack.
    key: Vec<u8>,
    /// The empty key's value, until the walk starts.
    empty: Option<u64>,
    /// The nodes from the root down to the last key's, each with the next transition to take.
    stack: Vec<Frame<A::State>>,
    /// The transitions the walk may still take.
    steps_left: usize,
}

struct Frame<S> {
    node: Node,
    trans: usize,
    out: u64,
    state: S,
}

impl<A: Automaton> Stream<'_, A> {
    /// The next key, its value and what `map` makes of the automaton's state after it.
    fn next_with<T>(&mut self, mut map: impl FnMut(&A::State) -> T) -> Option<(&[u8], u64, T)> {
        if let Some(out) = self.empty.take() {
            let start = self.aut.start();
            if self.aut.is_match(&start) {
                return Some((&[], out, map(&start)));
            }
        }
        let b = self.b;
        while let Some(f) = self.stack.pop() {
            // A node is done with past its last transition, at the length of the longest key a
            // build writes, or in a state that matches nothing below it. The root's frame adds
            // no byte to the key.
            if f.trans >= f.node.ntrans || self.key.len() >= MAX_KEY_BYTES || !self.aut.can_match(&f.state) {
                if !self.stack.is_empty() {
                    self.key.pop();
                }
                continue;
            }
            if self.steps_left == 0 {
                self.stack.clear();
                return None;
            }
            self.steps_left -= 1;
            // A transition, or a node it leads to, that does not read is as if it were not there,
            // as is one whose outputs add up past 64 bits (and every key below it).
            let Some((t, next)) = f.node.trans(b, f.trans).and_then(|t| Some((t, Node::read(b, t.addr)?))) else {
                self.stack.push(Frame { trans: f.trans + 1, ..f });
                continue;
            };
            let Some(out) = f.out.checked_add(t.out) else {
                self.stack.push(Frame { trans: f.trans + 1, ..f });
                continue;
            };
            let state = self.aut.accept(&f.state, t.input);
            let mut is_match = self.aut.is_match(&state);
            if next.is_final {
                if let Some(eof) = self.aut.accept_eof(&state) {
                    is_match = self.aut.is_match(&eof);
                }
            }
            self.key.push(t.input);
            self.stack.push(Frame { trans: f.trans + 1, ..f });
            let found = (next.is_final && is_match).then(|| map(&state));
            self.stack.push(Frame { node: next, trans: 0, out, state });
            // A key whose value is past 64 bits is none, the keys below it still are.
            if let (Some(mapped), Some(value)) = (found, out.checked_add(next.final_output)) {
                return Some((&self.key, value, mapped));
            }
        }
        None
    }
}

impl<'a, A: Automaton> Streamer<'a> for Stream<'_, A>
where
    A::State: Clone + 'a,
{
    type Item = (&'a [u8], u64, A::State);

    fn next(&'a mut self) -> Option<Self::Item> {
        self.next_with(Clone::clone)
    }
}

/// A `Stream` without the automaton's states.
pub struct Pairs<'k, A: Automaton>(Stream<'k, A>);

impl<'a, A: Automaton> Streamer<'a> for Pairs<'_, A> {
    type Item = (&'a [u8], u64);

    fn next(&'a mut self) -> Option<Self::Item> {
        self.0.next_with(|_| ()).map(|(k, v, _)| (k, v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fst::automaton::{Str, Subsequence};
    use fst::IntoStreamer;

    /// Every key and value a stream gives (ours or the fst crate's).
    macro_rules! pairs {
        ($s:expr) => {{
            let mut s = $s;
            let mut out: Vec<(Vec<u8>, u64)> = Vec::new();
            while let Some((k, v)) = s.next() {
                out.push((k.to_vec(), v));
            }
            out
        }};
    }

    /// Every key, value and automaton state a stream gives.
    macro_rules! triples {
        ($s:expr) => {{
            let mut s = $s;
            let mut out: Vec<(Vec<u8>, u64, usize)> = Vec::new();
            while let Some((k, v, st)) = s.next() {
                out.push((k.to_vec(), v, st));
            }
            out
        }};
    }

    /// Splitmix64.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let z = (self.0 ^ (self.0 >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            let z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        fn below(&mut self, n: usize) -> usize {
            (self.next() % n.max(1) as u64) as usize
        }
    }

    /// Maps of every node shape: no key; the empty key alone (no node bytes); keys sharing prefixes
    /// and suffixes over inputs in fst's common set and out of it; nodes of 2 to 32 transitions,
    /// of more (with the table by input) and of all 256; values of 0 to 8 bytes.
    fn maps() -> Vec<Vec<(Vec<u8>, u64)>> {
        let mut maps = vec![vec![], vec![(vec![], 0)], vec![(vec![], 5)], vec![(b"a".to_vec(), u64::MAX)]];
        let mut rng = Rng(7);
        for (n, alphabet) in [(50, &b"aet"[..]), (400, &b"aet/QXZ\x00\xff"[..]), (3000, &b"teoasZQ\x7f\x80 "[..])] {
            let mut keys: Vec<(Vec<u8>, u64)> = (0..n)
                .map(|_| {
                    let key = (0..rng.below(14)).map(|_| alphabet[rng.below(alphabet.len())]).collect();
                    (key, rng.next() >> rng.below(64))
                })
                .collect();
            keys.extend((0..=255u8).map(|x| (vec![b'w', x], x as u64)));
            keys.extend((0..40u8).map(|x| (vec![b'y', x * 3, b'z'], 1 << (x % 64))));
            keys.sort();
            keys.dedup_by(|a, b| a.0 == b.0);
            maps.push(keys);
        }
        maps
    }

    fn build(entries: &[(Vec<u8>, u64)]) -> Vec<u8> {
        let mut b = fst::MapBuilder::memory();
        for (k, v) in entries {
            b.insert(k, *v).unwrap();
        }
        b.into_inner().unwrap()
    }

    #[test]
    fn keys_read_as_the_fst_crate_reads_them() {
        for entries in maps() {
            let bytes = build(&entries);
            let (keys, map) = (Keys::new(bytes.clone()).unwrap(), fst::Map::new(bytes).unwrap());
            let n = entries.len();

            assert_eq!(pairs!(keys.stream()), entries, "{n} keys");
            let mut rng = Rng(n as u64);
            for (k, _) in entries.iter().take(300) {
                let mut other = k.clone();
                other.push(rng.next() as u8);
                for key in [k.clone(), other, k[..k.len() / 2].to_vec()] {
                    assert_eq!(keys.get(&key), map.get(&key), "{n} keys: {key:?}");
                    let prefix = String::from_utf8_lossy(&key).into_owned();
                    let starts = || Str::new(&prefix).starts_with();
                    assert_eq!(pairs!(keys.search(starts())), pairs!(map.search(starts()).into_stream()), "{n} keys: {prefix:?}");
                    let sub = || Subsequence::new(&prefix);
                    assert_eq!(triples!(keys.search_with_state(sub())), triples!(map.search_with_state(sub()).into_stream()), "{n} keys: {prefix:?}");
                }
            }
        }
    }

    /// fst's header and footer around `nodes` (written from HEADER_BYTES), the last node's state
    /// byte the root, a checksum of 0.
    fn fst_around(nodes: &[u8], keys: u64) -> Vec<u8> {
        let root = (HEADER_BYTES + nodes.len() - 1) as u64;
        [&VERSION.to_le_bytes()[..], &0u64.to_le_bytes(), nodes, &keys.to_le_bytes(), &root.to_le_bytes(), &[0; 4]].concat()
    }

    /// A node of one transition on `input` of 8-byte output `out`, `step` bytes below its first
    /// byte: [output][step][pack sizes][input][state].
    fn one(out: u64, step: u8, input: u8) -> Vec<u8> {
        [&out.to_le_bytes()[..], &[step, 0x18, input, 0x80]].concat()
    }

    #[test]
    fn a_step_to_before_the_nodes_is_no_transition_but_a_step_of_0_is_the_final_leaf() {
        // The root's one transition, on 'a', of a 1-byte step and no output: [step][pack sizes]
        // [input][state] from address 16, its first byte.
        let root = |step: u8| fst_around(&[step, 0x10, b'a', 0x80], 1);
        let (leaf, before) = (root(0), root(16));

        let leaf_keys = Keys::new(leaf.clone()).unwrap();
        assert_eq!(leaf_keys.get("a"), fst::Map::new(leaf).unwrap().get("a"));
        assert_eq!((leaf_keys.get("a"), pairs!(leaf_keys.stream())), (Some(0), vec![(b"a".to_vec(), 0)]));
        // A step down to address 0 counts the transition's node below the start of the file.
        let before = Keys::new(before).unwrap();
        assert_eq!((before.get("a"), pairs!(before.stream())), (None, vec![]));
    }

    #[test]
    fn outputs_past_64_bits_are_no_value() {
        // 'a' of output 2^64 - 1 to a final node of output 2 and no transition: [final output]
        // [pack sizes][transition count][state], then the root.
        let final_past = fst_around(&[[2, 0x01, 0, 0x40].as_slice(), &one(u64::MAX, 1, b'a')].concat(), 1);
        // 'a' of output 2^64 - 1, then 'b' of output 2 to the final leaf.
        let trans_past = fst_around(&[[2, 0, 0x11, b'b', 0x80].as_slice(), &one(u64::MAX, 1, b'a')].concat(), 1);
        // 'a' of output 2^64 - 1 to a final node of output 2 (past 64 bits) whose one transition,
        // 'b' of output 0, leads to the final leaf (2^64 - 1): [final output][output][step][input]
        // [pack sizes][state], then the root.
        let below_past = fst_around(&[[2, 0, 0, b'b', 0x11, 0x41].as_slice(), &one(u64::MAX, 1, b'a')].concat(), 2);
        let none: Vec<(Vec<u8>, u64)> = vec![];

        for (case, bytes, key, streamed) in [
            ("a final output", final_past, "a", none.clone()),
            ("a transition's output", trans_past, "ab", none),
            ("a final output above a key", below_past, "a", vec![(b"ab".to_vec(), u64::MAX)]),
        ] {
            let keys = Keys::new(bytes).unwrap();

            assert_eq!(keys.get(key), None, "{case}");
            assert_eq!(pairs!(keys.stream()), streamed, "{case}");
            assert_eq!(pairs!(keys.search(Str::new(key).starts_with())), streamed, "{case}");
        }
    }

    #[test]
    fn an_entry_of_a_wide_nodes_table_names_a_transition_of_its_input_or_none() {
        // A root of 40 transitions, 'A' to 'h', the k-th of value k: it holds a table by input.
        let entries: Vec<(Vec<u8>, u64)> = (0..40u8).map(|k| (vec![b'A' + k], k as u64)).collect();
        let mut b = build(&entries);
        let root = Node::read(&b, b.len() - FOOTER_BYTES - 1).unwrap();
        assert_eq!((root.ntrans, b[root.inputs_end + b'C' as usize]), (40, 2));

        // 'B' names the transition of 'C', 'z' (of none) that of 'D'.
        b[root.inputs_end + b'B' as usize] = 2;
        b[root.inputs_end + b'z' as usize] = 3;
        let keys = Keys::new(b).unwrap();

        assert_eq!([keys.get("B"), keys.get("z"), keys.get("C"), keys.get("D")], [None, None, Some(2), Some(3)]);
    }

    #[test]
    fn a_key_count_past_the_systems_addresses_is_refused_before_the_checksum() {
        let mut b = build(&[(b"a".to_vec(), 1)]);
        assert_eq!(key_count::<u32>(&b), Ok(1));
        let footer = b.len() - FOOTER_BYTES;
        b[footer..footer + 8].copy_from_slice(&(1u64 << 32).to_le_bytes());

        // As a 32-bit system counts it, and as a 64-bit one does (where the checksum refuses it).
        assert_eq!(key_count::<u32>(&b), Err("an fst of 4294967296 keys, more than a 32-bit system counts".to_string()));
        assert_eq!(key_count::<u64>(&b), Ok(1 << 32));
        assert!(Keys::new(b).unwrap().verify().is_err_and(|e| e.contains("checksum")));
    }

    #[test]
    fn damaged_maps_read_without_a_panic_and_every_walk_ends() {
        for entries in maps().into_iter().take(6) {
            let whole = build(&entries);
            let mut rng = Rng(whole.len() as u64);
            for case in 0..600 {
                let mut b = whole.clone();
                match case % 3 {
                    0 => b.truncate(rng.below(b.len())),
                    1 => (0..1 + rng.below(3)).for_each(|_| {
                        let at = rng.below(b.len());
                        b[at] = rng.next() as u8;
                    }),
                    _ => {
                        let at = b.len() - 1 - rng.below(b.len().min(40));
                        b[at] ^= 1 << rng.below(8);
                    }
                }
                let Ok(keys) = Keys::new(b) else { continue };
                let mut s = keys.stream();
                while s.next().is_some() {}
                for (k, _) in entries.iter().take(50) {
                    keys.get(k);
                    let prefix = String::from_utf8_lossy(k);
                    let mut s = keys.search(Str::new(&prefix).starts_with());
                    while s.next().is_some() {}
                }
            }
        }
    }
}
