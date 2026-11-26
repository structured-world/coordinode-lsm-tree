// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What a compaction's k-way merge costs per entry by the structure that picks
//! the next one, with everything around it left out: the inputs are entries
//! already in memory, so no block is decoded and nothing is written.
//!
//! Three structures merge the same inputs:
//!
//! | arm | structure |
//! |---|---|
//! | `sorted` | the engine's [`Merger`]: slot numbers kept in sorted order, a replaced entry placed by binary search |
//! | `binary` | a binary min-heap of slot numbers, the replaced top sifted down |
//! | `loser` | a tournament tree that only moves forward: the replaced winner replays its path to the root |
//!
//! The two written here compare as the engine's does: by an eight-byte word of
//! each key past the prefix every key shares, by the whole keys where the
//! words tie, and by input number last, so every arm yields the same entries
//! in the same order (checked before the rounds).
//!
//! | axis | values |
//! |---|---|
//! | fan-in | 2, 4, 8, 16, 32, 64 inputs |
//! | layout | `disjoint` (each input a contiguous slice), `interleaved` (key `i` in input `i % fan-in`), `zipf` (a key's input drawn with Zipf(0.99) weights, so one input wins most steps) or `mvcc` (every input holds a version of every key) |
//! | key shape | `word` (keys differ within the compared word) or `deep` (4096 keys in a row share the word, so whole keys are compared) |
//! | order | `bytes` (the default comparator) or `custom` (one the engine cannot treat as bytes, so no word helps) |
//!
//! Per arm it prints the median and P99 over the rounds of the merge time per
//! entry, and the heap allocations per merge. Built with the `diagnostics`
//! feature it also prints the word comparisons, whole-key comparisons and slot
//! moves per entry; those counters cost time, so that build answers what
//! happens, not how long it takes.
//!
//! `MERGE_FAN_IN_ENTRIES` (default 1000000) sets the entries per
//! configuration, `MERGE_FAN_IN_ROUNDS` (default 7) the rounds, and
//! `MERGE_FAN_IN_FANINS`, `MERGE_FAN_IN_LAYOUTS`, `MERGE_FAN_IN_SHAPES`,
//! `MERGE_FAN_IN_ORDERS` and `MERGE_FAN_IN_ARMS` (comma-separated) run only the
//! values they name. The arms of a configuration take turns going first.
//!
//! `MERGE_FAN_IN_PERF_CTL=<ctl>,<ack>` names the fifos of a
//! `perf stat --control fifo:<ctl>,<ack> -D -1`: the bench enables counting
//! around each merge, so the counts leave the inputs' construction out.

#![expect(
    clippy::expect_used,
    reason = "only for invariants of the harness itself; setup I/O is propagated"
)]
#![expect(
    clippy::indexing_slicing,
    reason = "slot and node numbers are below the lengths they were built for"
)]

use lsm_tree::merge::Merger;
use lsm_tree::{DefaultUserComparator, InternalValue, SharedComparator, UserComparator, ValueType};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::cmp::Ordering as KeyOrder;
use std::fs::File;
use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// The system allocator, counting allocations.
struct Counting;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every call delegates to `System` with the caller's layout and
// pointer unchanged; the counter only observes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: the caller upholds `alloc`'s contract, passed through.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller upholds `dealloc`'s contract, passed through.
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: the caller upholds `realloc`'s contract, passed through.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

// A program links one global allocator, and the crate's `mimalloc` feature
// installs its own: built with it, this one counts nothing.
#[cfg_attr(not(feature = "mimalloc"), global_allocator)]
#[cfg_attr(
    feature = "mimalloc",
    expect(dead_code, reason = "not the global allocator")
)]
static GLOBAL: Counting = Counting;

const FANINS: [usize; 6] = [2, 4, 8, 16, 32, 64];
/// Keys in a row that share the compared word under the `deep` shape.
const DEEP_RUN_BITS: u32 = 12;
/// What the `word` shape multiplies a key's number by, so consecutive keys
/// differ in the bytes the word covers.
const WORD_STEP: u64 = 1 << 20;

type Item = lsm_tree::Result<InternalValue>;
type Source = Box<dyn Iterator<Item = Item>>;

/// Bytewise order backwards: a comparator the engine cannot treat as bytes.
struct Backwards;

impl UserComparator for Backwards {
    fn name(&self) -> &'static str {
        "merge-fan-in-backwards"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> KeyOrder {
        b.cmp(a)
    }
}

#[derive(Clone, Copy)]
enum Order {
    Bytes,
    Custom,
}

impl Order {
    const ALL: [Self; 2] = [Self::Bytes, Self::Custom];

    fn label(self) -> &'static str {
        match self {
            Self::Bytes => "bytes",
            Self::Custom => "custom",
        }
    }

    fn comparator(self) -> SharedComparator {
        match self {
            Self::Bytes => Arc::new(DefaultUserComparator),
            Self::Custom => Arc::new(Backwards),
        }
    }
}

#[derive(Clone, Copy)]
enum Shape {
    Word,
    Deep,
}

impl Shape {
    const ALL: [Self; 2] = [Self::Word, Self::Deep];

    fn label(self) -> &'static str {
        match self {
            Self::Word => "word",
            Self::Deep => "deep",
        }
    }

    /// Key number `i`, ascending in `i` by its bytes.
    fn key(self, i: u64) -> Vec<u8> {
        match self {
            Self::Word => {
                let mut key = (i * WORD_STEP).to_be_bytes().to_vec();
                key.extend_from_slice(&i.to_be_bytes());
                key
            }
            Self::Deep => {
                let mut key = (i >> DEEP_RUN_BITS).to_be_bytes().to_vec();
                key.extend_from_slice(b"kkkkkkkk");
                key.extend_from_slice(&i.to_be_bytes());
                key
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Placement {
    Disjoint,
    Interleaved,
    Zipf,
    Mvcc,
}

impl Placement {
    const ALL: [Self; 4] = [Self::Disjoint, Self::Interleaved, Self::Zipf, Self::Mvcc];

    fn label(self) -> &'static str {
        match self {
            Self::Disjoint => "disjoint",
            Self::Interleaved => "interleaved",
            Self::Zipf => "zipf",
            Self::Mvcc => "mvcc",
        }
    }
}

/// The inputs of one configuration, each in the order its comparator gives:
/// `n` entries between `fanin` inputs.
fn inputs(
    order: Order,
    shape: Shape,
    placement: Placement,
    fanin: usize,
    n: usize,
) -> Vec<Vec<InternalValue>> {
    let entry = |i: u64, seqno: u64| {
        InternalValue::from_components(shape.key(i), b"vvvvvvvv".to_vec(), seqno, ValueType::Value)
    };
    let mut inputs: Vec<Vec<InternalValue>> = (0..fanin).map(|_| Vec::new()).collect();
    match placement {
        Placement::Disjoint => {
            for i in 0..n {
                inputs[i * fanin / n].push(entry(i as u64, i as u64));
            }
        }
        Placement::Interleaved => {
            for i in 0..n {
                inputs[i % fanin].push(entry(i as u64, i as u64));
            }
        }
        Placement::Zipf => {
            // Cumulative Zipf(0.99) weights over the inputs.
            let mut cumulative = Vec::with_capacity(fanin);
            let mut total = 0.0_f64;
            for rank in 1..=fanin {
                total += (rank as f64).powf(-0.99);
                cumulative.push(total);
            }
            let mut state = 0x9E37_79B9_7F4A_7C15_u64;
            for i in 0..n {
                // xorshift64*: the same draws every run.
                state ^= state >> 12;
                state ^= state << 25;
                state ^= state >> 27;
                let draw = state.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11;
                let at = draw as f64 / (1_u64 << 53) as f64 * total;
                let input = cumulative
                    .partition_point(|&upto| upto <= at)
                    .min(fanin - 1);
                inputs[input].push(entry(i as u64, i as u64));
            }
        }
        Placement::Mvcc => {
            // Input 0 holds each key's newest version, as a compaction lists
            // its inputs newest first.
            for i in 0..(n / fanin).max(1) {
                for (input, held) in inputs.iter_mut().enumerate() {
                    held.push(entry(i as u64, (fanin - input) as u64));
                }
            }
        }
    }
    if matches!(order, Order::Custom) {
        // The custom order is the byte order backwards.
        for input in &mut inputs {
            input.reverse();
        }
    }
    inputs
}

/// The bytes every key of `inputs` starts with, as a compaction takes them
/// from its tables' key ranges.
fn common_prefix(inputs: &[Vec<InternalValue>]) -> usize {
    let mut keys = inputs
        .iter()
        .flat_map(|input| [input.first(), input.last()])
        .flatten()
        .map(|entry| &*entry.key.user_key);
    let Some(first) = keys.next() else {
        return 0;
    };
    keys.fold(first.len(), |shared, key| {
        shared.min(first.iter().zip(key).take_while(|(a, b)| a == b).count())
    })
}

/// Adds one to `counter` in a build that counts.
#[inline]
fn tick(counter: &Cell<u64>) {
    if cfg!(feature = "diagnostics") {
        counter.set(counter.get() + 1);
    }
}

/// What a merge did, per the counters of a `diagnostics` build.
#[derive(Clone, Copy, Default)]
struct Work {
    word_compares: u64,
    key_compares: u64,
    moves: u64,
}

/// One current entry per input, compared as the engine's merge heap compares:
/// by key word, then by key, then by input number.
struct Slots {
    entries: Vec<Option<InternalValue>>,
    words: Vec<u64>,
    comparator: SharedComparator,
    lexicographic: bool,
    prefix: usize,
    word_compares: Cell<u64>,
    key_compares: Cell<u64>,
    moves: Cell<u64>,
}

impl Slots {
    fn new(slots: usize, comparator: SharedComparator, prefix: usize) -> Self {
        let lexicographic = comparator.is_lexicographic();
        Self {
            entries: (0..slots).map(|_| None).collect(),
            words: vec![0; slots],
            comparator,
            lexicographic,
            prefix: if lexicographic { prefix } else { 0 },
            word_compares: Cell::new(0),
            key_compares: Cell::new(0),
            moves: Cell::new(0),
        }
    }

    /// The eight bytes of `key` past the shared prefix, as an integer that
    /// orders as they do; zero under an order that is not the byte order.
    #[inline]
    fn word(&self, key: &[u8]) -> u64 {
        if !self.lexicographic {
            return 0;
        }
        let tail = key.get(self.prefix..).unwrap_or_default();
        if let Some(head) = tail.first_chunk::<8>() {
            return u64::from_be_bytes(*head);
        }
        let mut padded = [0; 8];
        padded[..tail.len()].copy_from_slice(tail);
        u64::from_be_bytes(padded)
    }

    /// Puts `entry` into `slot`, returning what it held.
    #[inline]
    fn put(&mut self, slot: usize, entry: InternalValue) -> Option<InternalValue> {
        self.words[slot] = self.word(&entry.key.user_key);
        self.entries[slot].replace(entry)
    }

    /// The order of two occupied slots.
    #[inline]
    fn order(&self, a: usize, b: usize) -> KeyOrder {
        tick(&self.word_compares);
        match self.words[a].cmp(&self.words[b]) {
            KeyOrder::Equal => {}
            order => return order,
        }
        tick(&self.key_compares);
        let (Some(x), Some(y)) = (&self.entries[a], &self.entries[b]) else {
            unreachable!("only occupied slots are ordered");
        };
        let keys = if self.lexicographic {
            x.key.cmp(&y.key)
        } else {
            self.comparator
                .compare(&x.key.user_key, &y.key.user_key)
                .then_with(|| y.key.seqno.cmp(&x.key.seqno))
        };
        keys.then_with(|| a.cmp(&b))
    }

    fn work(&self) -> Work {
        Work {
            word_compares: self.word_compares.get(),
            key_compares: self.key_compares.get(),
            moves: self.moves.get(),
        }
    }
}

/// A binary min-heap of slot numbers.
struct BinaryMerge {
    slots: Slots,
    sources: Vec<Source>,
    heap: Vec<u32>,
}

impl BinaryMerge {
    fn new(
        mut sources: Vec<Source>,
        comparator: SharedComparator,
        prefix: usize,
    ) -> lsm_tree::Result<Self> {
        let mut slots = Slots::new(sources.len(), comparator, prefix);
        let mut heap = Vec::with_capacity(sources.len());
        for (slot, source) in sources.iter_mut().enumerate() {
            if let Some(entry) = source.next() {
                slots.put(slot, entry?);
                heap.push(slot as u32);
            }
        }
        let mut merge = Self {
            slots,
            sources,
            heap,
        };
        for at in (0..merge.heap.len() / 2).rev() {
            merge.sift_down(at);
        }
        Ok(merge)
    }

    /// Moves the slot at `at` down to below every smaller one.
    #[inline]
    fn sift_down(&mut self, mut at: usize) {
        let moving = self.heap[at];
        loop {
            let mut child = 2 * at + 1;
            if child >= self.heap.len() {
                break;
            }
            if child + 1 < self.heap.len()
                && self
                    .slots
                    .order(self.heap[child + 1] as usize, self.heap[child] as usize)
                    == KeyOrder::Less
            {
                child += 1;
            }
            if self.slots.order(self.heap[child] as usize, moving as usize) != KeyOrder::Less {
                break;
            }
            self.heap[at] = self.heap[child];
            tick(&self.slots.moves);
            at = child;
        }
        self.heap[at] = moving;
    }
}

impl Iterator for BinaryMerge {
    type Item = Item;

    #[inline]
    fn next(&mut self) -> Option<Item> {
        let slot = *self.heap.first()? as usize;
        match self.sources[slot].next() {
            Some(Ok(entry)) => {
                let old = self.slots.put(slot, entry);
                self.sift_down(0);
                old.map(Ok)
            }
            Some(Err(e)) => Some(Err(e)),
            None => {
                let old = self.slots.entries[slot].take();
                let last = self.heap.pop()?;
                if !self.heap.is_empty() {
                    self.heap[0] = last;
                    self.sift_down(0);
                }
                old.map(Ok)
            }
        }
    }
}

/// A tournament tree that only moves forward: `tree[0]` is the winning slot,
/// `tree[node]` the slot that lost the game at `node`. An input that ran out
/// leaves its slot empty, and an empty slot loses every game.
struct LoserMerge {
    slots: Slots,
    sources: Vec<Source>,
    tree: Vec<u32>,
    live: usize,
}

impl LoserMerge {
    fn new(
        mut sources: Vec<Source>,
        comparator: SharedComparator,
        prefix: usize,
    ) -> lsm_tree::Result<Self> {
        let leaves = sources.len().next_power_of_two().max(2);
        let mut slots = Slots::new(leaves, comparator, prefix);
        let mut live = 0;
        for (slot, source) in sources.iter_mut().enumerate() {
            if let Some(entry) = source.next() {
                slots.put(slot, entry?);
                live += 1;
            }
        }
        let mut merge = Self {
            slots,
            sources,
            tree: vec![0; leaves],
            live,
        };
        // The winner of each subtree, by node number: leaves from `leaves` up.
        let mut winners: Vec<u32> = (0..2 * leaves as u32)
            .map(|node| node.saturating_sub(leaves as u32))
            .collect();
        for node in (1..leaves).rev() {
            let (left, right) = (winners[2 * node], winners[2 * node + 1]);
            let (winner, loser) = if merge.beats(right as usize, left as usize) {
                (right, left)
            } else {
                (left, right)
            };
            merge.tree[node] = loser;
            winners[node] = winner;
        }
        merge.tree[0] = winners[1];
        Ok(merge)
    }

    /// Whether slot `a` comes before slot `b`.
    #[inline]
    fn beats(&self, a: usize, b: usize) -> bool {
        match (&self.slots.entries[a], &self.slots.entries[b]) {
            (Some(_), Some(_)) => self.slots.order(a, b) == KeyOrder::Less,
            (Some(_), None) => true,
            (None, _) => false,
        }
    }

    /// Plays the games on the path from `leaf` to the root again.
    #[inline]
    fn replay(&mut self, leaf: usize) {
        let mut winner = leaf;
        let mut node = (self.tree.len() + leaf) / 2;
        while node >= 1 {
            let other = self.tree[node] as usize;
            if self.beats(other, winner) {
                self.tree[node] = winner as u32;
                tick(&self.slots.moves);
                winner = other;
            }
            node >>= 1;
        }
        self.tree[0] = winner as u32;
    }
}

impl Iterator for LoserMerge {
    type Item = Item;

    #[inline]
    fn next(&mut self) -> Option<Item> {
        if self.live == 0 {
            return None;
        }
        let slot = self.tree[0] as usize;
        let old = match self.sources[slot].next() {
            Some(Ok(entry)) => self.slots.put(slot, entry),
            Some(Err(e)) => return Some(Err(e)),
            None => {
                self.live -= 1;
                self.slots.entries[slot].take()
            }
        };
        self.replay(slot);
        old.map(Ok)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Arm {
    Sorted,
    Binary,
    Loser,
}

impl Arm {
    const ALL: [Self; 3] = [Self::Sorted, Self::Binary, Self::Loser];

    fn label(self) -> &'static str {
        match self {
            Self::Sorted => "sorted",
            Self::Binary => "binary",
            Self::Loser => "loser",
        }
    }
}

/// The work the engine's merge heap counted so far, zero in a build that does
/// not count.
fn engine_work() -> Work {
    #[cfg(feature = "diagnostics")]
    {
        let counts = lsm_tree::diagnostics::Counts::now();
        Work {
            word_compares: counts.merge_word_compares,
            key_compares: counts.merge_key_compares,
            moves: counts.merge_slides,
        }
    }
    #[cfg(not(feature = "diagnostics"))]
    Work::default()
}

/// Merges `inputs` through `arm`, handing every entry to `each`, and returns
/// the work it counted.
fn merge(
    arm: Arm,
    inputs: Vec<Vec<InternalValue>>,
    comparator: &SharedComparator,
    prefix: usize,
    mut each: impl FnMut(InternalValue),
) -> lsm_tree::Result<Work> {
    let sources: Vec<Source> = inputs
        .into_iter()
        .map(|input| Box::new(input.into_iter().map(Ok)) as Source)
        .collect();
    match arm {
        Arm::Sorted => {
            let before = engine_work();
            for entry in Merger::new(sources, Arc::clone(comparator)).with_common_prefix(prefix) {
                each(entry?);
            }
            let after = engine_work();
            Ok(Work {
                word_compares: after.word_compares - before.word_compares,
                key_compares: after.key_compares - before.key_compares,
                moves: after.moves - before.moves,
            })
        }
        Arm::Binary => {
            let mut merge = BinaryMerge::new(sources, Arc::clone(comparator), prefix)?;
            for entry in merge.by_ref() {
                each(entry?);
            }
            Ok(merge.slots.work())
        }
        Arm::Loser => {
            let mut merge = LoserMerge::new(sources, Arc::clone(comparator), prefix)?;
            for entry in merge.by_ref() {
                each(entry?);
            }
            Ok(merge.slots.work())
        }
    }
}

/// Holds every arm's output to the engine merger's: the same entries, each
/// from the same input version, in the same order, and that order ascending.
fn check_arms_agree(
    inputs: &[Vec<InternalValue>],
    comparator: &SharedComparator,
    prefix: usize,
    label: &str,
) -> lsm_tree::Result<()> {
    let output = |arm| -> lsm_tree::Result<Vec<(Vec<u8>, u64)>> {
        let mut out = Vec::new();
        merge(arm, inputs.to_vec(), comparator, prefix, |entry| {
            out.push((entry.key.user_key.to_vec(), entry.key.seqno));
        })?;
        Ok(out)
    };
    let expected = output(Arm::Sorted)?;
    assert_eq!(
        expected.len(),
        inputs.iter().map(Vec::len).sum::<usize>(),
        "{label}: every entry comes out once"
    );
    assert!(
        expected.windows(2).all(|pair| {
            comparator
                .compare(&pair[0].0, &pair[1].0)
                .then_with(|| pair[1].1.cmp(&pair[0].1))
                != KeyOrder::Greater
        }),
        "{label}: the merged entries ascend, a key's newer version first"
    );
    for arm in [Arm::Binary, Arm::Loser] {
        assert!(
            output(arm)? == expected,
            "{label}: the {} merge yields what the engine's does",
            arm.label()
        );
    }
    Ok(())
}

fn env_or(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Whether the comma-separated list in `name` names `item`; every item when
/// the variable is unset.
fn picked(name: &str, item: &str) -> bool {
    match std::env::var(name) {
        Ok(list) => list.split(',').any(|v| v.trim() == item),
        Err(_) => true,
    }
}

/// The control fifos of a running `perf`.
struct PerfControl {
    ctl: File,
    ack: File,
}

impl PerfControl {
    fn from_env() -> lsm_tree::Result<Option<Self>> {
        let Ok(paths) = std::env::var("MERGE_FAN_IN_PERF_CTL") else {
            return Ok(None);
        };
        let (ctl, ack) = paths
            .split_once(',')
            .expect("MERGE_FAN_IN_PERF_CTL is <ctl>,<ack>");
        Ok(Some(Self {
            ctl: File::options().write(true).open(ctl)?,
            ack: File::open(ack)?,
        }))
    }

    /// Sends `command` and waits for perf to apply it.
    fn send(&mut self, command: &str) -> lsm_tree::Result<()> {
        self.ctl.write_all(command.as_bytes())?;
        let mut reply = [0; 5];
        self.ack.read_exact(&mut reply)?;
        Ok(())
    }
}

/// What the rounds of one arm measured.
#[derive(Default)]
struct Samples {
    times: Vec<Duration>,
    allocs: Vec<usize>,
    work: Work,
}

fn main() -> lsm_tree::Result<()> {
    #[cfg(feature = "mimalloc")]
    eprintln!("merge_fan_in: built with the mimalloc feature, allocation counts read zero");
    let mut perf = PerfControl::from_env()?;
    // `cargo test` runs a bench target too: one small round then.
    let measuring = std::env::args().any(|a| a == "--bench");
    let (n, rounds) = if measuring {
        (
            env_or("MERGE_FAN_IN_ENTRIES", 1_000_000).max(64),
            env_or("MERGE_FAN_IN_ROUNDS", 7).max(1),
        )
    } else {
        (4_000, 1)
    };
    let arms: Vec<Arm> = Arm::ALL
        .into_iter()
        .filter(|arm| picked("MERGE_FAN_IN_ARMS", arm.label()))
        .collect();
    for order in Order::ALL {
        if !picked("MERGE_FAN_IN_ORDERS", order.label()) {
            continue;
        }
        let comparator = order.comparator();
        for shape in Shape::ALL {
            if !picked("MERGE_FAN_IN_SHAPES", shape.label()) {
                continue;
            }
            for placement in Placement::ALL {
                if !picked("MERGE_FAN_IN_LAYOUTS", placement.label()) {
                    continue;
                }
                for fanin in FANINS {
                    if !picked("MERGE_FAN_IN_FANINS", &fanin.to_string()) {
                        continue;
                    }
                    let inputs = inputs(order, shape, placement, fanin, n);
                    let entries: usize = inputs.iter().map(Vec::len).sum();
                    let prefix = common_prefix(&inputs);
                    let label = format!(
                        "merge_fan_in/{}/{}/{}/f{fanin}",
                        order.label(),
                        shape.label(),
                        placement.label(),
                    );
                    check_arms_agree(&inputs, &comparator, prefix, &label)?;
                    let mut samples: Vec<Samples> =
                        arms.iter().map(|_| Samples::default()).collect();
                    for round in 0..rounds {
                        for turn in 0..arms.len() {
                            let at = (round + turn) % arms.len();
                            // The copy an arm consumes is made before its time
                            // starts.
                            let copy = inputs.clone();
                            let allocs_before = ALLOCS.load(Ordering::Relaxed);
                            if let Some(perf) = &mut perf {
                                perf.send("enable\n")?;
                            }
                            let start = Instant::now();
                            let mut seen = 0_usize;
                            let work = merge(arms[at], copy, &comparator, prefix, |entry| {
                                seen += 1;
                                std::hint::black_box(&entry);
                            })?;
                            let elapsed = start.elapsed();
                            if let Some(perf) = &mut perf {
                                perf.send("disable\n")?;
                            }
                            assert_eq!(seen, entries, "{label}: every entry is merged");
                            let held = &mut samples[at];
                            held.times.push(elapsed);
                            held.allocs
                                .push(ALLOCS.load(Ordering::Relaxed) - allocs_before);
                            held.work = work;
                        }
                    }
                    for (arm, mut held) in arms.iter().zip(samples) {
                        held.times.sort_unstable();
                        held.allocs.sort_unstable();
                        let ns_at = |num: usize, den: usize| {
                            let at = ((held.times.len() - 1) * num).div_ceil(den);
                            held.times[at].as_nanos() as f64 / entries as f64
                        };
                        print!(
                            "{label}/{}: {:.2} ns/entry, p99 {:.2}, {} allocs",
                            arm.label(),
                            ns_at(1, 2),
                            ns_at(99, 100),
                            held.allocs[held.allocs.len() / 2],
                        );
                        if cfg!(feature = "diagnostics") {
                            let per_entry = |count: u64| count as f64 / entries as f64;
                            print!(
                                ", {:.2} word compares, {:.2} key compares, {:.2} moves /entry",
                                per_entry(held.work.word_compares),
                                per_entry(held.work.key_compares),
                                per_entry(held.work.moves),
                            );
                        }
                        println!();
                    }
                }
            }
        }
    }
    Ok(())
}
