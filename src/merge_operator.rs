// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::UserValue;
use core::panic::RefUnwindSafe;

/// A user-defined merge operator for commutative LSM operations.
///
/// Merge operators enable efficient read-modify-write operations by storing
/// partial updates (operands) that are lazily combined during reads and
/// compaction, avoiding the need for explicit read-modify-write cycles.
///
/// # Implementor contract
///
/// The merge function must be **deterministic and stable across multiple
/// passes**. The `base_value` may itself be the result of a previous merge
/// (e.g., from compaction or an earlier read resolution) rather than the
/// original stored value. Repeated merging must produce identical bytes
/// for the same logical state.
///
/// # Examples
///
/// A simple counter merge operator that sums integer operands:
///
/// ```
/// use lsm_tree::{MergeOperator, UserValue};
///
/// struct CounterMerge;
///
/// impl MergeOperator for CounterMerge {
///     fn merge(
///         &self,
///         _key: &[u8],
///         base_value: Option<&[u8]>,
///         operands: &[&[u8]],
///     ) -> lsm_tree::Result<UserValue> {
///         let mut counter: i64 = match base_value {
///             Some(bytes) if bytes.len() == 8 => i64::from_le_bytes(
///                 bytes.try_into().expect("checked length"),
///             ),
///             Some(_) => return Err(lsm_tree::Error::MergeOperator),
///             None => 0,
///         };
///
///         for operand in operands {
///             if operand.len() != 8 {
///                 return Err(lsm_tree::Error::MergeOperator);
///             }
///             counter += i64::from_le_bytes(
///                 (*operand).try_into().expect("checked length"),
///             );
///         }
///
///         Ok(counter.to_le_bytes().to_vec().into())
///     }
/// }
/// ```
pub trait MergeOperator: Send + Sync + RefUnwindSafe + 'static {
    /// Merges operands with an optional base value.
    ///
    /// `key` is the user key being merged.
    ///
    /// `base_value` is the existing value for the key, or `None` when the key
    /// has none: it was never written, or a delete removed it. `None` always
    /// means absence, never "the engine did not look": a read resolves over
    /// every level before calling this, and a compaction calls it only where it
    /// found the boundary or holds every surviving version of the key, so
    /// nothing outside it can hold a base. Where a compaction can prove
    /// neither, it keeps the operands instead of asking. The value may already
    /// be the output of a
    /// previous `merge` call (after compaction or an earlier read), so
    /// implementations must be stable when re-merging.
    ///
    /// `operands` contains the merge operand values in ascending sequence
    /// number order (chronological — oldest first).
    ///
    /// Returns the merged value on success.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::MergeOperator`] if the merge fails (e.g., corrupted
    /// operand data).
    fn merge(
        &self,
        key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> crate::Result<UserValue>;

    /// Whether folding a PREFIX of the operand chain, with no base, yields
    /// something that is still a valid operand.
    ///
    /// A key written only through [`crate::AbstractTree::merge`] never gets a
    /// base, so a compaction that holds neither a boundary nor every surviving
    /// version of the key cannot prove what the base is. It then keeps the
    /// operands and every read re-applies the whole chain, which for a
    /// continuously written key grows without bound: only a compaction spanning
    /// every level ever folds it.
    ///
    /// Returning `true` lets such a compaction fold the operands it holds into
    /// one composed operand instead. It is opt-in and defaults to `false`,
    /// because for most operators it would be wrong: an operand that edits a
    /// base is not the same thing as the state it produces against an empty
    /// one. A set removal folded onto an assumed-empty set is an empty set, and
    /// an empty set later meeting the real set is not a removal; a document
    /// patch becomes a whole record, and that record overwrites the base it was
    /// meant to amend.
    ///
    /// # Obligation
    ///
    /// Three properties, not just "my operator looks associative". Write
    /// `f(base, ops)` for `self.merge(key, base, ops)`, and let `P` be any
    /// non-empty prefix of a chain and `S` the rest of it.
    ///
    /// 1. **Closure.** `f(None, P)` is itself a valid operand: it can be fed
    ///    back into `f` in operand position, and re-folding it is stable.
    /// 2. **Identity compatibility.** `f(None, P)` represents exactly the
    ///    composition of `P`, not the state `P` would produce against an empty
    ///    base. The two coincide for a delta; for an edit against a base they
    ///    do not, which is what makes this opt-in.
    /// 3. **Composition law.** For every admissible base `B` (including
    ///    `None`), every prefix `P` and every suffix `S`:
    ///
    /// ```text
    /// f(B, [f(None, P), ...S])  ==  f(B, [...P, ...S])
    /// ```
    ///
    /// 4. **Order independence.** Operands must combine to the same result
    ///    whatever order they are applied in. A compaction sees the operands in
    ///    the tables it selected, and a strategy may select some runs and not
    ///    others (`SizeTiered` picks by size), so the operands it holds are not
    ///    always a contiguous run of sequence numbers. Composing 1 and 3 while
    ///    2 sits in a table left out produces an operand at 3, and the read
    ///    then applies 2 before it. For a sum that is the same answer; for a
    ///    concatenation it is `213` where the chain says `123`. The engine
    ///    cannot see what it did not select, so this one is on the operator.
    ///
    /// A sum of deltas satisfies all four, because a sum of deltas is itself a
    /// delta, carries no notion of the base it will land on, and does not care
    /// in which order the deltas arrive. An operator that cannot state the law
    /// above for arbitrary `B`, `P` and `S`, or whose result depends on operand
    /// order, must leave this `false`.
    ///
    /// Getting this wrong does not cost performance, it costs correctness: the
    /// composed operand is written to disk and later folded onto a real base,
    /// so a `true` that does not hold produces silently wrong state rather than
    /// an error.
    ///
    /// # Partial operators
    ///
    /// The three properties are about the values a successful merge returns, so
    /// an operator that can REFUSE is not held to them when it refuses. It does
    /// not have to be total, and its failures do not have to be independent of
    /// how the chain is bracketed. Composition changes which intermediate
    /// results exist, so checked arithmetic can legitimately refuse a prefix
    /// whose whole chain against the real base is in range: summing
    /// `[i64::MAX, 1]` overflows on its own, while the same operands applied to
    /// a base of `-1` do not.
    ///
    /// The engine treats a refused composition as a composition it will not do:
    /// the operands are re-emitted and the fold happens where the base is,
    /// exactly as it would for an operator that left this `false`. A failing
    /// composition therefore costs the optimisation for that key and nothing
    /// else, and never fails a compaction that would otherwise have succeeded.
    ///
    /// The freedom is one-directional, and this part IS an obligation. A
    /// composition that SUCCEEDS is written to disk in place of the operands it
    /// replaces, and nothing can bring them back, so a refusal that appears only
    /// after composing is permanent: a read or a later proven-base compaction
    /// that used to succeed now fails. So whenever `f(None, P)` succeeds and
    /// `f(B, [...P, ...S])` succeeds, `f(B, [f(None, P), ...S])` must succeed
    /// too, and equal it. Refusing earlier than the un-composed chain would is
    /// free; refusing later than it would is not allowed.
    ///
    /// Checked arithmetic satisfies this without extra care, because the
    /// composed step lands on a total the un-composed chain also passes
    /// through: if every one of its intermediates is in range, so is that one.
    ///
    /// Folding stays gated on the GC watermark either way: the composed operand
    /// carries the head's sequence number, so only operands the watermark has
    /// already certified as collapsible take part, and no live snapshot can
    /// read between them.
    fn composes_operands(&self) -> bool {
        false
    }
}
