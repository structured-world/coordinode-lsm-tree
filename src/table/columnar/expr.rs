// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The encoding of one column page's values: an expression over light
//! operators, chosen per page by what it costs to store and to read.
//!
//! A page holds one column's values for one row page. They are written as an
//! expression: an operator, its own fields, and the expressions of the vectors
//! it is made of. A dictionary is its distinct values and a code per row, and
//! each of those is an expression again; so is a run's value and where it
//! ends. The operators:
//!
//! - **Constant**: one value, for every row.
//! - **RLE**: runs of equal values.
//! - **Dictionary**: the distinct values once, in the column's order, and a
//!   code per row. Being sorted, a range over the values is a range over the
//!   codes, so a predicate is answered from the dictionary and the codes.
//! - **FFOR**: integers as their offset from a base in the bits the offsets
//!   need, fused rather than a frame of reference followed by bit packing, with
//!   the values that do not fit patched in as exceptions, so one outlier does
//!   not widen every row.
//! - **Delta**: integers as the zigzagged difference from the one before.
//! - **Ordinals**: a number column's values as integers in their order
//!   ([`Number::ordinal`](super::Number)), which is what makes a signed or
//!   float column's narrow range a narrow range of integers.
//! - **Lengths**: a bytes column's values as their lengths and their bytes,
//!   in place of the offset table.
//! - **Plain**: the column's own layout, served as a view of the page.
//!
//! All of one page's operators live in the page: splitting them into pages of
//! their own would buy a read nothing, since no read wants a dictionary without
//! its codes or a bit-packed vector without its exceptions, and would cost
//! every part a block frame and a directory entry per row page.
//!
//! # Wire format
//!
//! Counts, lengths and integers are LEB128 varints (`var`).
//!
//! ```text
//! values(type, n):
//!   [op: u8]
//!   0 PLAIN     the column's own layout: n * width bytes, or a bytes column's
//!               (n + 1) little-endian u32 offsets and its payload
//!   1 CONSTANT  one value: width bytes, or [len: var][bytes] for a bytes column
//!   2 RLE       [runs: var] values(type, runs) ints(runs)
//!               the runs' values, then their ends: the row after each run
//!   3 DICT      [size: var] values(type, size) ints(n)
//!               the distinct values in the column's order, then a code per row
//!   5 ORDINALS  ints(n)  a number column of at most 8 bytes, by its ordinals
//!   7 LENGTHS   ints(n) [payload_len: var] [payload]  a bytes column
//! ints(n):
//!   [op: u8]
//!   1 CONSTANT  [value: var]
//!   2 RLE       [runs: var] ints(runs) ints(runs)  values, then ends
//!   4 FFOR      [base: var] [bit_width: u8] [packed: ceil(n * bit_width / 8)]
//!               [exceptions: var], each [gap: var] [value: var]
//!   6 DELTA     ints(n)  zigzag of each value minus the one before it
//! ```
//!
//! A run's or a dictionary's values are one of PLAIN, ORDINALS or LENGTHS. An
//! FFOR row is `base + offset`, its offsets packed least significant bit
//! first; an exception replaces the row at its position with its value, and
//! positions strictly increase, each written as the gap from the one before
//! (minus one after the first). Expressions nest at most [`MAX_DEPTH`] deep.
//!
//! A reader refuses an operator it does not know, an operator where the
//! column's type has no use for it, and every length, count, code, end or
//! position the rows cannot hold, rather than decoding part of a page.

use super::{
    Number, TypeTag, bytes_column_row, check_bytes_framing, frame_bytes_column,
    frame_bytes_column_within,
};
use crate::config::ColumnEncoding;
use crate::table::column_page::{VAR_U64_MAX_LEN, put_varint, take, take_slice, take_varint};
use crate::table::columnar_predicate::Selection;
use crate::{Error, Result, Slice};
use alloc::{boxed::Box, vec::Vec};
use core::fmt;

/// How deep an expression nests: a dictionary whose codes run-length encode
/// and whose ends delta-encode into FFOR is four levels, the deepest shape
/// the writer chooses.
pub const MAX_DEPTH: u8 = 5;

const PLAIN: u8 = 0;
const CONSTANT: u8 = 1;
const RLE: u8 = 2;
const DICT: u8 = 3;
const FFOR: u8 = 4;
const ORDINALS: u8 = 5;
const DELTA: u8 = 6;
const LENGTHS: u8 = 7;

/// The refusal every malformed encoding gets, built only where it is
/// returned.
const MALFORMED: Error = Error::InvalidHeader("columnar: malformed column encoding");

/// What a column page's values were encoded as: the operators, nested as they
/// were applied. Read back from a page and shown by `sst-dump`, so a bad
/// automatic choice can be diagnosed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Expression {
    /// The column's own layout.
    Plain,
    /// One value for every row.
    Constant,
    /// Runs of equal values: each run's value, and where each run ends.
    Rle {
        /// The runs' values.
        values: Box<Self>,
        /// The row after each run.
        ends: Box<Self>,
    },
    /// The distinct values once, in the column's order, and a code per row.
    Dict {
        /// The distinct values.
        values: Box<Self>,
        /// Each row's position among them.
        codes: Box<Self>,
    },
    /// Integers as their offset from a base, in `bit_width` bits, with the
    /// values that do not fit patched in.
    Ffor {
        /// The bits every offset is stored in.
        bit_width: u8,
        /// The rows stored whole instead.
        exceptions: u32,
    },
    /// A number column's values as integers in their order.
    Ordinals(Box<Self>),
    /// Integers as the zigzagged difference from the one before.
    Delta(Box<Self>),
    /// A bytes column's values as their lengths and their bytes.
    Lengths(Box<Self>),
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Plain => f.write_str("plain"),
            Self::Constant => f.write_str("constant"),
            Self::Rle { values, ends } => write!(f, "rle(values={values}, ends={ends})"),
            Self::Dict { values, codes } => write!(f, "dict(values={values}, codes={codes})"),
            Self::Ffor {
                bit_width,
                exceptions: 0,
            } => write!(f, "ffor({bit_width})"),
            Self::Ffor {
                bit_width,
                exceptions,
            } => write!(f, "ffor({bit_width}, {exceptions} exceptions)"),
            Self::Ordinals(ints) => write!(f, "ordinals({ints})"),
            Self::Delta(ints) => write!(f, "delta({ints})"),
            Self::Lengths(ints) => write!(f, "lengths({ints})"),
        }
    }
}

// --- Reading -----------------------------------------------------------------

/// An FFOR vector as stored: its fields and its packed bits, borrowed from the
/// page.
#[derive(Clone, Debug)]
pub struct Ffor<'a> {
    base: u64,
    bit_width: u8,
    packed: &'a [u8],
    exception_count: u32,
    /// The `(gap, value)` pairs, already walked once by the parse.
    exceptions: &'a [u8],
}

/// An integer vector as stored.
#[derive(Clone, Debug)]
pub enum Ints<'a> {
    /// One value for every row.
    Constant(u64),
    /// Offsets from a base, with exceptions.
    Ffor(Ffor<'a>),
    /// Zigzagged differences from the value before.
    Delta(Box<Self>),
    /// Runs of equal integers; `ends` are decoded, since every read of the
    /// vector needs them and the parse proves them in order.
    Rle {
        values: Box<Self>,
        ends: Vec<u32>,
        /// The ends as stored, kept to describe them.
        stored_ends: &'a [u8],
    },
}

/// A column page's values as stored, borrowed from the page.
#[derive(Clone, Debug)]
pub enum Values<'a> {
    /// The column's own layout, checked for its framing.
    Plain(&'a [u8]),
    /// The one value of every row.
    Constant(&'a [u8]),
    /// The runs' values, `ends.len()` of them, and the row after each run.
    Rle {
        values: Box<Self>,
        ends: Vec<u32>,
        /// The ends as stored, kept to describe them.
        stored_ends: &'a [u8],
    },
    /// `size` distinct values in the column's order and a code per row.
    Dict {
        values: Box<Self>,
        size: u32,
        codes: Ints<'a>,
    },
    /// A number column's values by their ordinals.
    Ordinals(Ints<'a>),
    /// A bytes column's values as their lengths and their bytes.
    Lengths {
        lengths: Ints<'a>,
        /// The bytes the lengths are stored in, what replaces the offset
        /// table.
        lengths_len: usize,
        payload: &'a [u8],
    },
}

/// Takes a `u32` varint.
fn var_u32(rest: &mut &[u8]) -> Result<u32> {
    let Some(value) = take_varint(rest, VAR_U64_MAX_LEN) else {
        return Err(MALFORMED);
    };
    u32::try_from(value).map_err(|_| MALFORMED)
}

/// Takes a `u64` varint.
fn var_u64(rest: &mut &[u8]) -> Result<u64> {
    let Some(value) = take_varint(rest, VAR_U64_MAX_LEN) else {
        return Err(MALFORMED);
    };
    Ok(value)
}

/// Takes one byte.
fn byte(rest: &mut &[u8]) -> Result<u8> {
    let Some([b]) = take::<1>(rest) else {
        return Err(MALFORMED);
    };
    Ok(b)
}

/// Takes `len` bytes.
fn bytes<'a>(rest: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    let Some(slice) = take_slice(rest, len) else {
        return Err(MALFORMED);
    };
    Ok(slice)
}

/// The bytes `n` offsets of `bit_width` bits pack into, refused when a
/// target's address space cannot hold them.
fn packed_len(n: u32, bit_width: u8) -> Result<usize> {
    // `n` is a u32 and a width at most 64, so the product fits a u64.
    usize::try_from((u64::from(n) * u64::from(bit_width)).div_ceil(8)).map_err(|_| MALFORMED)
}

/// Checks that `ends` are the ends of runs covering exactly `n` rows: each
/// past the one before, the last `n`.
fn check_ends(ends: &[u32], n: u32) -> Result<()> {
    let mut prev = 0u32;
    for &end in ends {
        if end <= prev {
            return Err(MALFORMED);
        }
        prev = end;
    }
    if prev != n {
        return Err(MALFORMED);
    }
    Ok(())
}

impl<'a> Ints<'a> {
    /// Reads an integer vector of `n` values off the front of `rest`.
    fn parse(rest: &mut &'a [u8], n: u32, depth: u8) -> Result<Self> {
        if depth > MAX_DEPTH {
            return Err(MALFORMED);
        }
        match byte(rest)? {
            CONSTANT => Ok(Self::Constant(var_u64(rest)?)),
            FFOR => {
                let base = var_u64(rest)?;
                let bit_width = byte(rest)?;
                if bit_width > 64 {
                    return Err(MALFORMED);
                }
                let packed = bytes(rest, packed_len(n, bit_width)?)?;
                let exception_count = var_u32(rest)?;
                if exception_count > n {
                    return Err(MALFORMED);
                }
                let start = *rest;
                let mut next = 0u64;
                for _ in 0..exception_count {
                    let Some(position) = next.checked_add(var_u64(rest)?) else {
                        return Err(MALFORMED);
                    };
                    if position >= u64::from(n) {
                        return Err(MALFORMED);
                    }
                    var_u64(rest)?;
                    next = position + 1;
                }
                let used = start.len() - rest.len();
                Ok(Self::Ffor(Ffor {
                    base,
                    bit_width,
                    packed,
                    exception_count,
                    exceptions: start.get(..used).unwrap_or_default(),
                }))
            }
            DELTA => Ok(Self::Delta(Box::new(Self::parse(rest, n, depth + 1)?))),
            RLE => {
                let runs = var_u32(rest)?;
                if runs == 0 || runs > n {
                    return Err(MALFORMED);
                }
                let values = Self::parse(rest, runs, depth + 1)?;
                let (ends, stored_ends) = parse_ends(rest, runs, n, depth + 1)?;
                Ok(Self::Rle {
                    values: Box::new(values),
                    ends,
                    stored_ends,
                })
            }
            _ => Err(MALFORMED),
        }
    }

    /// Appends the vector's `n` values to `out`.
    pub(crate) fn decode_into(&self, n: u32, out: &mut Vec<u64>) -> Result<()> {
        match self {
            Self::Constant(value) => {
                out.resize(out.len() + n as usize, *value);
                Ok(())
            }
            Self::Ffor(ffor) => ffor.decode_into(n, out),
            Self::Delta(inner) => {
                let start = out.len();
                inner.decode_into(n, out)?;
                let mut prev = 0u64;
                for value in out.get_mut(start..).unwrap_or_default() {
                    prev = prev.wrapping_add(unzigzag(*value));
                    *value = prev;
                }
                Ok(())
            }
            Self::Rle { values, ends, .. } => {
                let mut runs = Vec::with_capacity(ends.len());
                values.decode_into(u32::try_from(ends.len()).map_err(|_| MALFORMED)?, &mut runs)?;
                let mut start = 0u32;
                for (&value, &end) in runs.iter().zip(ends) {
                    out.resize(out.len() + (end - start) as usize, value);
                    start = end;
                }
                Ok(())
            }
        }
    }

    /// The run ends the parse holds decoded, at any depth.
    fn held_rows(&self) -> u64 {
        match self {
            Self::Constant(_) | Self::Ffor(_) => 0,
            Self::Delta(inner) => inner.held_rows(),
            Self::Rle { values, ends, .. } => ends.len() as u64 + values.held_rows(),
        }
    }

    /// What the vector was encoded as.
    ///
    /// # Errors
    ///
    /// As [`describe_ends`].
    fn describe(&self) -> Result<Expression> {
        Ok(match self {
            Self::Constant(_) => Expression::Constant,
            Self::Ffor(ffor) => Expression::Ffor {
                bit_width: ffor.bit_width,
                exceptions: ffor.exception_count,
            },
            Self::Delta(inner) => Expression::Delta(Box::new(inner.describe()?)),
            Self::Rle {
                values,
                ends,
                stored_ends,
            } => Expression::Rle {
                values: Box::new(values.describe()?),
                ends: Box::new(describe_ends(stored_ends, ends.len())?),
            },
        })
    }
}

/// Reads a run's ends, `runs` of them covering `n` rows, off the front of
/// `rest`: decoded, since every read of the runs needs them and the parse
/// proves them in order, and the bytes they were stored in.
fn parse_ends<'a>(
    rest: &mut &'a [u8],
    runs: u32,
    n: u32,
    depth: u8,
) -> Result<(Vec<u32>, &'a [u8])> {
    let start = *rest;
    let ends = Ints::parse(rest, runs, depth)?;
    let stored = start.get(..start.len() - rest.len()).unwrap_or_default();
    let mut decoded = Vec::with_capacity(runs as usize);
    ends.decode_into(runs, &mut decoded)?;
    let ends: Vec<u32> = decoded
        .into_iter()
        .map(|end| u32::try_from(end).map_err(|_| MALFORMED))
        .collect::<Result<_>>()?;
    check_ends(&ends, n)?;
    Ok((ends, stored))
}

/// What a run's ends, `runs` of them stored in `stored`, were encoded as. A
/// read keeps them decoded and describes them only when asked, from the bytes
/// the parse already accepted.
///
/// # Errors
///
/// [`Error::InvalidHeader`] if `stored` no longer parses, which only a
/// caller other than the parse could cause.
fn describe_ends(stored: &[u8], runs: usize) -> Result<Expression> {
    let mut rest = stored;
    let runs = u32::try_from(runs).map_err(|_| MALFORMED)?;
    Ints::parse(&mut rest, runs, 0)?.describe()
}

impl Ffor<'_> {
    /// Appends the vector's `n` values to `out`: the packed offsets added to
    /// the base, then the exceptions put in their rows.
    fn decode_into(&self, n: u32, out: &mut Vec<u64>) -> Result<()> {
        let start = out.len();
        out.reserve(n as usize);
        let width = u32::from(self.bit_width);
        if width == 0 {
            out.resize(start + n as usize, self.base);
        } else {
            let mask = if width == 64 {
                u64::MAX
            } else {
                (1u64 << width) - 1
            };
            // A 128-bit window refilled a byte at a time holds every offset
            // whole, whatever its alignment, for widths up to 64.
            let mut window = 0u128;
            let mut held = 0u32;
            let mut packed = self.packed.iter();
            for _ in 0..n {
                while held < width {
                    let Some(&b) = packed.next() else {
                        return Err(MALFORMED);
                    };
                    window |= u128::from(b) << held;
                    held += 8;
                }
                // Masked to `width` bits, so the narrowing keeps the offset.
                #[expect(clippy::cast_possible_truncation, reason = "masked to 64 bits")]
                let offset = (window as u64) & mask;
                window >>= width;
                held -= width;
                let Some(value) = self.base.checked_add(offset) else {
                    return Err(MALFORMED);
                };
                out.push(value);
            }
        }
        let mut rest = self.exceptions;
        let mut next = 0u64;
        for _ in 0..self.exception_count {
            let position = next + var_u64(&mut rest)?;
            let value = var_u64(&mut rest)?;
            let Some(slot) = usize::try_from(position)
                .ok()
                .and_then(|p| out.get_mut(start + p))
            else {
                return Err(MALFORMED);
            };
            *slot = value;
            next = position + 1;
        }
        Ok(())
    }
}

impl<'a> Values<'a> {
    /// Reads the values of `n` rows of a column of `type_tag`: the whole of
    /// `bytes`, which is refused if anything is left over.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidHeader`] for an encoding that is malformed, uses an
    /// operator the type has no use for, or does not fill `bytes` exactly.
    pub(crate) fn parse(type_tag: TypeTag, n: u32, bytes: &'a [u8]) -> Result<Self> {
        let mut rest = bytes;
        let values = Self::parse_from(&mut rest, type_tag, n, 0, true)?;
        if !rest.is_empty() {
            return Err(MALFORMED);
        }
        Ok(values)
    }

    /// Reads a values expression off the front of `rest`. `outer` is false
    /// for a run's or a dictionary's values, which may not nest those again.
    fn parse_from(
        rest: &mut &'a [u8],
        type_tag: TypeTag,
        n: u32,
        depth: u8,
        outer: bool,
    ) -> Result<Self> {
        if depth > MAX_DEPTH {
            return Err(MALFORMED);
        }
        let op = byte(rest)?;
        match (op, type_tag.fixed_width()) {
            (PLAIN, Some(width)) => {
                let Some(len) = (n as usize).checked_mul(usize::from(width)) else {
                    return Err(MALFORMED);
                };
                Ok(Self::Plain(bytes(rest, len)?))
            }
            (PLAIN, None) => {
                // The offset table's last entry is the payload length, so the
                // table is read first and the payload after it.
                let Some(table) = (n as usize).checked_add(1).and_then(|c| c.checked_mul(4)) else {
                    return Err(MALFORMED);
                };
                let offsets = rest.get(..table).ok_or(MALFORMED)?;
                let Some(last) = offsets.last_chunk::<4>() else {
                    return Err(MALFORMED);
                };
                let payload = u32::from_le_bytes(*last) as usize;
                let data = bytes(rest, table.checked_add(payload).ok_or(MALFORMED)?)?;
                check_bytes_framing(data, n)?;
                Ok(Self::Plain(data))
            }
            (CONSTANT, Some(width)) if outer => {
                Ok(Self::Constant(bytes(rest, usize::from(width))?))
            }
            (CONSTANT, None) if outer => {
                let len = var_u32(rest)? as usize;
                Ok(Self::Constant(bytes(rest, len)?))
            }
            (RLE, _) if outer => {
                let runs = var_u32(rest)?;
                if runs == 0 || runs > n {
                    return Err(MALFORMED);
                }
                let values = Self::parse_from(rest, type_tag, runs, depth + 1, false)?;
                let (ends, stored_ends) = parse_ends(rest, runs, n, depth + 1)?;
                Ok(Self::Rle {
                    values: Box::new(values),
                    ends,
                    stored_ends,
                })
            }
            (DICT, _) if outer => {
                let size = var_u32(rest)?;
                if size == 0 || size > n {
                    return Err(MALFORMED);
                }
                let values = Self::parse_from(rest, type_tag, size, depth + 1, false)?;
                let codes = Ints::parse(rest, n, depth + 1)?;
                Ok(Self::Dict {
                    values: Box::new(values),
                    size,
                    codes,
                })
            }
            (ORDINALS, Some(width)) if ordinal_width(type_tag) == Some(width) => {
                Ok(Self::Ordinals(Ints::parse(rest, n, depth + 1)?))
            }
            (LENGTHS, None) => {
                let before = rest.len();
                let lengths = Ints::parse(rest, n, depth + 1)?;
                let lengths_len = before - rest.len();
                let len = var_u32(rest)? as usize;
                Ok(Self::Lengths {
                    lengths,
                    lengths_len,
                    payload: bytes(rest, len)?,
                })
            }
            _ => Err(MALFORMED),
        }
    }

    /// The run ends the parse holds decoded, at any depth: what a parsed
    /// page keeps allocated until it is dropped.
    pub(crate) fn held_rows(&self) -> u64 {
        match self {
            Self::Plain(_) | Self::Constant(_) => 0,
            Self::Rle { values, ends, .. } => ends.len() as u64 + values.held_rows(),
            Self::Dict { values, codes, .. } => values.held_rows() + codes.held_rows(),
            Self::Ordinals(ints) | Self::Lengths { lengths: ints, .. } => ints.held_rows(),
        }
    }

    /// What the values were encoded as, a run's ends as they were stored.
    ///
    /// # Errors
    ///
    /// As [`describe_ends`].
    pub(crate) fn describe(&self) -> Result<Expression> {
        Ok(match self {
            Self::Plain(_) => Expression::Plain,
            Self::Constant(_) => Expression::Constant,
            Self::Rle {
                values,
                ends,
                stored_ends,
            } => Expression::Rle {
                values: Box::new(values.describe()?),
                ends: Box::new(describe_ends(stored_ends, ends.len())?),
            },
            Self::Dict { values, codes, .. } => Expression::Dict {
                values: Box::new(values.describe()?),
                codes: Box::new(codes.describe()?),
            },
            Self::Ordinals(ints) => Expression::Ordinals(Box::new(ints.describe()?)),
            Self::Lengths { lengths, .. } => Expression::Lengths(Box::new(lengths.describe()?)),
        })
    }

    /// The bytes a bytes column of `n` rows spends on where its values
    /// start, apart from the values themselves: the offset table of a plain
    /// page or the lengths that replace it. `None` for a column of another
    /// type, or one whose encoding keeps no per-row position (a constant, a
    /// dictionary, runs).
    pub(crate) fn offsets_len(&self, type_tag: TypeTag, n: u32) -> Option<usize> {
        match (self, type_tag) {
            (Self::Plain(_), TypeTag::Bytes) => Some((n as usize + 1) * 4),
            (Self::Lengths { lengths_len, .. }, _) => Some(*lengths_len),
            _ => None,
        }
    }

    /// The column's layout for its `n` rows: the page itself for
    /// [`Self::Plain`], a buffer built from the encoding otherwise, refused
    /// past `limit` bytes before it is allocated.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidHeader`] when the encoding does not describe `n` rows:
    /// a code past its dictionary, an ordinal no value has, or lengths that
    /// do not add up to the payload. [`Error::DecompressedSizeTooLarge`] for
    /// a bytes column built past `limit`.
    pub(crate) fn materialize(
        &self,
        type_tag: TypeTag,
        n: u32,
        page: &Slice,
        limit: u64,
    ) -> Result<Slice> {
        if !matches!(self, Self::Plain(_)) {
            fixed_within(type_tag, n, limit)?;
        }
        match self {
            Self::Plain(data) => Ok(view_of(page, data)),
            Self::Constant(value) => repeat(type_tag, n, value, limit),
            Self::Rle { values, ends, .. } => {
                let runs = u32::try_from(ends.len()).map_err(|_| MALFORMED)?;
                let heads = values.materialize(type_tag, runs, page, limit)?;
                let mut run = 0usize;
                let mut rows = Vec::with_capacity(n as usize);
                for row in 0..n {
                    while ends.get(run).is_some_and(|&end| row >= end) {
                        run += 1;
                    }
                    rows.push(u32::try_from(run).map_err(|_| MALFORMED)?);
                }
                gather(type_tag, &heads, runs, &rows, limit)
            }
            Self::Dict {
                values,
                size,
                codes,
            } => {
                let dictionary = values.materialize(type_tag, *size, page, limit)?;
                let mut decoded = Vec::with_capacity(n as usize);
                codes.decode_into(n, &mut decoded)?;
                let rows: Vec<u32> = decoded
                    .into_iter()
                    .map(|code| {
                        u32::try_from(code)
                            .ok()
                            .filter(|&c| c < *size)
                            .ok_or(MALFORMED)
                    })
                    .collect::<Result<_>>()?;
                gather(type_tag, &dictionary, *size, &rows, limit)
            }
            Self::Ordinals(ints) => {
                let TypeTag::Number(number) = type_tag else {
                    return Err(MALFORMED);
                };
                let mut ordinals = Vec::with_capacity(n as usize);
                ints.decode_into(n, &mut ordinals)?;
                from_ordinals(number, &ordinals)
            }
            Self::Lengths {
                lengths, payload, ..
            } => {
                let mut decoded = Vec::with_capacity(n as usize);
                lengths.decode_into(n, &mut decoded)?;
                let mut at = 0usize;
                let cells: Vec<&[u8]> = decoded
                    .iter()
                    .map(|&len| {
                        let end = usize::try_from(len)
                            .ok()
                            .and_then(|len| at.checked_add(len))
                            .ok_or(MALFORMED)?;
                        let cell = payload.get(at..end).ok_or(MALFORMED)?;
                        at = end;
                        Ok(cell)
                    })
                    .collect::<Result<_>>()?;
                if at != payload.len() {
                    return Err(MALFORMED);
                }
                // Every cell is a distinct span of the payload, but the
                // offset table still grows with the rows the lengths declare.
                frame_bytes_column_within(cells.len(), limit, || cells.iter().copied())
            }
        }
    }
}

// --- Filtering -----------------------------------------------------------------

/// What a filter asks of a column's values, in the form its encoding compares.
#[derive(Clone, Copy, Debug)]
pub enum Bounds<'b> {
    /// An inclusive byte-wise range over a bytes column's values, either side
    /// unbounded when `None`.
    Bytes {
        /// The lowest value kept.
        lower: Option<&'b [u8]>,
        /// The highest value kept.
        upper: Option<&'b [u8]>,
    },
    /// An inclusive range of a number column's ordinals.
    Ordinals {
        /// The column's number type.
        number: Number,
        /// The lowest ordinal kept.
        lo: u128,
        /// The highest ordinal kept.
        hi: u128,
    },
    /// No value is kept.
    Nothing,
}

impl Bounds<'_> {
    /// Whether one value, in its column's layout, is kept.
    fn keeps(&self, cell: &[u8]) -> bool {
        match *self {
            Self::Bytes { lower, upper } => {
                lower.is_none_or(|lo| cell >= lo) && upper.is_none_or(|hi| cell <= hi)
            }
            Self::Ordinals { number, lo, hi } => {
                let ordinal = number.ordinal(cell);
                lo <= ordinal && ordinal <= hi
            }
            Self::Nothing => false,
        }
    }

    /// Whether one ordinal is kept.
    fn keeps_ordinal(&self, ordinal: u64) -> bool {
        match *self {
            Self::Ordinals { lo, hi, .. } => {
                let ordinal = u128::from(ordinal);
                lo <= ordinal && ordinal <= hi
            }
            Self::Bytes { .. } | Self::Nothing => false,
        }
    }
}

impl Ints<'_> {
    /// The rows of this vector of `n` values that `bounds` keeps, comparing
    /// the integers as they decode: a constant is compared once and a run
    /// once per run, never per row.
    fn select(&self, n: u32, bounds: &Bounds<'_>) -> Result<Selection> {
        match self {
            Self::Constant(value) => Ok(if bounds.keeps_ordinal(*value) {
                Selection::all(n)
            } else {
                Selection::none(n)
            }),
            Self::Rle { values, ends, .. } => {
                let runs = u32::try_from(ends.len()).map_err(|_| MALFORMED)?;
                let kept = values.select(runs, bounds)?;
                let mut out = Selection::none(n);
                let mut start = 0u32;
                for (run, &end) in (0u32..).zip(ends) {
                    if kept.contains(run) {
                        out.insert_range(start..end);
                    }
                    start = end;
                }
                Ok(out)
            }
            Self::Ffor(_) | Self::Delta(_) => {
                let mut values = Vec::with_capacity(n as usize);
                self.decode_into(n, &mut values)?;
                let mut out = Selection::none(n);
                for (row, value) in (0u32..).zip(values) {
                    if bounds.keeps_ordinal(value) {
                        out.insert(row);
                    }
                }
                Ok(out)
            }
        }
    }
}

impl Values<'_> {
    /// The rows of these `n` values of a column of `type_tag` that `bounds`
    /// keeps, answered from the encoding: a constant with one comparison, runs
    /// with one per run, a dictionary with one per distinct value and a code
    /// lookup per row, integers as they decode. No value is materialized, and
    /// a null row is the caller's to drop, from the validity it holds.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidHeader`] when the encoding does not describe `n` rows.
    pub(crate) fn select(
        &self,
        type_tag: TypeTag,
        n: u32,
        bounds: &Bounds<'_>,
    ) -> Result<Selection> {
        if matches!(bounds, Bounds::Nothing) {
            return Ok(Selection::none(n));
        }
        match self {
            Self::Plain(data) => Ok(select_plain(type_tag, data, n, bounds)),
            Self::Constant(value) => Ok(if bounds.keeps(value) {
                Selection::all(n)
            } else {
                Selection::none(n)
            }),
            Self::Rle { values, ends, .. } => {
                let runs = u32::try_from(ends.len()).map_err(|_| MALFORMED)?;
                let kept = values.select(type_tag, runs, bounds)?;
                let mut out = Selection::none(n);
                let mut start = 0u32;
                for (run, &end) in (0u32..).zip(ends) {
                    if kept.contains(run) {
                        out.insert_range(start..end);
                    }
                    start = end;
                }
                Ok(out)
            }
            Self::Dict {
                values,
                size,
                codes,
            } => {
                // A dictionary is sorted, but the kept codes are found by
                // testing each entry: a range over a sorted dictionary is a
                // run of codes either way, and testing does not trust the sort.
                let kept = values.select(type_tag, *size, bounds)?;
                if kept.count() == 0 {
                    return Ok(Selection::none(n));
                }
                let mut decoded = Vec::with_capacity(n as usize);
                codes.decode_into(n, &mut decoded)?;
                let mut out = Selection::none(n);
                for (row, code) in (0u32..).zip(decoded) {
                    let Some(code) = u32::try_from(code).ok().filter(|c| c < size) else {
                        return Err(MALFORMED);
                    };
                    if kept.contains(code) {
                        out.insert(row);
                    }
                }
                Ok(out)
            }
            Self::Ordinals(ints) => ints.select(n, bounds),
            Self::Lengths {
                lengths, payload, ..
            } => {
                let mut decoded = Vec::with_capacity(n as usize);
                lengths.decode_into(n, &mut decoded)?;
                let mut out = Selection::none(n);
                let mut at = 0usize;
                for (row, len) in (0u32..).zip(decoded) {
                    let end = usize::try_from(len)
                        .ok()
                        .and_then(|len| at.checked_add(len))
                        .ok_or(MALFORMED)?;
                    if bounds.keeps(payload.get(at..end).ok_or(MALFORMED)?) {
                        out.insert(row);
                    }
                    at = end;
                }
                if at != payload.len() {
                    return Err(MALFORMED);
                }
                Ok(out)
            }
        }
    }
}

// --- Row access ----------------------------------------------------------------

/// One row's value: a view of the page, or a number rebuilt from its ordinal,
/// which has no bytes in the page to view.
#[derive(Clone, Copy, Debug)]
pub enum Cell<'a> {
    /// The value's bytes, in the page.
    Borrowed(&'a [u8]),
    /// A value rebuilt in place, its first `len` bytes.
    Inline {
        /// The bytes, the value's first.
        bytes: [u8; 16],
        /// How many of them are the value.
        len: u8,
    },
}

impl core::ops::Deref for Cell<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Inline { bytes, len } => bytes.get(..usize::from(*len)).unwrap_or_default(),
        }
    }
}

/// An integer vector read one value at a time: what a lookup of a few rows
/// needs, without decoding the rest.
#[derive(Clone, Debug)]
pub enum IntRows<'a> {
    /// One value for every row.
    Constant(u64),
    /// Offsets from a base in `bit_width` bits, the exceptions sorted by row.
    Ffor {
        /// Added to every offset.
        base: u64,
        /// The bits each offset takes.
        bit_width: u8,
        /// The packed offsets.
        packed: &'a [u8],
        /// The rows stored whole, by row.
        exceptions: Vec<(u32, u64)>,
    },
    /// Values a single row cannot be read from, decoded once.
    Decoded(Vec<u64>),
    /// Runs of equal integers and the row after each.
    Runs {
        /// The row after each run.
        ends: Vec<u32>,
        /// The runs' values.
        values: Box<Self>,
    },
}

impl<'a> IntRows<'a> {
    /// The vector `ints` of `n` values, prepared for one-at-a-time reads.
    fn new(ints: Ints<'a>, n: u32) -> Result<Self> {
        Ok(match ints {
            Ints::Constant(value) => Self::Constant(value),
            Ints::Ffor(ffor) => {
                let mut exceptions = Vec::with_capacity(ffor.exception_count as usize);
                let mut rest = ffor.exceptions;
                let mut next = 0u32;
                for _ in 0..ffor.exception_count {
                    // The parse proved every position below `n`, a u32.
                    let position = next + var_u32(&mut rest)?;
                    exceptions.push((position, var_u64(&mut rest)?));
                    next = position + 1;
                }
                Self::Ffor {
                    base: ffor.base,
                    bit_width: ffor.bit_width,
                    packed: ffor.packed,
                    exceptions,
                }
            }
            // A delta's value is the sum of every difference before it, so no
            // row reads alone: decode the vector once.
            delta @ Ints::Delta(_) => {
                let mut values = Vec::with_capacity(n as usize);
                delta.decode_into(n, &mut values)?;
                Self::Decoded(values)
            }
            Ints::Rle { values, ends, .. } => {
                let runs = u32::try_from(ends.len()).map_err(|_| MALFORMED)?;
                Self::Runs {
                    values: Box::new(Self::new(*values, runs)?),
                    ends,
                }
            }
        })
    }

    /// The value of row `row`.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidHeader`] for a row past the vector or an offset past
    /// the base's range.
    pub fn get(&self, row: u32) -> Result<u64> {
        match self {
            Self::Constant(value) => Ok(*value),
            Self::Ffor {
                base,
                bit_width,
                packed,
                exceptions,
            } => {
                if !exceptions.is_empty()
                    && let Ok(at) = exceptions.binary_search_by_key(&row, |&(position, _)| position)
                    && let Some(&(_, value)) = exceptions.get(at)
                {
                    return Ok(value);
                }
                let offset = unpack_one(packed, *bit_width, row)?;
                let Some(value) = base.checked_add(offset) else {
                    return Err(MALFORMED);
                };
                Ok(value)
            }
            Self::Decoded(values) => {
                let Some(&value) = values.get(row as usize) else {
                    return Err(MALFORMED);
                };
                Ok(value)
            }
            Self::Runs { ends, values } => {
                // At most `ends.len()` runs, which the parse bounded by a u32.
                #[expect(clippy::cast_possible_truncation, reason = "a u32 run count")]
                let run = ends.partition_point(|&end| end <= row) as u32;
                values.get(run)
            }
        }
    }
}

/// The `bit_width`-bit offset of row `row` in `packed`, least significant bit
/// first.
fn unpack_one(packed: &[u8], bit_width: u8, row: u32) -> Result<u64> {
    if bit_width == 0 {
        return Ok(0);
    }
    let width = u64::from(bit_width);
    let bit = u64::from(row) * width;
    let (Ok(first), Ok(last)) = (
        usize::try_from(bit / 8),
        usize::try_from((bit + width).div_ceil(8)),
    ) else {
        return Err(MALFORMED);
    };
    let Some(bytes) = packed.get(first..last) else {
        return Err(MALFORMED);
    };
    // At most nine bytes, which a u128 holds whole.
    let mut window = 0u128;
    for (i, &b) in bytes.iter().enumerate() {
        window |= u128::from(b) << (8 * i);
    }
    let mask = if width == 64 {
        u128::from(u64::MAX)
    } else {
        (1u128 << width) - 1
    };
    // Masked to `width` bits, at most 64.
    #[expect(clippy::cast_possible_truncation, reason = "masked to 64 bits")]
    Ok(((window >> (bit % 8)) & mask) as u64)
}

/// A column page's values read one row at a time: what a lookup of a few rows
/// needs, without decoding the page. Built from the page's parsed
/// [`Values`], borrowing it.
#[derive(Clone, Debug)]
pub enum Rows<'a> {
    /// The column's own layout.
    Plain(&'a [u8]),
    /// The one value of every row.
    Constant(&'a [u8]),
    /// Runs of equal values and the row after each.
    Runs {
        /// The row after each run.
        ends: Vec<u32>,
        /// The runs' values.
        values: Box<Self>,
    },
    /// Distinct values and a code per row.
    Dict {
        /// The dictionary's size.
        size: u32,
        /// Each row's code.
        codes: IntRows<'a>,
        /// The distinct values.
        values: Box<Self>,
    },
    /// A number column's values by their ordinals.
    Ordinals {
        /// The column's number type.
        number: Number,
        /// Each row's ordinal.
        ordinals: IntRows<'a>,
    },
    /// A bytes column whose values all have one length: row `i` is
    /// `payload[i * len..]`, found without reading any other row.
    Stride {
        /// Every value's length.
        len: usize,
        /// The values' bytes.
        payload: &'a [u8],
    },
    /// A bytes column's values, each row's start in `payload` found once.
    Lengths {
        /// Where each row starts, and where the last ends.
        offsets: Vec<u32>,
        /// The values' bytes.
        payload: &'a [u8],
    },
}

impl<'a> Values<'a> {
    /// These values of `n` rows of a column of `type_tag`, prepared for
    /// one-row reads.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidHeader`] when the encoding does not describe `n` rows.
    pub(crate) fn rows(self, type_tag: TypeTag, n: u32) -> Result<Rows<'a>> {
        Ok(match self {
            Self::Plain(data) => Rows::Plain(data),
            Self::Constant(value) => Rows::Constant(value),
            Self::Rle { values, ends, .. } => {
                let runs = u32::try_from(ends.len()).map_err(|_| MALFORMED)?;
                Rows::Runs {
                    values: Box::new(values.rows(type_tag, runs)?),
                    ends,
                }
            }
            Self::Dict {
                values,
                size,
                codes,
            } => Rows::Dict {
                size,
                codes: IntRows::new(codes, n)?,
                values: Box::new(values.rows(type_tag, size)?),
            },
            Self::Ordinals(ints) => {
                let TypeTag::Number(number) = type_tag else {
                    return Err(MALFORMED);
                };
                Rows::Ordinals {
                    number,
                    ordinals: IntRows::new(ints, n)?,
                }
            }
            // One length for every row: a row's bytes are found by their
            // position, so a lookup reads no other row's length.
            Self::Lengths {
                lengths: Ints::Constant(len),
                payload,
                ..
            } => {
                let Some(len) = usize::try_from(len)
                    .ok()
                    .filter(|&len| len.checked_mul(n as usize) == Some(payload.len()))
                else {
                    return Err(MALFORMED);
                };
                Rows::Stride { len, payload }
            }
            Self::Lengths {
                lengths, payload, ..
            } => {
                let mut decoded = Vec::with_capacity(n as usize);
                lengths.decode_into(n, &mut decoded)?;
                let mut offsets = Vec::with_capacity(decoded.len() + 1);
                let mut at = 0u32;
                offsets.push(at);
                for len in decoded {
                    at = u32::try_from(len)
                        .ok()
                        .and_then(|len| at.checked_add(len))
                        .ok_or(MALFORMED)?;
                    offsets.push(at);
                }
                if at as usize != payload.len() {
                    return Err(MALFORMED);
                }
                Rows::Lengths { offsets, payload }
            }
        })
    }
}

impl<'a> Rows<'a> {
    /// Row `row`'s value, of a column of `type_tag` with `n` rows.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidHeader`] for a row past the column, a code past its
    /// dictionary or an ordinal no value of the type has.
    pub fn get(&self, type_tag: TypeTag, n: u32, row: u32) -> Result<Cell<'a>> {
        // Checked here for every operator: a bit-packed vector's padding or a
        // constant would otherwise answer for a row the column does not have.
        if row >= n {
            return Err(MALFORMED);
        }
        match self {
            Self::Plain(data) => cell(type_tag, data, n, row).map(Cell::Borrowed),
            Self::Constant(value) => Ok(Cell::Borrowed(value)),
            Self::Runs { ends, values } => {
                let run = ends.partition_point(|&end| end <= row);
                let runs = u32::try_from(ends.len()).map_err(|_| MALFORMED)?;
                values.get(type_tag, runs, u32::try_from(run).map_err(|_| MALFORMED)?)
            }
            Self::Dict {
                size,
                codes,
                values,
            } => {
                let code = codes.get(row)?;
                if code >= u64::from(*size) {
                    return Err(MALFORMED);
                }
                // Below `size`, a u32.
                #[expect(clippy::cast_possible_truncation, reason = "below a u32 size")]
                values.get(type_tag, *size, code as u32)
            }
            Self::Ordinals { number, ordinals } => {
                let mut bytes = [0u8; 16];
                let len = number.width();
                let ordinal = ordinals.get(row)?;
                let Some(out) = bytes.get_mut(..usize::from(len)) else {
                    return Err(MALFORMED);
                };
                if !number.write_ordinal(u128::from(ordinal), out) {
                    return Err(MALFORMED);
                }
                Ok(Cell::Inline { bytes, len })
            }
            Self::Stride { len, payload } => {
                // The parse proved `len * n` is the payload's length, so a row
                // below `n` starts inside it without overflow.
                let start = row as usize * len;
                let Some(value) = payload.get(start..start + len) else {
                    return Err(MALFORMED);
                };
                Ok(Cell::Borrowed(value))
            }
            Self::Lengths { offsets, payload } => {
                let (Some(&start), Some(&end)) =
                    (offsets.get(row as usize), offsets.get(row as usize + 1))
                else {
                    return Err(MALFORMED);
                };
                let Some(value) = payload.get(start as usize..end as usize) else {
                    return Err(MALFORMED);
                };
                Ok(Cell::Borrowed(value))
            }
        }
    }
}

impl Values<'_> {
    /// The column's layout for the rows `keep` selects, `count` of them, read
    /// straight from the encoding: a filtered read builds its survivors once
    /// instead of decoding the page and then gathering from it.
    ///
    /// # Errors
    ///
    /// As [`Self::materialize`], for the rows it reads and within `limit`.
    pub(crate) fn materialize_rows(
        self,
        type_tag: TypeTag,
        n: u32,
        keep: &Selection,
        limit: u64,
    ) -> Result<Slice> {
        if let Self::Plain(data) = self {
            return gather_plain(type_tag, data, n, keep);
        }
        fixed_within(type_tag, keep.count(), limit)?;
        let count = keep.count() as usize;
        let access = self.rows(type_tag, n)?;
        let cells = keep
            .rows()
            .map(|row| access.get(type_tag, n, row))
            .collect::<Result<Vec<_>>>()?;
        match type_tag.fixed_width() {
            Some(width) => Ok(super::gather_fixed_column(
                usize::from(width),
                count,
                cells.iter().map(|c| Some(&**c)),
            )),
            None => frame_bytes_column_within(count, limit, || cells.iter().map(|c| &**c)),
        }
    }
}

/// A view of `page` over `data`, which the parse took from it; a copy if it
/// did not, which no parse of `page` produces.
fn view_of(page: &Slice, data: &[u8]) -> Slice {
    let outer = page.as_ptr_range();
    let inner = data.as_ptr_range();
    if outer.start <= inner.start && inner.end <= outer.end {
        let start = inner.start as usize - outer.start as usize;
        page.slice(start..start + data.len())
    } else {
        Slice::from(data)
    }
}

/// The width a number column's ordinals are encoded at, when its values are
/// encoded by them at all: numbers of at most 8 bytes.
pub fn ordinal_width(type_tag: TypeTag) -> Option<u8> {
    match type_tag {
        TypeTag::Number(number) if number.width() <= 8 => Some(number.width()),
        _ => None,
    }
}

/// Refuses building `rows` rows of a fixed column of `type_tag` past `limit`
/// bytes, before anything is allocated; a bytes column is sized as it is
/// framed instead.
fn fixed_within(type_tag: TypeTag, rows: u32, limit: u64) -> Result<()> {
    let Some(width) = type_tag.fixed_width() else {
        return Ok(());
    };
    // A u32 times a u8, well within a u64.
    let len = u64::from(rows) * u64::from(width);
    if len > limit {
        return Err(Error::DecompressedSizeTooLarge {
            declared: len,
            limit,
        });
    }
    Ok(())
}

/// `value` for each of `n` rows, in the column's layout; a bytes column is
/// refused past `limit` bytes.
fn repeat(type_tag: TypeTag, n: u32, value: &[u8], limit: u64) -> Result<Slice> {
    match type_tag.fixed_width() {
        Some(_) => Ok(super::gather_fixed_column(
            value.len(),
            n as usize,
            (0..n).map(|_| Some(value)),
        )),
        None => frame_bytes_column_within(n as usize, limit, || (0..n).map(|_| value)),
    }
}

/// Row `row` of a column's layout `data` of `rows` rows.
fn cell(type_tag: TypeTag, data: &[u8], rows: u32, row: u32) -> Result<&[u8]> {
    match type_tag.fixed_width() {
        Some(width) => {
            let width = usize::from(width);
            let Some(value) = (row as usize)
                .checked_mul(width)
                .and_then(|start| data.get(start..start + width))
            else {
                return Err(MALFORMED);
            };
            Ok(value)
        }
        None => bytes_column_row(data, rows, row),
    }
}

/// Each row's value in a bytes column's layout `data` of `n` rows, in order:
/// the offset table walked once. The parse checked the framing, so a cell it
/// cannot slice ends the walk rather than being served.
fn plain_bytes_cells(data: &[u8], n: u32) -> impl Iterator<Item = &[u8]> {
    let table = (n as usize + 1) * 4;
    let (offsets, payload) = data.split_at_checked(table).unwrap_or_default();
    let mut start = 0usize;
    offsets.chunks_exact(4).skip(1).map_while(move |end| {
        let end = u32::from_le_bytes(end.try_into().ok()?) as usize;
        let value = payload.get(start..end)?;
        start = end;
        Some(value)
    })
}

/// The rows of a plain layout that `bounds` keeps, tested in one pass over
/// the layout with the bounds' kind resolved once rather than per row.
fn select_plain(type_tag: TypeTag, data: &[u8], n: u32, bounds: &Bounds<'_>) -> Selection {
    let mut out = Selection::none(n);
    match (type_tag.fixed_width().map(usize::from), *bounds) {
        (Some(width @ 1..), Bounds::Ordinals { number, lo, hi }) => {
            for (row, value) in (0u32..).zip(data.chunks_exact(width)) {
                let ordinal = number.ordinal(value);
                if lo <= ordinal && ordinal <= hi {
                    out.insert(row);
                }
            }
        }
        (Some(width @ 1..), _) => {
            for (row, value) in (0u32..).zip(data.chunks_exact(width)) {
                if bounds.keeps(value) {
                    out.insert(row);
                }
            }
        }
        // A zero-width column has no bytes to walk: every row is the empty
        // value.
        (Some(_), _) => {
            if bounds.keeps(&[]) {
                out.insert_range(0..n);
            }
        }
        (None, _) => {
            for (row, value) in (0u32..).zip(plain_bytes_cells(data, n)) {
                if bounds.keeps(value) {
                    out.insert(row);
                }
            }
        }
    }
    out
}

/// The rows `keep` selects of a plain layout `data` of `n` rows, as a layout
/// of their own, copied in one pass. The parse checked the layout, so every
/// selected row is in it.
fn gather_plain(type_tag: TypeTag, data: &[u8], n: u32, keep: &Selection) -> Result<Slice> {
    let count = keep.count() as usize;
    if let Some(width) = type_tag.fixed_width() {
        let width = usize::from(width);
        return Ok(super::gather_fixed_column(
            width,
            count,
            keep.rows()
                .map(|row| data.get(row as usize * width..row as usize * width + width)),
        ));
    }
    let table = (n as usize + 1) * 4;
    let (offsets, payload) = data.split_at_checked(table).unwrap_or_default();
    let at = |i: u32| {
        offsets
            .get(i as usize * 4..i as usize * 4 + 4)
            .and_then(|b| b.try_into().ok())
            .map(|b| u32::from_le_bytes(b) as usize)
    };
    frame_bytes_column(count, || {
        keep.rows().map(move |row| {
            at(row)
                .zip(at(row + 1))
                .and_then(|(start, end)| payload.get(start..end))
                .unwrap_or_default()
        })
    })
}

/// The rows of `source`, a layout of `source_rows` rows, that `rows` names,
/// in that order, as a layout of their own; a bytes column is refused past
/// `limit` bytes, since `rows` may name one row many times.
fn gather(
    type_tag: TypeTag,
    source: &[u8],
    source_rows: u32,
    rows: &[u32],
    limit: u64,
) -> Result<Slice> {
    // Checked once, so the gather below can take each cell as found.
    for &row in rows {
        cell(type_tag, source, source_rows, row)?;
    }
    let take = |row: u32| cell(type_tag, source, source_rows, row).unwrap_or_default();
    if let Some(width) = type_tag.fixed_width() {
        Ok(super::gather_fixed_column(
            usize::from(width),
            rows.len(),
            rows.iter().map(|&row| Some(take(row))),
        ))
    } else {
        frame_bytes_column_within(rows.len(), limit, || rows.iter().map(|&row| take(row)))
    }
}

/// The values whose ordinals are `ordinals`, in `number`'s layout.
fn from_ordinals(number: Number, ordinals: &[u64]) -> Result<Slice> {
    let width = usize::from(number.width());
    let mut out = alloc::vec![0u8; ordinals.len() * width];
    for (dst, &ordinal) in out.chunks_exact_mut(width.max(1)).zip(ordinals) {
        if !number.write_ordinal(u128::from(ordinal), dst) {
            return Err(MALFORMED);
        }
    }
    Ok(Slice::from(out))
}

/// Zigzag: a signed difference as an unsigned integer small when the
/// difference is small either way.
const fn zigzag(delta: u64) -> u64 {
    // The difference's two's complement read as signed.
    let signed = delta.cast_signed();
    ((signed << 1) ^ (signed >> 63)).cast_unsigned()
}

/// Inverse of [`zigzag`].
const fn unzigzag(value: u64) -> u64 {
    (value >> 1) ^ (value & 1).wrapping_neg()
}

// --- Writing -----------------------------------------------------------------

/// What choosing an encoding costs, in thousandths of a stored byte.
///
/// A stored byte is read, checksummed, possibly decompressed and decrypted,
/// and held in the cache; a decoded one is written once into the buffer a
/// read hands out. The per-value figures are what each operator does per row
/// on top of that. A [`Plain`](Expression::Plain) page is handed out as a view
/// of itself and costs its bytes alone.
mod cost {
    /// A byte stored in the page.
    pub(super) const STORED_BYTE: u64 = 1000;
    /// A byte written into the column a read hands out.
    pub(super) const BUILT_BYTE: u64 = 60;
    /// A row unpacked from FFOR.
    pub(super) const UNPACK: u64 = 300;
    /// An exception put back in its row.
    pub(super) const PATCH: u64 = 2000;
    /// A row's running sum through a delta.
    pub(super) const DELTA: u64 = 150;
    /// A row found in its run.
    pub(super) const EXPAND: u64 = 100;
    /// A row looked up in its dictionary.
    pub(super) const GATHER: u64 = 250;
    /// A number rebuilt from its ordinal.
    pub(super) const ORDINAL: u64 = 150;
    /// A bytes cell's offset rebuilt from its length.
    pub(super) const LENGTH: u64 = 100;
}

/// One encoding the writer considered for a vector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Candidate {
    /// What it would have been encoded as.
    pub expression: Expression,
    /// Its encoded bytes.
    pub bytes: usize,
    /// Of those, the bytes of its operators' own fields rather than of the
    /// values: tags, counts, bases and widths, the per-unit overhead that
    /// makes an encoding lose on a small page.
    pub overhead: usize,
    /// What storing and reading it costs, in thousandths of a stored byte.
    pub cost: u64,
}

/// An encoded vector: its bytes and what it costs.
struct Trial {
    expression: Expression,
    bytes: Vec<u8>,
    /// Bytes of values rather than operator fields.
    payload: usize,
    /// Decoding work, in thousandths of a stored byte.
    work: u64,
}

impl Trial {
    fn cost(&self) -> u64 {
        // A page is at most a few MiB and every per-row figure a few thousand,
        // so the sum stays many orders of magnitude below `u64::MAX`.
        self.bytes.len() as u64 * cost::STORED_BYTE + self.work
    }

    fn candidate(&self) -> Candidate {
        Candidate {
            expression: self.expression.clone(),
            bytes: self.bytes.len(),
            overhead: self.bytes.len() - self.payload,
            cost: self.cost(),
        }
    }
}

/// The trial that costs least; ties go to the one considered first, which
/// the callers order simplest first.
fn cheapest(trials: Vec<Trial>) -> Option<Trial> {
    let mut best: Option<Trial> = None;
    for trial in trials {
        if best.as_ref().is_none_or(|b| trial.cost() < b.cost()) {
            best = Some(trial);
        }
    }
    best
}

/// The bits `value` needs.
#[expect(clippy::cast_possible_truncation, reason = "at most 64")]
const fn bits(value: u64) -> u8 {
    (u64::BITS - value.leading_zeros()) as u8
}

/// The bytes `value` takes as a varint.
const fn var_len(value: u64) -> usize {
    // Zero still takes its one byte.
    if value == 0 {
        1
    } else {
        (bits(value) as usize).div_ceil(7)
    }
}

/// `values` as FFOR at the bit width that costs least to store and to patch,
/// the values past that width patched in as exceptions. Only this width
/// becomes a trial, so it is chosen by the same cost the trials are.
fn ffor(values: &[u64]) -> Trial {
    let n = u32::try_from(values.len()).unwrap_or(u32::MAX);
    let base = values.iter().copied().min().unwrap_or(0);
    // Per width, the rows whose offsets need exactly that many bits and the
    // varint bytes their values take whole, which is what an exception costs.
    let mut count = [0u64; 65];
    let mut whole = [0u64; 65];
    for &value in values {
        let width = usize::from(bits(value - base));
        if let (Some(c), Some(w)) = (count.get_mut(width), whole.get_mut(width)) {
            *c += 1;
            *w += var_len(value) as u64;
        }
    }
    // The cost at width `w`: the packed offsets, plus every row needing more
    // than `w` bits as a gap (a byte, near enough) and its value, stored, and
    // each of those rows patched back. Unpacking costs the same per row at
    // every width, so it does not decide.
    let mut best_width = 64u8;
    let mut best_cost = u64::MAX;
    let (mut above_count, mut above_whole) = (0u64, 0u64);
    for width in (0..=64u8).rev() {
        let size = (u64::from(n) * u64::from(width)).div_ceil(8) + above_count + above_whole;
        // A page's rows and bytes are far below where this could overflow.
        let cost = size * cost::STORED_BYTE + above_count * cost::PATCH;
        if cost <= best_cost {
            best_cost = cost;
            best_width = width;
        }
        above_count += count.get(usize::from(width)).copied().unwrap_or(0);
        above_whole += whole.get(usize::from(width)).copied().unwrap_or(0);
    }
    ffor_at(values, base, best_width)
}

/// `values` as FFOR from `base` in `bit_width` bits.
fn ffor_at(values: &[u64], base: u64, bit_width: u8) -> Trial {
    let n = u32::try_from(values.len()).unwrap_or(u32::MAX);
    let mut bytes = vec![FFOR];
    put_varint(&mut bytes, base);
    bytes.push(bit_width);
    let fits = |offset: u64| bit_width == 64 || offset >> bit_width == 0;
    let packed_start = bytes.len();
    let mut window = 0u128;
    let mut held = 0u32;
    let mut exceptions = Vec::new();
    for (row, &value) in values.iter().enumerate() {
        let offset = value - base;
        let stored = if fits(offset) {
            offset
        } else {
            exceptions.push((row, value));
            0
        };
        if bit_width > 0 {
            window |= u128::from(stored) << held;
            held += u32::from(bit_width);
            while held >= 8 {
                // The low byte of the window.
                #[expect(clippy::cast_possible_truncation, reason = "one byte")]
                bytes.push(window as u8);
                window >>= 8;
                held -= 8;
            }
        }
    }
    if held > 0 {
        #[expect(clippy::cast_possible_truncation, reason = "one byte")]
        bytes.push(window as u8);
    }
    let packed = bytes.len() - packed_start;
    let count = u32::try_from(exceptions.len()).unwrap_or(u32::MAX);
    put_varint(&mut bytes, u64::from(count));
    let exceptions_start = bytes.len();
    let mut next = 0usize;
    for &(row, value) in &exceptions {
        put_varint(&mut bytes, (row - next) as u64);
        put_varint(&mut bytes, value);
        next = row + 1;
    }
    let payload = packed + (bytes.len() - exceptions_start);
    Trial {
        expression: Expression::Ffor {
            bit_width,
            exceptions: count,
        },
        bytes,
        payload,
        work: u64::from(n) * cost::UNPACK + u64::from(count) * cost::PATCH,
    }
}

/// `values` as one constant.
fn int_constant(value: u64, n: usize) -> Trial {
    let mut bytes = vec![CONSTANT];
    put_varint(&mut bytes, value);
    Trial {
        expression: Expression::Constant,
        bytes,
        payload: 0,
        // Filled once per row, but only into the integers a caller asked
        // for; free next to any decode.
        work: n as u64,
    }
}

/// The runs of equal values in `values`: each run's value and the row after
/// it.
fn int_runs(values: &[u64]) -> (Vec<u64>, Vec<u64>) {
    let mut heads = Vec::new();
    let mut ends = Vec::new();
    for (row, &value) in values.iter().enumerate() {
        if heads.last() != Some(&value) || row == 0 {
            if row > 0 {
                ends.push(row as u64);
            }
            heads.push(value);
        }
    }
    if !values.is_empty() {
        ends.push(values.len() as u64);
    }
    (heads, ends)
}

/// The encodings of an integer vector the writer considers, simplest first.
///
/// `nested` excludes run-length encoding, which a run's own values and ends
/// do not repeat enough to need.
fn int_trials(values: &[u64], nested: bool) -> Vec<Trial> {
    let n = values.len();
    let mut trials = Vec::new();
    let first = values.first().copied().unwrap_or(0);
    if values.iter().all(|&v| v == first) {
        trials.push(int_constant(first, n));
        return trials;
    }
    trials.push(ffor(values));
    let deltas: Vec<u64> = {
        let mut prev = 0u64;
        values
            .iter()
            .map(|&v| {
                let d = zigzag(v.wrapping_sub(prev));
                prev = v;
                d
            })
            .collect()
    };
    let inner = ffor(&deltas);
    let mut bytes = vec![DELTA];
    bytes.extend_from_slice(&inner.bytes);
    trials.push(Trial {
        expression: Expression::Delta(Box::new(inner.expression)),
        payload: inner.payload,
        work: inner.work + n as u64 * cost::DELTA,
        bytes,
    });
    if !nested {
        let (heads, ends) = int_runs(values);
        if heads.len() * 2 <= n
            && let (Some(values_trial), Some(ends_trial)) = (
                cheapest(int_trials(&heads, true)),
                cheapest(int_trials(&ends, true)),
            )
        {
            let mut bytes = vec![RLE];
            put_varint(&mut bytes, heads.len() as u64);
            bytes.extend_from_slice(&values_trial.bytes);
            bytes.extend_from_slice(&ends_trial.bytes);
            trials.push(Trial {
                expression: Expression::Rle {
                    values: Box::new(values_trial.expression),
                    ends: Box::new(ends_trial.expression),
                },
                payload: values_trial.payload + ends_trial.payload,
                work: values_trial.work + ends_trial.work + n as u64 * cost::EXPAND,
                bytes,
            });
        }
    }
    trials
}

/// The cheapest encoding of an integer vector.
fn best_ints(values: &[u64], nested: bool) -> Trial {
    cheapest(int_trials(values, nested)).unwrap_or_else(|| int_constant(0, values.len()))
}

/// A column page's rows, as the cells the encoders compare and copy.
struct Cells<'a> {
    type_tag: TypeTag,
    rows: u32,
    data: &'a [u8],
}

impl<'a> Cells<'a> {
    fn get(&self, row: u32) -> &'a [u8] {
        // The column was validated before it is encoded, so every row reads.
        cell(self.type_tag, self.data, self.rows, row).unwrap_or_default()
    }

    fn iter(&self) -> impl Iterator<Item = &'a [u8]> + '_ {
        (0..self.rows).map(|row| self.get(row))
    }

    /// The bytes a read builds for these rows, which every encoding but a
    /// plain one pays.
    fn built(&self) -> u64 {
        self.data.len() as u64 * cost::BUILT_BYTE
    }
}

/// The comparable form of a cell: a number's ordinal, a bytes or opaque
/// cell's bytes. A dictionary is sorted by it, so a range over the values is
/// a range over the codes.
fn order_key(type_tag: TypeTag, cell: &[u8]) -> (u128, &[u8]) {
    match type_tag {
        TypeTag::Number(number) => (number.ordinal(cell), &[]),
        TypeTag::Bytes | TypeTag::Fixed(_) => (0, cell),
    }
}

/// `cells` as the column's own layout, which a read serves as a view.
fn values_plain(cells: &Cells<'_>) -> Trial {
    let mut bytes = vec![PLAIN];
    bytes.extend_from_slice(cells.data);
    Trial {
        expression: Expression::Plain,
        payload: cells.data.len(),
        bytes,
        work: 0,
    }
}

/// `cells` of a number column by their ordinals.
fn values_ordinals(cells: &Cells<'_>, number: Number) -> Trial {
    let ordinals: Vec<u64> = cells
        .iter()
        // A number of at most 8 bytes has an ordinal that fits a u64.
        .map(|c| u64::try_from(number.ordinal(c)).unwrap_or(u64::MAX))
        .collect();
    let inner = best_ints(&ordinals, false);
    let mut bytes = vec![ORDINALS];
    bytes.extend_from_slice(&inner.bytes);
    Trial {
        expression: Expression::Ordinals(Box::new(inner.expression)),
        payload: inner.payload,
        work: inner.work + u64::from(cells.rows) * cost::ORDINAL + cells.built(),
        bytes,
    }
}

/// `cells` of a bytes column as their lengths and their bytes.
fn values_lengths(cells: &Cells<'_>) -> Trial {
    let lengths: Vec<u64> = cells.iter().map(|c| c.len() as u64).collect();
    let inner = best_ints(&lengths, false);
    let payload_len: usize = cells.iter().map(<[u8]>::len).sum();
    let mut bytes = vec![LENGTHS];
    bytes.extend_from_slice(&inner.bytes);
    put_varint(&mut bytes, payload_len as u64);
    for c in cells.iter() {
        bytes.extend_from_slice(c);
    }
    Trial {
        expression: Expression::Lengths(Box::new(inner.expression)),
        payload: inner.payload + payload_len,
        work: inner.work + u64::from(cells.rows) * cost::LENGTH + cells.built(),
        bytes,
    }
}

/// The cheapest encoding of a run's or a dictionary's values: plain, or the
/// type's integer form.
fn inner_values(cells: &Cells<'_>) -> Trial {
    let mut trials = vec![values_plain(cells)];
    match cells.type_tag {
        TypeTag::Number(number) if ordinal_width(cells.type_tag).is_some() => {
            trials.push(values_ordinals(cells, number));
        }
        TypeTag::Bytes => trials.push(values_lengths(cells)),
        TypeTag::Number(_) | TypeTag::Fixed(_) => {}
    }
    cheapest(trials).unwrap_or_else(|| values_plain(cells))
}

/// Builds the layout of `rows` cells, for a run's or a dictionary's values.
fn layout(type_tag: TypeTag, rows: &[&[u8]]) -> Result<Slice> {
    match type_tag.fixed_width() {
        Some(width) => Ok(super::gather_fixed_column(
            usize::from(width),
            rows.len(),
            rows.iter().map(|&c| Some(c)),
        )),
        None => frame_bytes_column(rows.len(), || rows.iter().copied()),
    }
}

/// Every encoding the writer considers for a column page's values, simplest
/// first. `Ok` holds at least the plain one.
fn values_trials(cells: &Cells<'_>) -> Result<Vec<Trial>> {
    let n = cells.rows as usize;
    let mut trials = vec![values_plain(cells)];
    let Some(first) = cells.iter().next() else {
        return Ok(trials);
    };
    if cells.iter().all(|c| c == first) {
        let mut bytes = vec![CONSTANT];
        if cells.type_tag.fixed_width().is_none() {
            put_varint(&mut bytes, first.len() as u64);
        }
        bytes.extend_from_slice(first);
        trials.push(Trial {
            expression: Expression::Constant,
            payload: first.len(),
            bytes,
            work: cells.built(),
        });
        return Ok(trials);
    }
    match cells.type_tag {
        TypeTag::Number(number) if ordinal_width(cells.type_tag).is_some() => {
            trials.push(values_ordinals(cells, number));
        }
        TypeTag::Bytes => trials.push(values_lengths(cells)),
        TypeTag::Number(_) | TypeTag::Fixed(_) => {}
    }
    // Runs: worth considering when they are at most half the rows.
    let mut heads: Vec<&[u8]> = Vec::new();
    let mut ends: Vec<u64> = Vec::new();
    for (row, c) in cells.iter().enumerate() {
        if heads.last() != Some(&c) || row == 0 {
            if row > 0 {
                ends.push(row as u64);
            }
            heads.push(c);
        }
    }
    ends.push(n as u64);
    if heads.len() * 2 <= n {
        let head_data = layout(cells.type_tag, &heads)?;
        let head_cells = Cells {
            type_tag: cells.type_tag,
            rows: u32::try_from(heads.len()).map_err(|_| MALFORMED)?,
            data: &head_data,
        };
        let values = inner_values(&head_cells);
        let ends_trial = best_ints(&ends, true);
        let mut bytes = vec![RLE];
        put_varint(&mut bytes, heads.len() as u64);
        bytes.extend_from_slice(&values.bytes);
        bytes.extend_from_slice(&ends_trial.bytes);
        trials.push(Trial {
            expression: Expression::Rle {
                values: Box::new(values.expression),
                ends: Box::new(ends_trial.expression),
            },
            payload: values.payload + ends_trial.payload,
            work: values.work + ends_trial.work + n as u64 * cost::EXPAND + cells.built(),
            bytes,
        });
    }
    // A dictionary: worth considering when the distinct values are at most
    // half the rows.
    let mut distinct: crate::HashMap<&[u8], u32> = crate::HashMap::default();
    for c in cells.iter() {
        let next = u32::try_from(distinct.len()).unwrap_or(u32::MAX);
        distinct.entry(c).or_insert(next);
        if distinct.len() * 2 > n {
            break;
        }
    }
    if distinct.len() * 2 <= n {
        let mut sorted: Vec<&[u8]> = distinct.keys().copied().collect();
        sorted.sort_unstable_by(|a, b| {
            order_key(cells.type_tag, a).cmp(&order_key(cells.type_tag, b))
        });
        for (rank, value) in sorted.iter().enumerate() {
            if let Some(code) = distinct.get_mut(value) {
                *code = u32::try_from(rank).unwrap_or(u32::MAX);
            }
        }
        let codes: Vec<u64> = cells
            .iter()
            .map(|c| u64::from(distinct.get(c).copied().unwrap_or(0)))
            .collect();
        let dictionary = layout(cells.type_tag, &sorted)?;
        let dictionary_cells = Cells {
            type_tag: cells.type_tag,
            rows: u32::try_from(sorted.len()).map_err(|_| MALFORMED)?,
            data: &dictionary,
        };
        let values = inner_values(&dictionary_cells);
        let codes_trial = best_ints(&codes, false);
        let mut bytes = vec![DICT];
        put_varint(&mut bytes, sorted.len() as u64);
        bytes.extend_from_slice(&values.bytes);
        bytes.extend_from_slice(&codes_trial.bytes);
        trials.push(Trial {
            expression: Expression::Dict {
                values: Box::new(values.expression),
                codes: Box::new(codes_trial.expression),
            },
            payload: values.payload + codes_trial.payload,
            work: values.work + codes_trial.work + n as u64 * cost::GATHER + cells.built(),
            bytes,
        });
    }
    Ok(trials)
}

/// A column page's values encoded as the cheapest expression the writer
/// considers.
pub struct Choice {
    /// The encoded expression, ready for the page.
    pub bytes: Vec<u8>,
    /// What it is.
    pub expression: Expression,
}

/// Encodes `rows` rows of a column of `type_tag` whose layout is `data` as
/// `encoding` says: the layout itself, or the expression that costs least to
/// store and to read.
///
/// The choice is per page: every page of a column is encoded on its own.
///
/// # Errors
///
/// [`Error::InvalidHeader`] when a bytes column's values do not fit its
/// offsets, which a validated column cannot reach.
pub fn choose(
    type_tag: TypeTag,
    rows: u32,
    data: &[u8],
    encoding: ColumnEncoding,
) -> Result<Choice> {
    let cells = Cells {
        type_tag,
        rows,
        data,
    };
    if encoding == ColumnEncoding::Plain {
        let plain = values_plain(&cells);
        return Ok(Choice {
            bytes: plain.bytes,
            expression: plain.expression,
        });
    }
    let trials = values_trials(&cells)?;
    let Some(best) = cheapest(trials) else {
        return Err(MALFORMED);
    };
    Ok(Choice {
        bytes: best.bytes,
        expression: best.expression,
    })
}

/// Every encoding the writer considers for a page's values, simplest first.
///
/// The page holds `rows` rows of a column of `type_tag` whose layout is
/// `data`. Each candidate carries what it would store and cost: the decision
/// a level under [`ColumnEncoding::Auto`] makes, laid out so a surprising
/// choice can be explained. The cheapest is the one written.
///
/// # Errors
///
/// [`Error::InvalidHeader`] when the layout is not `rows` rows of the type.
pub fn candidates(type_tag: TypeTag, rows: u32, data: &[u8]) -> Result<Vec<Candidate>> {
    super::check_layout(type_tag, rows, data)?;
    Ok(values_trials(&Cells {
        type_tag,
        rows,
        data,
    })?
    .iter()
    .map(Trial::candidate)
    .collect())
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test code: a failed expectation is the assertion"
)]
mod tests;
