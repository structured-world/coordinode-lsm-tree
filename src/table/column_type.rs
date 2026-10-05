// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The physical type of a column's values, shared by the columnar format and
//! by rows written as cells, which carry each cell's type whether or not the
//! columnar format is built.

use crate::{Error, Result};
use alloc::vec::Vec;

/// Physical layout category of a column's values.
///
/// Drives codec selection, decode framing and, for a [`TypeTag::Number`], the
/// order the engine filters and prunes by; it carries no logical (schema)
/// meaning.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TypeTag {
    /// Fixed-width opaque values: every row occupies exactly `N` bytes
    /// (`N > 0`). The engine defines no order on them, so such a column has no
    /// statistics, is never pruned on and a predicate over it does not run.
    Fixed(u8),
    /// Fixed-width numbers: every row occupies [`Number::width`] bytes, ordered
    /// as the [`Number`] describes. Statistics, pruning and row filtering run
    /// on the column's comparable encoding ([`Number::comparable`]).
    Number(Number),
    /// Variable-width opaque byte arrays. The column data is a
    /// `(row_count + 1)`-entry little-endian `u32` offset array followed by the
    /// concatenated value bytes; row `i` spans `offset[i]..offset[i + 1]`.
    Bytes,
}

impl TypeTag {
    /// The byte width of every row, or `None` for the variable-width
    /// [`TypeTag::Bytes`].
    #[must_use]
    pub const fn fixed_width(self) -> Option<u8> {
        match self {
            Self::Fixed(width) => Some(width),
            Self::Number(number) => Some(number.width),
            Self::Bytes => None,
        }
    }

    /// Wire form: a `(tag, width)` pair. `width` is the fixed byte width, or `0`
    /// for the variable-width [`TypeTag::Bytes`]. A [`TypeTag::Number`] takes
    /// tags `2..=7`: kind in steps of two, byte order in the low bit.
    pub(crate) const fn to_wire(self) -> (u8, u8) {
        match self {
            Self::Fixed(width) => (0, width),
            Self::Bytes => (1, 0),
            Self::Number(number) => {
                let kind = match number.kind {
                    NumberKind::Unsigned => 0,
                    NumberKind::Signed => 1,
                    NumberKind::Float => 2,
                };
                let order = match number.order {
                    ByteOrder::Little => 0,
                    ByteOrder::Big => 1,
                };
                (2 + kind * 2 + order, number.width)
            }
        }
    }

    pub(crate) fn from_wire(tag: u8, width: u8) -> Result<Self> {
        match tag {
            0 => {
                if width == 0 {
                    return Err(Error::InvalidHeader("columnar: fixed column width is zero"));
                }
                Ok(Self::Fixed(width))
            }
            1 => {
                if width != 0 {
                    return Err(Error::InvalidHeader(
                        "columnar: bytes column width must be zero",
                    ));
                }
                Ok(Self::Bytes)
            }
            2..=7 => {
                let kind = match (tag - 2) / 2 {
                    0 => NumberKind::Unsigned,
                    1 => NumberKind::Signed,
                    _ => NumberKind::Float,
                };
                let order = if tag.is_multiple_of(2) {
                    ByteOrder::Little
                } else {
                    ByteOrder::Big
                };
                Number::new(kind, width, order).map(Self::Number)
            }
            _ => Err(Error::InvalidTag(("ColumnTypeTag", tag))),
        }
    }
}

/// What a fixed-width number is, as far as ordering it goes.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum NumberKind {
    /// An unsigned integer.
    Unsigned,
    /// A two's-complement signed integer.
    Signed,
    /// An IEEE 754 binary floating-point number, ordered by the standard's
    /// `totalOrder` predicate (IEEE 754-2019 5.10): `-NaN < -inf < ... < -0 <
    /// +0 < ... < +inf < +NaN`, so `-0` sorts below `+0` and every NaN has a
    /// place by its sign and payload.
    Float,
}

/// The order of a fixed-width number's bytes in the column.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum ByteOrder {
    /// Least significant byte first.
    Little,
    /// Most significant byte first.
    Big,
}

/// The physical description of a fixed-width number column: its kind, byte
/// width and byte order. Enough to order the values and nothing more; the
/// engine still attaches no logical meaning to the column.
///
/// Nulls live in the column's validity bitmap: a null row has no value, so it
/// is excluded from the statistics and matches no predicate.
///
/// # Examples
///
/// ```
/// use lsm_tree::table::column_type::{ByteOrder, Number, NumberKind};
///
/// let n = Number::new(NumberKind::Signed, 4, ByteOrder::Little).unwrap();
/// // -1 sorts below 1 in the comparable encoding.
/// let minus_one = n.comparable(&(-1i32).to_le_bytes()).unwrap();
/// let one = n.comparable(&1i32.to_le_bytes()).unwrap();
/// assert!(minus_one < one);
/// ```
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Number {
    pub(crate) kind: NumberKind,
    pub(crate) width: u8,
    pub(crate) order: ByteOrder,
}

impl Number {
    /// The engine's own seqno column: an unsigned little-endian `u64`.
    pub const U64_LE: Self = Self {
        kind: NumberKind::Unsigned,
        width: 8,
        order: ByteOrder::Little,
    };

    /// A number of `kind`, `width` bytes wide, stored in `order`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidHeader`] for a width the kind has no encoding
    /// at: an integer is 1, 2, 4, 8 or 16 bytes, a float 4 or 8.
    pub fn new(kind: NumberKind, width: u8, order: ByteOrder) -> Result<Self> {
        let valid = match kind {
            NumberKind::Unsigned | NumberKind::Signed => matches!(width, 1 | 2 | 4 | 8 | 16),
            NumberKind::Float => matches!(width, 4 | 8),
        };
        if !valid {
            return Err(Error::InvalidHeader(
                "columnar: no number of this kind has this width",
            ));
        }
        Ok(Self { kind, width, order })
    }

    /// The kind of number.
    #[must_use]
    pub const fn kind(self) -> NumberKind {
        self.kind
    }

    /// The byte width of every value.
    #[must_use]
    pub const fn width(self) -> u8 {
        self.width
    }

    /// The byte order of every value.
    #[must_use]
    pub const fn order(self) -> ByteOrder {
        self.order
    }

    /// The comparable encoding of one value stored as `native`: [`Self::width`]
    /// bytes whose byte-wise order is the numbers' order. Predicate bounds over
    /// a number column are given in this encoding.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidHeader`] when `native` is not
    /// [`Self::width`] bytes long.
    pub fn comparable(self, native: &[u8]) -> Result<Vec<u8>> {
        if native.len() != usize::from(self.width) {
            return Err(Error::InvalidHeader(
                "columnar: a number value is not its column's width",
            ));
        }
        Ok(comparable_bytes(self, self.ordinal(native)).to_vec())
    }

    /// The largest ordinal a value of this width has: `2^(8 * width) - 1`.
    pub(crate) const fn max_ordinal(self) -> u128 {
        // `width` is 1..=16, so the shift is 0..=120.
        u128::MAX >> (128 - 8 * self.width as u32)
    }

    /// The comparable encoding of `native` read as an unsigned integer: the
    /// number's position in its order. `native` is [`Self::width`] bytes; a
    /// shorter slice reads as if zero-extended, which a caller that checked the
    /// column's framing never passes.
    #[inline]
    pub(crate) fn ordinal(self, native: &[u8]) -> u128 {
        let mut raw = 0u128;
        match self.order {
            ByteOrder::Big => {
                for &b in native {
                    raw = (raw << 8) | u128::from(b);
                }
            }
            ByteOrder::Little => {
                for &b in native.iter().rev() {
                    raw = (raw << 8) | u128::from(b);
                }
            }
        }
        let sign = 1u128 << (8 * u32::from(self.width) - 1);
        match self.kind {
            NumberKind::Unsigned => raw,
            // Flipping the sign bit moves the negatives below the positives
            // and keeps each half in order.
            NumberKind::Signed => raw ^ sign,
            // IEEE 754-2019 5.10 totalOrder: a negative's magnitude grows as
            // its bits do, so all of it is inverted; a positive only moves
            // above the negatives.
            NumberKind::Float => {
                if raw & sign == 0 {
                    raw | sign
                } else {
                    !raw & self.max_ordinal()
                }
            }
        }
    }

    /// Writes the value whose [`Self::ordinal`] is `ordinal` into `out`, which
    /// is [`Self::width`] bytes: the inverse of [`Self::ordinal`].
    ///
    /// Returns `false`, leaving `out` unspecified, when `ordinal` is past
    /// [`Self::max_ordinal`] or `out` is not the width: no value has it.
    #[cfg(feature = "columnar")]
    #[inline]
    pub(crate) fn write_ordinal(self, ordinal: u128, out: &mut [u8]) -> bool {
        if ordinal > self.max_ordinal() || out.len() != usize::from(self.width) {
            return false;
        }
        let sign = 1u128 << (8 * u32::from(self.width) - 1);
        let raw = match self.kind {
            NumberKind::Unsigned => ordinal,
            NumberKind::Signed => ordinal ^ sign,
            // The inverse of the two totalOrder cases: a positive's ordinal
            // has the sign bit set, a negative's has it clear.
            NumberKind::Float => {
                if ordinal & sign == 0 {
                    !ordinal & self.max_ordinal()
                } else {
                    ordinal ^ sign
                }
            }
        };
        let bytes = raw.to_le_bytes();
        let low = bytes.get(..out.len()).unwrap_or_default();
        match self.order {
            ByteOrder::Little => out.copy_from_slice(low),
            ByteOrder::Big => {
                for (dst, &src) in out.iter_mut().zip(low.iter().rev()) {
                    *dst = src;
                }
            }
        }
        true
    }
}

/// A number's comparable encoding held inline: at most 16 bytes, so building
/// one for a statistic or a bound never allocates.
pub(crate) struct Comparable {
    bytes: [u8; 16],
    width: usize,
}

impl core::ops::Deref for Comparable {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // `width` is a `Number`'s, at most 16.
        self.bytes.get(16 - self.width..).unwrap_or_default()
    }
}

/// `ordinal` as `number`'s comparable encoding: its low [`Number::width`]
/// bytes, most significant first.
pub(crate) fn comparable_bytes(number: Number, ordinal: u128) -> Comparable {
    Comparable {
        bytes: ordinal.to_be_bytes(),
        width: usize::from(number.width),
    }
}
