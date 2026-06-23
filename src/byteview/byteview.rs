// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use alloc::{string::String, sync::Arc, vec::Vec};
use core::{mem::ManuallyDrop, ops::Deref, sync::atomic::Ordering};

use portable_atomic::AtomicU64;

pub use super::builder::Builder;

#[cfg(target_pointer_width = "64")]
const INLINE_SIZE: usize = 20;

#[cfg(target_pointer_width = "32")]
const INLINE_SIZE: usize = 16;

const PREFIX_SIZE: usize = 4;

#[repr(C)]
struct HeapAllocationHeader {
    ref_count: AtomicU64,
}

#[repr(C)]
struct ShortRepr {
    len: u32,
    data: [u8; INLINE_SIZE],
}

#[repr(C)]
struct LongRepr {
    len: u32,
    prefix: [u8; PREFIX_SIZE],
    heap: *const u8,
    original_len: u32,
    offset: u32,
}

#[repr(C)]
pub union Trailer {
    short: ManuallyDrop<ShortRepr>,
    long: ManuallyDrop<LongRepr>,
}

impl Default for Trailer {
    fn default() -> Self {
        Self {
            short: ManuallyDrop::new(ShortRepr {
                len: 0,
                data: [0; INLINE_SIZE],
            }),
        }
    }
}

/// An immutable byte slice
///
/// Will be inlined (no pointer dereference or heap allocation)
/// if it is 20 characters or shorter (on a 64-bit system).
///
/// A single heap allocation will be shared between multiple slices.
/// Even subslices of that heap allocation can be cloned without additional heap allocation.
///
/// [`ByteView`] does not guarantee any sort of alignment for zero-copy (de)serialization.
///
/// The design is very similar to:
///
/// - [Polars' strings](<https://pola.rs/posts/polars-string-type>)
/// - [CedarDB's German strings](<https://cedardb.com/blog/german_strings>)
/// - [Umbra's string](<https://db.in.tum.de/~freitag/papers/p29-neumann-cidr20.pdf>)
/// - [Velox' String View](https://facebookincubator.github.io/velox/develop/vectors.html)
/// - [Apache Arrow's String View](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow14BinaryViewType6c_typeE)
#[repr(C)]
#[derive(Default)]
pub struct ByteView {
    trailer: Trailer,
}

#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for ByteView {}
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Sync for ByteView {}

impl Clone for ByteView {
    fn clone(&self) -> Self {
        self.slice(..)
    }
}

impl Drop for ByteView {
    fn drop(&mut self) {
        if self.is_inline() {
            return;
        }

        let heap_region = self.get_heap_region();

        if heap_region.ref_count.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }

        unsafe {
            let header_size = core::mem::size_of::<HeapAllocationHeader>();
            let alignment = core::mem::align_of::<HeapAllocationHeader>();
            let total_size = header_size + self.trailer.long.original_len as usize;
            let layout = alloc::alloc::Layout::from_size_align(total_size, alignment).unwrap();

            let ptr = self.trailer.long.heap.cast_mut();
            alloc::alloc::dealloc(ptr, layout);
        }
    }
}

impl Eq for ByteView {}

impl core::cmp::PartialEq for ByteView {
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            let src_ptr = (self as *const Self).cast::<u8>();
            let other_ptr: *const u8 = (other as *const Self).cast::<u8>();

            let a = *src_ptr.cast::<u64>();
            let b = *other_ptr.cast::<u64>();

            if a != b {
                return false;
            }
        }

        // NOTE: At this point we know
        // both strings must have the same prefix and same length
        //
        // If we are inlined, the other string must be inlined too,
        // so checking the short slice is enough
        if self.is_inline() {
            self.get_short_slice() == other.get_short_slice()
        } else {
            self.get_long_slice() == other.get_long_slice()
        }
    }
}

impl core::cmp::Ord for ByteView {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.prefix()
            .cmp(other.prefix())
            .then_with(|| self.deref().cmp(&**other))
    }
}

impl core::cmp::PartialOrd for ByteView {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl core::fmt::Debug for ByteView {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{:?}", &**self)
    }
}

impl Deref for ByteView {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        if self.is_inline() {
            self.get_short_slice()
        } else {
            self.get_long_slice()
        }
    }
}

impl core::hash::Hash for ByteView {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.deref().hash(state);
    }
}

/// RAII guard for [`ByteView::get_mut`], so the prefix gets
/// updated properly when the mutation is done
pub struct Mutator<'a>(pub(crate) &'a mut ByteView);

impl core::ops::Deref for Mutator<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl core::ops::DerefMut for Mutator<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.get_mut_slice()
    }
}

impl Drop for Mutator<'_> {
    fn drop(&mut self) {
        self.0.update_prefix();
    }
}

impl ByteView {
    #[doc(hidden)]
    #[must_use]
    pub unsafe fn builder_unzeroed(len: usize) -> Builder {
        // SAFETY: forwards the caller's "fully initialize before reading" contract
        // for the uninitialized allocation to `with_size_unzeroed`.
        Builder::new(unsafe { Self::with_size_unzeroed(len) })
    }

    #[doc(hidden)]
    #[must_use]
    pub fn builder(len: usize) -> Builder {
        Builder::new(Self::with_size(len))
    }

    fn prefix(&self) -> &[u8] {
        let len = PREFIX_SIZE.min(self.len());

        // SAFETY: Both trailer layouts have the prefix stored at the same position
        unsafe { self.trailer.short.data.get_unchecked(..len) }
    }

    fn is_inline(&self) -> bool {
        self.len() <= INLINE_SIZE
    }

    pub(crate) fn update_prefix(&mut self) {
        if !self.is_inline() {
            unsafe {
                let slice_ptr: &[u8] = &*self;
                let slice_ptr = slice_ptr.as_ptr();

                // Zero out prefix
                (*self.trailer.long).prefix[0] = 0;
                (*self.trailer.long).prefix[1] = 0;
                (*self.trailer.long).prefix[2] = 0;
                (*self.trailer.long).prefix[3] = 0;

                let prefix = (*self.trailer.long).prefix.as_mut_ptr();
                core::ptr::copy_nonoverlapping(slice_ptr, prefix, self.len().min(4));
            }
        }
    }

    /// Returns a mutable reference into the given byteview, if there are no other pointers to the same allocation.
    pub fn get_mut(&mut self) -> Option<Mutator<'_>> {
        if self.ref_count() == 1 {
            Some(Mutator(self))
        } else {
            None
        }
    }

    /// Creates a byteview and populates it with `len` bytes
    /// from the given reader.
    ///
    /// Requires the `std` feature (the `std::io::Read` reader trait has no
    /// `core`/`alloc` equivalent); the consuming read paths are themselves
    /// `std`-bound.
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurred.
    #[cfg(feature = "std")]
    pub fn from_reader<R: std::io::Read>(reader: &mut R, len: usize) -> std::io::Result<Self> {
        // NOTE: We can use _unzeroed to skip zeroing of the heap allocated slice
        // because we receive the `len` parameter
        // If the reader does not give us exactly `len` bytes, `read_exact` fails anyway
        let mut s = unsafe { Self::with_size_unzeroed(len) };
        {
            let mut builder = Mutator(&mut s);
            reader.read_exact(&mut builder)?;
        }
        Ok(s)
    }

    /// `no_std` mirror of [`ByteView::from_reader`] over [`crate::io::Read`].
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurred.
    #[cfg(not(feature = "std"))]
    pub fn from_reader<R: crate::io::Read>(reader: &mut R, len: usize) -> crate::io::Result<Self> {
        let mut s = unsafe { Self::with_size_unzeroed(len) };
        {
            let mut builder = Mutator(&mut s);
            reader.read_exact(&mut builder)?;
        }
        Ok(s)
    }

    /// Fuses two byte slices into a single byteview.
    #[must_use]
    pub fn fused(left: &[u8], right: &[u8]) -> Self {
        let len = left.len() + right.len();
        let mut builder = unsafe { Self::builder_unzeroed(len) };
        builder[0..left.len()].copy_from_slice(left);
        builder[left.len()..].copy_from_slice(right);
        builder.freeze()
    }

    /// Creates a new zeroed, fixed-length byteview.
    ///
    /// Use [`ByteView::get_mut`] to mutate the content.
    ///
    /// # Panics
    ///
    /// Panics if the length does not fit in a u32 (4 GiB).
    #[must_use]
    pub fn with_size(slice_len: usize) -> Self {
        Self::with_size_zeroed(slice_len)
    }

    /// Creates a new zeroed, fixed-length byteview.
    ///
    /// # Panics
    ///
    /// Panics if the length does not fit in a u32 (4 GiB).
    fn with_size_zeroed(slice_len: usize) -> Self {
        let view = if slice_len <= INLINE_SIZE {
            Self {
                trailer: Trailer {
                    short: ManuallyDrop::new(ShortRepr {
                        // SAFETY: We know slice_len is INLINE_SIZE or less, so it must be
                        // a valid u32
                        #[allow(clippy::cast_possible_truncation)]
                        len: slice_len as u32,
                        data: [0; INLINE_SIZE],
                    }),
                },
            }
        } else {
            let Ok(len) = u32::try_from(slice_len) else {
                panic!("byte slice too long");
            };

            unsafe {
                const HEADER_SIZE: usize = core::mem::size_of::<HeapAllocationHeader>();
                const ALIGNMENT: usize = core::mem::align_of::<HeapAllocationHeader>();

                let total_size = HEADER_SIZE + slice_len;
                let layout = alloc::alloc::Layout::from_size_align(total_size, ALIGNMENT).unwrap();

                // IMPORTANT: Zero-allocate the region
                let heap_ptr = alloc::alloc::alloc_zeroed(layout);
                if heap_ptr.is_null() {
                    alloc::alloc::handle_alloc_error(layout);
                }

                // Set ref count
                let heap_region = heap_ptr as *const HeapAllocationHeader;
                let heap_region = &*heap_region;
                heap_region.ref_count.store(1, Ordering::Release);

                Self {
                    trailer: Trailer {
                        long: ManuallyDrop::new(LongRepr {
                            len,
                            prefix: [0; PREFIX_SIZE],
                            heap: heap_ptr,
                            original_len: len,
                            offset: 0,
                        }),
                    },
                }
            }
        };

        debug_assert_eq!(1, view.ref_count());

        view
    }

    /// Creates a new fixed-length byteview, **with uninitialized contents**.
    ///
    /// # Panics
    ///
    /// Panics if the length does not fit in a u32 (4 GiB).
    #[doc(hidden)]
    #[must_use]
    pub unsafe fn with_size_unzeroed(slice_len: usize) -> Self {
        let view = if slice_len <= INLINE_SIZE {
            Self {
                trailer: Trailer {
                    short: ManuallyDrop::new(ShortRepr {
                        // SAFETY: We know slice_len is INLINE_SIZE or less, so it must be
                        // a valid u32
                        #[allow(clippy::cast_possible_truncation)]
                        len: slice_len as u32,
                        data: [0; INLINE_SIZE],
                    }),
                },
            }
        } else {
            let Ok(len) = u32::try_from(slice_len) else {
                panic!("byte slice too long");
            };

            unsafe {
                const HEADER_SIZE: usize = core::mem::size_of::<HeapAllocationHeader>();
                const ALIGNMENT: usize = core::mem::align_of::<HeapAllocationHeader>();

                let total_size = HEADER_SIZE + slice_len;
                let layout = alloc::alloc::Layout::from_size_align(total_size, ALIGNMENT).unwrap();

                let heap_ptr = alloc::alloc::alloc(layout);
                if heap_ptr.is_null() {
                    alloc::alloc::handle_alloc_error(layout);
                }

                // Set ref count
                let heap_region = heap_ptr as *const HeapAllocationHeader;
                let heap_region = &*heap_region;
                heap_region.ref_count.store(1, Ordering::Release);

                Self {
                    trailer: Trailer {
                        long: ManuallyDrop::new(LongRepr {
                            len,
                            prefix: [0; PREFIX_SIZE],
                            heap: heap_ptr,
                            original_len: len,
                            offset: 0,
                        }),
                    },
                }
            }
        };

        debug_assert_eq!(1, view.ref_count());

        view
    }

    /// Creates a new byteview from an existing byte slice.
    ///
    /// Will heap-allocate the slice if it has at least length 21.
    ///
    /// # Panics
    ///
    /// Panics if the length does not fit in a u32 (4 GiB).
    #[must_use]
    pub fn new(slice: &[u8]) -> Self {
        let slice_len = slice.len();

        let mut view = unsafe { Self::with_size_unzeroed(slice_len) };

        if view.is_inline() {
            // SAFETY: We check for inlinability
            // so we know the the input slice fits our buffer
            unsafe {
                let data_ptr = core::ptr::addr_of_mut!((*view.trailer.short).data).cast();
                core::ptr::copy_nonoverlapping(slice.as_ptr(), data_ptr, slice_len);
            }
        } else {
            let long_repr = unsafe { &mut *view.trailer.long };

            // Copy prefix
            // SAFETY: We know that there are at least 4 bytes in the input slice
            #[allow(clippy::indexing_slicing)]
            long_repr.prefix.copy_from_slice(&slice[0..PREFIX_SIZE]);

            // Copy byte slice into heap allocation
            view.get_mut_slice().copy_from_slice(slice);
        }

        debug_assert_eq!(1, view.ref_count());

        view
    }

    unsafe fn data_ptr(&self) -> *const u8 {
        const HEADER_SIZE: usize = core::mem::size_of::<HeapAllocationHeader>();

        debug_assert!(!self.is_inline());

        // SAFETY: caller guarantees the heap (long) union variant is active, so
        // reading `trailer.long` and offsetting past the header is in-bounds.
        unsafe {
            self.trailer
                .long
                .heap
                .add(HEADER_SIZE)
                .add(self.trailer.long.offset as usize)
        }
    }

    unsafe fn data_ptr_mut(&mut self) -> *mut u8 {
        const HEADER_SIZE: usize = core::mem::size_of::<HeapAllocationHeader>();

        debug_assert!(!self.is_inline());

        // SAFETY: caller guarantees the heap (long) union variant is active, so
        // reading `trailer.long` and offsetting past the header is in-bounds.
        unsafe {
            self.trailer
                .long
                .heap
                .add(HEADER_SIZE)
                .add(self.trailer.long.offset as usize)
                .cast_mut()
        }
    }

    fn get_heap_region(&self) -> &HeapAllocationHeader {
        debug_assert!(
            !self.is_inline(),
            "inline slice does not have a heap allocation"
        );

        unsafe {
            let ptr = self.trailer.long.heap;
            let heap_region: *const HeapAllocationHeader = ptr.cast::<HeapAllocationHeader>();
            &*heap_region
        }
    }

    /// Returns the ref_count of the underlying heap allocation.
    #[doc(hidden)]
    #[must_use]
    pub fn ref_count(&self) -> u64 {
        if self.is_inline() {
            1
        } else {
            self.get_heap_region().ref_count.load(Ordering::Acquire)
        }
    }

    /// Clones the contents of this slice into an independently tracked slice.
    #[must_use]
    pub fn to_detached(&self) -> Self {
        Self::new(self)
    }

    /// Clones the given range of the existing byteview without heap allocation.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// # use byteview::ByteView;
    /// let slice = ByteView::from("helloworld_thisisalongstring");
    /// let copy = slice.slice(11..);
    /// assert_eq!(b"thisisalongstring", &*copy);
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the slice is out of bounds.
    #[must_use]
    pub fn slice(&self, range: impl core::ops::RangeBounds<usize>) -> Self {
        use core::ops::Bound;

        // Credits: This is essentially taken from
        // https://github.com/tokio-rs/bytes/blob/291df5acc94b82a48765e67eeb1c1a2074539e68/src/bytes.rs#L264

        let self_len = self.len();

        let begin = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n.checked_add(1).expect("out of range"),
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            Bound::Included(&n) => n.checked_add(1).expect("out of range"),
            Bound::Excluded(&n) => n,
            Bound::Unbounded => self_len,
        };

        assert!(
            begin <= end,
            "range start must not be greater than end: {begin:?} <= {end:?}",
        );
        assert!(
            end <= self_len,
            "range end out of bounds: {end:?} <= {self_len:?}",
        );

        let new_len = end - begin;
        let len = u32::try_from(new_len).unwrap();

        // Target and destination slices are inlined
        // so we just need to memcpy the struct, and replace
        // the inline slice with the requested range
        if new_len <= INLINE_SIZE {
            let mut child = Self {
                trailer: Trailer {
                    short: ManuallyDrop::new(ShortRepr {
                        len,
                        data: [0; INLINE_SIZE],
                    }),
                },
            };

            let slice = &self[begin..end];
            debug_assert_eq!(slice.len(), new_len);

            let data_ptr = unsafe { &mut (*child.trailer.short).data };

            unsafe {
                core::ptr::copy_nonoverlapping(slice.as_ptr(), data_ptr.as_mut_ptr(), new_len);
            }

            child
        } else {
            // IMPORTANT: Increase ref count
            let heap_region = self.get_heap_region();
            heap_region.ref_count.fetch_add(1, Ordering::Release);

            let mut child = Self {
                // SAFETY: self.data must be defined
                // we cannot get a range larger than our own slice
                // so we cannot be inlined while the requested slice is not inlinable
                trailer: Trailer {
                    long: ManuallyDrop::new(LongRepr {
                        len,
                        prefix: [0; PREFIX_SIZE],
                        heap: unsafe { self.trailer.long.heap },
                        offset: unsafe { self.trailer.long.offset } + begin as u32,
                        original_len: unsafe { self.trailer.long.original_len },
                    }),
                },
            };

            let prefix = &self[begin..(begin + 4)];
            debug_assert_eq!(prefix.len(), 4);

            unsafe {
                (*child.trailer.long).prefix.copy_from_slice(prefix);
            }

            child
        }
    }

    /// Returns `true` if `needle` is a prefix of the slice or equal to the slice.
    pub fn starts_with<T: AsRef<[u8]>>(&self, needle: T) -> bool {
        let needle = needle.as_ref();

        unsafe {
            let len = PREFIX_SIZE.min(needle.len());
            let needle_prefix: &[u8] = needle.get_unchecked(..len);

            if !self.prefix().starts_with(needle_prefix) {
                return false;
            }
        }

        self.deref().starts_with(needle)
    }

    /// Returns `true` if the slice is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the amount of bytes in the slice.
    #[must_use]
    pub fn len(&self) -> usize {
        unsafe { self.trailer.short.len as usize }
    }

    pub(crate) fn get_mut_slice(&mut self) -> &mut [u8] {
        let len = self.len();

        if self.is_inline() {
            unsafe { core::slice::from_raw_parts_mut((*self.trailer.short).data.as_mut_ptr(), len) }
        } else {
            unsafe { core::slice::from_raw_parts_mut(self.data_ptr_mut(), len) }
        }
    }

    fn get_short_slice(&self) -> &[u8] {
        let len = self.len();

        debug_assert!(
            len <= INLINE_SIZE,
            "cannot get short slice - slice is not inlined",
        );

        // SAFETY: Shall only be called if slice is inlined
        unsafe { core::slice::from_raw_parts((*self.trailer.short).data.as_ptr(), len) }
    }

    fn get_long_slice(&self) -> &[u8] {
        let len = self.len();

        debug_assert!(
            len > INLINE_SIZE,
            "cannot get long slice - slice is inlined"
        );

        // SAFETY: Shall only be called if slice is heap allocated
        unsafe { core::slice::from_raw_parts(self.data_ptr(), len) }
    }
}

impl core::borrow::Borrow<[u8]> for ByteView {
    fn borrow(&self) -> &[u8] {
        self
    }
}

impl AsRef<[u8]> for ByteView {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl FromIterator<u8> for ByteView {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = u8>,
    {
        Self::from(iter.into_iter().collect::<Vec<u8>>())
    }
}

impl From<&[u8]> for ByteView {
    fn from(value: &[u8]) -> Self {
        Self::new(value)
    }
}

impl From<Arc<[u8]>> for ByteView {
    fn from(value: Arc<[u8]>) -> Self {
        Self::new(&value)
    }
}

impl From<Vec<u8>> for ByteView {
    fn from(value: Vec<u8>) -> Self {
        Self::new(&value)
    }
}

impl From<&str> for ByteView {
    fn from(value: &str) -> Self {
        Self::from(value.as_bytes())
    }
}

impl From<String> for ByteView {
    fn from(value: String) -> Self {
        Self::from(value.as_bytes())
    }
}

impl From<Arc<str>> for ByteView {
    fn from(value: Arc<str>) -> Self {
        Self::from(&*value)
    }
}

impl<const N: usize> From<[u8; N]> for ByteView {
    fn from(value: [u8; N]) -> Self {
        Self::from(value.as_slice())
    }
}

#[cfg(feature = "serde")]
mod serde {
    use super::ByteView;
    use core::fmt;
    use serde::de::{self, Visitor};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    impl Serialize for ByteView {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_bytes(self)
        }
    }

    impl<'de> Deserialize<'de> for ByteView {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct ByteViewVisitor;

            impl<'de> Visitor<'de> for ByteViewVisitor {
                type Value = ByteView;

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_str("a byte array")
                }

                fn visit_bytes<E>(self, v: &[u8]) -> Result<ByteView, E>
                where
                    E: de::Error,
                {
                    Ok(ByteView::new(v))
                }

                fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
                where
                    A: de::SeqAccess<'de>,
                {
                    let bytes: Vec<u8> =
                        Deserialize::deserialize(de::value::SeqAccessDeserializer::new(seq))?;

                    Ok(ByteView::new(&bytes))
                }
            }

            deserializer.deserialize_bytes(ByteViewVisitor)
        }
    }
}

#[cfg(test)]
mod tests;
