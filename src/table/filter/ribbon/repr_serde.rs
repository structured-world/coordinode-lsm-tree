//! `Serialize` / `Deserialize` for the ribbon repr, written out so the crate
//! needs no proc-macro: the shape is the one `#[derive]` gives. A struct is
//! its named fields (a sequence of them in declaration order is accepted too,
//! unknown fields are skipped), and [`Mode`] is its variant name or index.

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::fmt;

use serde::de::{
    self, DeserializeSeed, Deserializer, EnumAccess, Expected, MapAccess, SeqAccess, Unexpected,
    VariantAccess, Visitor,
};
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};

use super::filter::RibbonFilterRepr;
use super::params::{Mode, Params};

#[cfg(test)]
mod tests;

/// Resolves a struct field or enum variant identifier, by name or by index,
/// to its name in `names`. An unknown field resolves to `None` (skipped); an
/// unknown variant is an error.
struct Identifier {
    names: &'static [&'static str],
    variant: bool,
}

/// What a variant index was expected to be.
struct VariantRange(usize);

impl Expected for VariantRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "variant index 0 <= i < {}", self.0)
    }
}

/// What a struct given as a sequence was expected to hold.
struct StructLen(&'static str, usize);

impl Expected for StructLen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "struct {} with {} elements", self.0, self.1)
    }
}

impl Identifier {
    fn by_name<E: de::Error>(&self, name: &str) -> Result<Option<&'static str>, E> {
        match self.names.iter().find(|known| **known == name) {
            Some(known) => Ok(Some(known)),
            None if self.variant => Err(E::unknown_variant(name, self.names)),
            None => Ok(None),
        }
    }
}

impl<'de> DeserializeSeed<'de> for Identifier {
    type Value = Option<&'static str>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_identifier(self)
    }
}

impl<'de> Visitor<'de> for Identifier {
    type Value = Option<&'static str>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.variant {
            "variant identifier"
        } else {
            "field identifier"
        })
    }

    fn visit_u64<E: de::Error>(self, index: u64) -> Result<Self::Value, E> {
        match usize::try_from(index).ok().and_then(|i| self.names.get(i)) {
            Some(name) => Ok(Some(name)),
            None if self.variant => Err(E::invalid_value(
                Unexpected::Unsigned(index),
                &VariantRange(self.names.len()),
            )),
            None => Ok(None),
        }
    }

    fn visit_str<E: de::Error>(self, name: &str) -> Result<Self::Value, E> {
        self.by_name(name)
    }

    fn visit_bytes<E: de::Error>(self, name: &[u8]) -> Result<Self::Value, E> {
        self.by_name(&alloc::string::String::from_utf8_lossy(name))
    }
}

/// `Serialize` and `Deserialize` for a struct of named fields, as the derive
/// writes them. Each field is listed once with its type; a field left out of
/// the list fails to compile at the struct literal.
macro_rules! serde_struct {
    ($ty:ident { $($field:ident: $fty:ty),+ $(,)? }) => {
        impl Serialize for $ty {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                const FIELDS: &[&str] = &[$(stringify!($field)),+];
                let mut state = serializer.serialize_struct(stringify!($ty), FIELDS.len())?;
                $(state.serialize_field(stringify!($field), &self.$field)?;)+
                state.end()
            }
        }

        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                const FIELDS: &[&str] = &[$(stringify!($field)),+];

                struct StructVisitor;

                impl<'de> Visitor<'de> for StructVisitor {
                    type Value = $ty;

                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str(concat!("struct ", stringify!($ty)))
                    }

                    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<$ty, A::Error> {
                        $(
                            let Some($field) = seq.next_element::<$fty>()? else {
                                // The fields before this one were read.
                                let read = FIELDS
                                    .iter()
                                    .position(|name| *name == stringify!($field))
                                    .unwrap_or_default();
                                return Err(de::Error::invalid_length(
                                    read,
                                    &StructLen(stringify!($ty), FIELDS.len()),
                                ));
                            };
                        )+
                        Ok($ty { $($field),+ })
                    }

                    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<$ty, A::Error> {
                        $(let mut $field: Option<$fty> = None;)+
                        while let Some(key) =
                            map.next_key_seed(Identifier { names: FIELDS, variant: false })?
                        {
                            $(
                                if key == Some(stringify!($field)) {
                                    if $field.is_some() {
                                        return Err(de::Error::duplicate_field(stringify!($field)));
                                    }
                                    $field = Some(map.next_value()?);
                                    continue;
                                }
                            )+
                            map.next_value::<de::IgnoredAny>()?;
                        }
                        $(
                            let $field =
                                $field.ok_or_else(|| de::Error::missing_field(stringify!($field)))?;
                        )+
                        Ok($ty { $($field),+ })
                    }
                }

                deserializer.deserialize_struct(stringify!($ty), FIELDS, StructVisitor)
            }
        }
    };
}

serde_struct!(Params {
    m: usize,
    w: usize,
    r: usize,
    mode: Mode,
    seed: u64,
    retry_limit: usize,
    grow_limit: usize,
});

serde_struct!(RibbonFilterRepr {
    version: u8,
    params: Params,
    z: Vec<u64>,
});

const MODE_VARIANTS: &[&str] = &["Standard", "Homogeneous"];

impl Serialize for Mode {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Standard => serializer.serialize_unit_variant("Mode", 0, "Standard"),
            Self::Homogeneous => serializer.serialize_unit_variant("Mode", 1, "Homogeneous"),
        }
    }
}

impl<'de> Deserialize<'de> for Mode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ModeVisitor;

        impl<'de> Visitor<'de> for ModeVisitor {
            type Value = Mode;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("enum Mode")
            }

            fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<Mode, A::Error> {
                let (name, variant) = data.variant_seed(Identifier {
                    names: MODE_VARIANTS,
                    variant: true,
                })?;
                variant.unit_variant()?;
                // A variant identifier resolves to one of `MODE_VARIANTS` or
                // fails, so anything but `Homogeneous` is `Standard`.
                Ok(if name == Some("Homogeneous") {
                    Mode::Homogeneous
                } else {
                    Mode::Standard
                })
            }
        }

        deserializer.deserialize_enum("Mode", MODE_VARIANTS, ModeVisitor)
    }
}
