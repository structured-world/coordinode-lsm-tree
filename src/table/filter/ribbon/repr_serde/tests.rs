use super::{Identifier, MODE_VARIANTS};
use crate::table::filter::ribbon::params::{Mode, Params};
use serde::Deserialize;
use serde::de::{
    DeserializeSeed,
    value::{BoolDeserializer, BytesDeserializer, Error, U64Deserializer},
};

const FIELDS: &[&str] = &["first", "second"];

fn field() -> Identifier {
    Identifier {
        names: FIELDS,
        variant: false,
    }
}

fn variant() -> Identifier {
    Identifier {
        names: MODE_VARIANTS,
        variant: true,
    }
}

#[test]
fn identifier_by_index_resolves_known_and_skips_or_refuses_unknown() {
    let at = |seed: Identifier, index| seed.deserialize(U64Deserializer::<Error>::new(index));

    assert_eq!(at(field(), 1).ok(), Some(Some("second")));
    assert_eq!(at(field(), 9).ok(), Some(None));
    assert_eq!(at(variant(), 1).ok(), Some(Some("Homogeneous")));
    assert_eq!(
        at(variant(), 9).map_err(|e| e.to_string()),
        Err("invalid value: integer `9`, expected variant index 0 <= i < 2".to_owned())
    );
}

#[test]
fn identifier_by_bytes_resolves_like_by_name() {
    let at = |seed: Identifier, name: &'static [u8]| {
        seed.deserialize(BytesDeserializer::<Error>::new(name))
    };

    assert_eq!(at(field(), b"first").ok(), Some(Some("first")));
    assert_eq!(at(field(), b"third").ok(), Some(None));
    assert_eq!(at(variant(), b"Standard").ok(), Some(Some("Standard")));
    assert_eq!(
        at(variant(), b"Sideways").map_err(|e| e.to_string()),
        Err("unknown variant `Sideways`, expected `Standard` or `Homogeneous`".to_owned())
    );
}

#[test]
fn wrong_input_type_names_what_was_expected() {
    let identifier = |seed: Identifier| {
        seed.deserialize(BoolDeserializer::<Error>::new(true))
            .map_err(|e| e.to_string())
    };
    assert_eq!(
        identifier(field()),
        Err("invalid type: boolean `true`, expected field identifier".to_owned())
    );
    assert_eq!(
        identifier(variant()),
        Err("invalid type: boolean `true`, expected variant identifier".to_owned())
    );

    assert_eq!(
        Params::deserialize(BoolDeserializer::<Error>::new(true)).map_err(|e| e.to_string()),
        Err("invalid type: boolean `true`, expected struct Params".to_owned())
    );
    assert_eq!(
        Mode::deserialize(BoolDeserializer::<Error>::new(true)).map_err(|e| e.to_string()),
        Err("invalid type: boolean `true`, expected enum Mode".to_owned())
    );
}

#[test]
fn mode_round_trips_both_variants() {
    for mode in [Mode::Standard, Mode::Homogeneous] {
        let json = serde_json::to_string(&mode).expect("serialize");
        assert_eq!(serde_json::from_str::<Mode>(&json).ok(), Some(mode));
    }
}

#[test]
fn params_rejects_a_duplicate_field() {
    let err = serde_json::from_str::<Params>(
        r#"{"m":3,"m":3,"w":16,"r":8,"mode":"Standard","seed":0,"retry_limit":8,"grow_limit":2}"#,
    )
    .err()
    .map(|e| e.to_string());
    assert!(
        err.as_deref()
            .is_some_and(|e| e.starts_with("duplicate field `m`")),
        "{err:?}"
    );
}
