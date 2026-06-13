use serde::de::{Deserializer, MapAccess, Visitor};
use serde::ser::{SerializeMap, Serializer};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::borrow::Cow;

/// A top-level JSON object whose values are kept as raw, unparsed slices
/// borrowed from the request buffer.
///
/// serde_json still parses and validates the entire input — there is no
/// hand-rolled scanning — but only top-level keys are materialized; every
/// value is forwarded byte-for-byte, preserving key order, number
/// precision, and formatting inside values. Duplicate top-level keys
/// collapse last-wins, matching a `serde_json::Map` round-trip.
pub struct RawBody<'a> {
    fields: Vec<(String, Cow<'a, RawValue>)>,
}

impl<'a> RawBody<'a> {
    pub fn get(&self, key: &str) -> Option<&RawValue> {
        self.fields
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| &**v)
    }

    /// Parse one field into a concrete type (None if absent or mismatched).
    pub fn get_as<T: serde::de::DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.get(key)
            .and_then(|raw| serde_json::from_str(raw.get()).ok())
    }

    /// Insert or replace a top-level field, keeping its original position.
    pub fn set(&mut self, key: &str, value: Box<RawValue>) {
        match self.fields.iter_mut().find(|(k, _)| k == key) {
            Some((_, slot)) => *slot = Cow::Owned(value),
            None => self.fields.push((key.to_string(), Cow::Owned(value))),
        }
    }
}

impl<'de> Deserialize<'de> for RawBody<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RawBodyVisitor;

        impl<'de> Visitor<'de> for RawBodyVisitor {
            type Value = RawBody<'de>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a JSON object")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut access: M) -> Result<Self::Value, M::Error> {
                let mut body = RawBody {
                    fields: Vec::with_capacity(access.size_hint().unwrap_or(0)),
                };
                while let Some((key, value)) = access.next_entry::<String, &RawValue>()? {
                    match body.fields.iter_mut().find(|(k, _)| *k == key) {
                        Some((_, slot)) => *slot = Cow::Borrowed(value),
                        None => body.fields.push((key, Cow::Borrowed(value))),
                    }
                }
                Ok(body)
            }
        }

        deserializer.deserialize_map(RawBodyVisitor)
    }
}

impl Serialize for RawBody<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.fields.len()))?;
        for (key, value) in &self.fields {
            map.serialize_entry(key, &**value)?;
        }
        map.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(input: &str) -> String {
        let body: RawBody = serde_json::from_str(input).unwrap();
        serde_json::to_string(&body).unwrap()
    }

    #[test]
    fn values_pass_through_byte_for_byte() {
        // Key order, number precision, and nested formatting all survive.
        let input = r#"{"z":1,"model":"fast","big":184467440737095516150.123456789012345678,"nested":{ "b":2, "a":1 }}"#;
        assert_eq!(
            round_trip(input),
            r#"{"z":1,"model":"fast","big":184467440737095516150.123456789012345678,"nested":{ "b":2, "a":1 }}"#
        );
    }

    #[test]
    fn duplicate_keys_collapse_last_wins() {
        assert_eq!(round_trip(r#"{"a":1,"a":2}"#), r#"{"a":2}"#);
    }

    #[test]
    fn non_object_is_rejected() {
        assert!(serde_json::from_str::<RawBody>("[1,2]").is_err());
        assert!(serde_json::from_str::<RawBody>("\"hi\"").is_err());
    }

    #[test]
    fn invalid_json_is_rejected() {
        assert!(serde_json::from_str::<RawBody>(r#"{"a":"#).is_err());
        // The whole input is validated, not just the top-level keys.
        assert!(serde_json::from_str::<RawBody>(r#"{"a":{"b":}}"#).is_err());
    }

    #[test]
    fn get_as_parses_fields() {
        let body: RawBody =
            serde_json::from_str(r#"{"model":"fa\"st","stream":true,"n":3}"#).unwrap();
        assert_eq!(body.get_as::<String>("model").unwrap(), "fa\"st");
        assert_eq!(body.get_as::<bool>("stream"), Some(true));
        assert_eq!(body.get_as::<bool>("n"), None);
        assert_eq!(body.get_as::<bool>("absent"), None);
    }

    #[test]
    fn set_replaces_in_place_and_appends() {
        let mut body: RawBody = serde_json::from_str(r#"{"a":1,"model":"x","b":2}"#).unwrap();
        body.set("model", serde_json::value::to_raw_value("y").unwrap());
        body.set("c", serde_json::value::to_raw_value(&3).unwrap());
        assert_eq!(
            serde_json::to_string(&body).unwrap(),
            r#"{"a":1,"model":"y","b":2,"c":3}"#
        );
    }
}
