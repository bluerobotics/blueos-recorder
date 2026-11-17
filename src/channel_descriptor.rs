use std::{borrow::Cow, collections::BTreeMap, fmt, path::PathBuf};

use anyhow::Result;
use serde_json::{Value, json};
use tracing::*;
use zenoh::bytes::{Encoding, ZBytes};

pub struct ChannelDescriptor {
    pub topic: String,
    pub schema: Option<SchemaDescriptor>,
    pub message_encoding: MessageEncoding,
    /// The topic carries JSON values that are not objects (arrays, numbers, strings...).
    /// Foxglove only reads objects, so each message is written as `{"value": <payload>}`
    /// (see [`wrap_json_payload`]) and the schema describes that wrapper.
    pub wrap_json_value: bool,
}

pub struct SchemaDescriptor {
    pub encoding: SchemaEncoding,
    pub content: Option<SchemaDescriptorContent>,
}

pub struct SchemaDescriptorContent {
    pub name: String,
    pub data: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaEncoding {
    Ros2Msg,
    JsonSchema,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageEncoding {
    Cdr,
    Json,
    OctetStream,
}

impl ChannelDescriptor {
    #[instrument(skip_all)]
    pub fn new(
        topic: &str,
        encoding: &Encoding,
        payload: &ZBytes,
        schema_path: Option<&PathBuf>,
    ) -> Option<Self> {
        let encoding = Cow::from(encoding);
        let (encoding, schema) = {
            let mut encoding_split = encoding.split(';');
            let Some(encoding) = encoding_split.next() else {
                warn!("No encoding string");
                return None;
            };
            let schema = encoding_split.next();

            (encoding, schema)
        };

        let cdr = Cow::from(Encoding::APPLICATION_CDR);
        let json = Cow::from(Encoding::APPLICATION_JSON);
        let octet = Cow::from(Encoding::APPLICATION_OCTET_STREAM);

        // For more information: https://mcap.dev/spec/registry#well-known-schema-encodings
        match (encoding, schema) {
            (encoding, Some(schema_name)) if encoding == cdr => {
                let schema_data = match load_cdr_schema(schema_name, schema_path) {
                    Ok(schema_data) => schema_data,
                    Err(error) => {
                        error!(%error, "Failed to load schema");
                        return None;
                    }
                };
                Some(Self {
                    topic: topic.to_owned(),
                    schema: Some(SchemaDescriptor {
                        encoding: SchemaEncoding::Ros2Msg,
                        content: Some(SchemaDescriptorContent {
                            name: schema_name.to_owned(),
                            data: schema_data,
                        }),
                    }),
                    message_encoding: MessageEncoding::Cdr,
                    wrap_json_value: false,
                })
            }
            (encoding, schema) if encoding == json => {
                let Ok(string) = payload.try_to_string() else {
                    warn!("Failed to decode payload as UTF-8 string");
                    return None;
                };
                let Ok(mut value) = serde_json5::from_str::<Value>(&string) else {
                    warn!(payload = %string, "Failed to parse payload as JSON5");
                    return None;
                };
                // Foxglove does not support non-object messages, so they are recorded
                // wrapped in an object instead of being dropped
                let wrap_json_value = !value.is_object();
                if wrap_json_value {
                    info!("Recording non-object JSON payloads as {{\"value\": <payload>}}");
                    value = json!({ "value": value });
                }
                let schema_name = match schema {
                    Some(name) => name.to_owned(),
                    None => topic.replace('/', "."),
                };
                let schema_data = create_schema(&value).to_string();
                Some(Self {
                    topic: topic.to_owned(),
                    schema: Some(SchemaDescriptor {
                        encoding: SchemaEncoding::JsonSchema,
                        content: Some(SchemaDescriptorContent {
                            name: schema_name,
                            data: schema_data,
                        }),
                    }),
                    message_encoding: MessageEncoding::Json,
                    wrap_json_value,
                })
            }
            (encoding, _schema) if encoding == octet => Some(Self {
                topic: topic.to_owned(),
                schema: None,
                message_encoding: MessageEncoding::OctetStream,
                wrap_json_value: false,
            }),
            _ => {
                warn!(encoding = %encoding, "Received unknown encoding");
                None
            }
        }
    }
}

/// Turns a JSON (or JSON5) payload into the `{"value": <payload>}` object written for
/// channels with [`ChannelDescriptor::wrap_json_value`] set.
pub fn wrap_json_payload(payload: &[u8]) -> Option<Vec<u8>> {
    let string = std::str::from_utf8(payload).ok()?;
    let value = serde_json5::from_str::<Value>(string).ok()?;
    serde_json::to_vec(&json!({ "value": value })).ok()
}

impl SchemaEncoding {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ros2Msg => "ros2msg",
            Self::JsonSchema => "jsonschema",
        }
    }
}

impl MessageEncoding {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cdr => "cdr",
            Self::Json => "json",
            Self::OctetStream => "application/octet-stream",
        }
    }
}

impl fmt::Display for SchemaEncoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for MessageEncoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

const MSGS_DIR: include_dir::Dir = include_dir::include_dir!("src/external/zBlueberry/msgs");

#[instrument(skip_all)]
fn load_cdr_schema(schema: &str, schema_path: Option<&PathBuf>) -> Result<String> {
    let mut schema_splitted = schema.split(".");
    let schema_package = schema_splitted.next().ok_or(anyhow::anyhow!(
        "Failed to get schema package from {schema}"
    ))?;
    let schema_name = schema_splitted
        .next()
        .ok_or(anyhow::anyhow!("Failed to get schema name from {schema}"))?;

    if let Some(schema_path) = schema_path {
        let schema_path = schema_path.join(format!("{schema_package}/{schema_name}.msg"));
        std::fs::read_to_string(&schema_path)
            .map_err(|error| anyhow::anyhow!("Failed to read schema: {error}, ({schema_path:?})"))
    } else {
        let schema_path = format!("{schema_package}/{schema_name}.msg");
        let schema = MSGS_DIR.get_file(&schema_path).ok_or(anyhow::anyhow!(
            "Failed to get schema file from {schema_path}"
        ))?;
        let schema = schema.contents_utf8().ok_or(anyhow::anyhow!(
            "Failed to get schema contents from {schema_path}"
        ))?;
        Ok(schema.to_string())
    }
}

fn create_schema(value: &Value) -> Value {
    match value {
        Value::Null => json!({ "type": "null" }),
        Value::Bool(_) => json!({ "type": "boolean" }),
        Value::Number(n) if n.is_i64() => json!({ "type": "integer" }),
        Value::Number(_) => json!({ "type": "number" }),
        Value::String(_) => json!({ "type": "string" }),
        Value::Array(arr) => {
            let items = if let Some(first) = arr.first() {
                create_schema(first)
            } else {
                json!({})
            };
            json!({ "type": "array", "items": items })
        }
        Value::Object(map) => {
            let properties: BTreeMap<_, _> = map
                .iter()
                .map(|(k, v)| (k.clone(), create_schema(v)))
                .collect();
            json!({ "type": "object", "properties": properties })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json_descriptor(payload: &str) -> ChannelDescriptor {
        ChannelDescriptor::new(
            "services/system_information/cpu",
            &Encoding::APPLICATION_JSON,
            &ZBytes::from(payload),
            None,
        )
        .expect("JSON payloads should get a channel")
    }

    fn schema(descriptor: &ChannelDescriptor) -> Value {
        let content = descriptor
            .schema
            .as_ref()
            .unwrap()
            .content
            .as_ref()
            .unwrap();
        serde_json::from_str(&content.data).unwrap()
    }

    #[test]
    fn array_payload_gets_a_wrapped_channel() {
        let descriptor = json_descriptor(r#"[{"name": "cpu0", "usage": 12.5}]"#);
        assert!(descriptor.wrap_json_value);
        assert_eq!(descriptor.message_encoding, MessageEncoding::Json);
        let schema = schema(&descriptor);
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["properties"]["value"]["type"], "array");
        assert_eq!(
            schema["properties"]["value"]["items"]["properties"]["usage"]["type"],
            "number"
        );
    }

    #[test]
    fn scalar_payloads_get_a_wrapped_channel() {
        for (payload, kind) in [
            ("1763400000", "integer"),
            ("12.5", "number"),
            ("\"text\"", "string"),
            ("true", "boolean"),
        ] {
            let descriptor = json_descriptor(payload);
            assert!(descriptor.wrap_json_value, "{payload}");
            assert_eq!(schema(&descriptor)["properties"]["value"]["type"], kind);
        }
    }

    #[test]
    fn object_payload_is_unchanged() {
        let descriptor = json_descriptor(r#"{"total": 4096, "used": 1024}"#);
        assert!(!descriptor.wrap_json_value);
        let schema = schema(&descriptor);
        assert_eq!(schema["properties"]["total"]["type"], "integer");
        assert!(schema["properties"].get("value").is_none());
    }

    #[test]
    fn wrapped_payload_matches_the_schema() {
        let wrapped = wrap_json_payload(br#"[{"name": "cpu0", "usage": 12.5}]"#).unwrap();
        let wrapped: Value = serde_json::from_slice(&wrapped).unwrap();
        assert_eq!(
            wrapped,
            json!({ "value": [{ "name": "cpu0", "usage": 12.5 }] })
        );

        let wrapped = wrap_json_payload(b"42").unwrap();
        assert_eq!(wrapped, br#"{"value":42}"#);

        // JSON5, as accepted when the channel is created
        let wrapped = wrap_json_payload(b"['a', 1,]").unwrap();
        assert_eq!(wrapped, br#"{"value":["a",1]}"#);

        assert!(wrap_json_payload(b"not json").is_none());
    }
}
