use std::{borrow::Cow, fmt, path::PathBuf};

use anyhow::Result;
use tracing::*;
use zenoh::bytes::{Encoding, ZBytes};

pub struct ChannelDescriptor {
    pub topic: String,
    pub schema: Option<SchemaDescriptor>,
    pub message_encoding: MessageEncoding,
}

pub struct SchemaDescriptor {
    pub name: String,
    pub encoding: SchemaEncoding,
    pub content: String,
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
    #[instrument(
        skip_all,
        fields(schema_name = tracing::field::Empty),
    )]
    pub fn new(
        topic: &str,
        encoding: &Encoding,
        _payload: &ZBytes,
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

        // For more information: https://mcap.dev/spec/registry#well-known-schema-encodings
        match (encoding, schema) {
            (encoding, Some(schema_name)) if encoding == Cow::from(Encoding::APPLICATION_CDR) => {
                Span::current().record("schema_name", schema_name);
                let schema_content = match load_cdr_schema(schema_name, schema_path) {
                    Ok(schema) => schema,
                    Err(error) => {
                        error!(%error, "Failed to load schema");
                        return None;
                    }
                };
                Some(Self {
                    topic: topic.to_owned(),
                    schema: Some(SchemaDescriptor {
                        name: schema_name.to_owned(),
                        encoding: SchemaEncoding::Ros2Msg,
                        content: schema_content,
                    }),
                    message_encoding: MessageEncoding::Cdr,
                })
            }
            (encoding, schema) if encoding == Cow::from(Encoding::APPLICATION_JSON) => {
                let schema_name = match schema {
                    Some(name) => name.to_owned(),
                    None => topic.replace('/', "."),
                };
                Span::current().record("schema_name", schema_name.as_str());
                Some(Self {
                    topic: topic.to_owned(),
                    schema: Some(SchemaDescriptor {
                        name: schema_name,
                        encoding: SchemaEncoding::JsonSchema,
                        content: r#"{"type":"object"}"#.to_owned(),
                    }),
                    message_encoding: MessageEncoding::Json,
                })
            }
            (encoding, _schema) if encoding == Cow::from(Encoding::APPLICATION_OCTET_STREAM) => {
                Some(Self {
                    topic: topic.to_owned(),
                    schema: None,
                    message_encoding: MessageEncoding::OctetStream,
                })
            }
            _ => {
                warn!("Received unknown encoding");
                None
            }
        }
    }
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
