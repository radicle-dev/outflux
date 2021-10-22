use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use reqwest::Url;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("HTTP client error")]
    ReqwestError(#[from] reqwest::Error),

    #[error("Invalid headers value")]
    InvalidHeaderValueError(#[from] reqwest::header::InvalidHeaderValue),

    #[error("URL parse error")]
    URLParseError(#[from] url::ParseError),

    #[error("SystemTime error")]
    SystemTimeError(#[from] std::time::SystemTimeError),

    #[error("No measurement fields set (at least one is required")]
    AtLeastOneMeasurementFieldRequired,
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Client {
    authenticated_client: Arc<reqwest::Client>,
    write_endpoint_url: Url,
}

fn make_authenticated_client_builder(auth_token: &str) -> Result<reqwest::ClientBuilder> {
    let mut auth_value = reqwest::header::HeaderValue::from_str(auth_token)?;
    auth_value.set_sensitive(true);
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(reqwest::header::AUTHORIZATION, auth_value);
    let reqwest_client_builder = reqwest::Client::builder().default_headers(headers);
    Ok(reqwest_client_builder)
}

fn make_write_endpoint_url(url: &str) -> Result<Url> {
    Ok(Url::parse(&url)?.join("/api/v2/write")?)
}

impl Client {
    pub fn new(url: &str, auth_token: &str) -> Result<Self> {
        let client_builder = make_authenticated_client_builder(auth_token)?;
        let result = Self {
            authenticated_client: Arc::new(client_builder.build()?),
            write_endpoint_url: make_write_endpoint_url(url)?,
        };
        Ok(result)
    }

    pub fn make_bucket(&self, org: &str, bucket: &str) -> Result<Bucket> {
        let mut write_endpoint_url = self.write_endpoint_url.clone();
        write_endpoint_url
            .query_pairs_mut()
            .append_pair("org", org)
            .append_pair("bucket", bucket);
        let result = Bucket {
            authenticated_client: Arc::clone(&self.authenticated_client),
            write_endpoint_url,
        };
        Ok(result)
    }
}

pub enum FieldValue {
    Float(f64),
    Integer(i64),
    UInteger(u64),
    String(String),
    Boolean(bool),
}

impl ToString for FieldValue {
    fn to_string(&self) -> String {
        match self {
            FieldValue::Float(f) => format!("{}", f),
            FieldValue::Integer(i) => format!("{}i", i),
            FieldValue::UInteger(u) => format!("{}u", u),
            FieldValue::String(s) => format!(r#""{}""#, escape_field_value(s)),
            FieldValue::Boolean(b) => match b {
                true => "t".to_string(),
                false => "f".to_string(),
            },
        }
    }
}

pub struct MeasurementBuilder {
    name: String,
    fields: Option<BTreeMap<String, FieldValue>>,
    tags: Option<BTreeMap<String, String>>,
    unix_timestamp: Option<Duration>,
}

impl MeasurementBuilder {
    fn new(name: String) -> Self {
        Self {
            name,
            fields: Default::default(),
            tags: Default::default(),
            unix_timestamp: Default::default(),
        }
    }

    pub fn fields(mut self, fields: BTreeMap<String, FieldValue>) -> Self {
        self.fields = Some(fields);
        self
    }

    pub fn tags(mut self, tags: BTreeMap<String, String>) -> Self {
        self.tags = Some(tags);
        self
    }

    pub fn timestamp(mut self, timestamp: SystemTime) -> Result<Self> {
        self.unix_timestamp = Some(timestamp.duration_since(SystemTime::UNIX_EPOCH)?);
        Ok(self)
    }

    pub fn build(self) -> Result<Measurement> {
        let fields = match self.fields {
            None => return Err(Error::AtLeastOneMeasurementFieldRequired),
            Some(map) if map.is_empty() => return Err(Error::AtLeastOneMeasurementFieldRequired),
            Some(map) => map,
        };
        let unix_timestamp = match self.unix_timestamp {
            Some(timestamp) => timestamp,
            None => {
                let now = SystemTime::now();
                now.duration_since(SystemTime::UNIX_EPOCH)?
            }
        };
        let result = Measurement {
            name: self.name,
            fields,
            tags: self.tags.unwrap_or_default(),
            unix_timestamp,
        };
        Ok(result)
    }
}

pub struct Measurement {
    name: String,
    fields: BTreeMap<String, FieldValue>,
    tags: BTreeMap<String, String>,
    unix_timestamp: Duration,
}

impl Measurement {
    pub fn builder<S: Into<String>>(name: S) -> MeasurementBuilder {
        MeasurementBuilder::new(name.into())
    }
}

fn escape_comma_equals_space(tag_key: &str) -> String {
    tag_key
        .replace(",", "\\,")
        .replace("=", "\\=")
        .replace(" ", "\\ ")
}

fn escape_field_value(field_value: &str) -> String {
    field_value
        .replace(r#"\"#, r#"\\"#)
        .replace(r#"""#, r#"\""#)
}

impl ToString for Measurement {
    fn to_string(&self) -> String {
        let escaped_name = self.name.replace(",", "\\,").replace(" ", "\\ ");
        let optional_tags: Vec<String> = self
            .tags
            .iter()
            .map(|(tag_key, value)| {
                format!(
                    "{}={}",
                    escape_comma_equals_space(tag_key),
                    escape_comma_equals_space(value)
                )
            })
            .collect();
        let fields: Vec<String> = self
            .fields
            .iter()
            .map(|(field_key, value)| {
                format!(
                    "{}={}",
                    escape_comma_equals_space(field_key),
                    value.to_string()
                )
            })
            .collect();
        let tags_str = if optional_tags.is_empty() {
            "".to_string()
        } else {
            format!(",{}", optional_tags.join(","))
        };
        format!(
            "{}{} {} {}",
            escaped_name,
            tags_str,
            fields.join(","),
            self.unix_timestamp.as_nanos()
        )
    }
}

pub struct Bucket {
    authenticated_client: Arc<reqwest::Client>,
    write_endpoint_url: Url,
}

impl Bucket {
    pub async fn write(
        &self,
        measurement: &[Measurement],
        timeout: Duration,
    ) -> Result<reqwest::Response> {
        // https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/
        let lines: Vec<String> = measurement.iter().map(|m| m.to_string()).collect();
        let body = lines.join("\n");
        tracing::debug!(
            "Sending measurements {url}: {body}",
            url = self.write_endpoint_url,
            body = body,
        );
        let resp = self
            .authenticated_client
            .post(self.write_endpoint_url.clone())
            .body(body)
            .timeout(timeout)
            .send()
            .await?;
        resp.error_for_status().map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn measurement_name_with_spaces() {
        let mut fields: BTreeMap<String, FieldValue> = Default::default();
        fields.insert(
            "fieldKey".to_string(),
            FieldValue::String("string value".to_string()),
        );
        let measurement = Measurement::builder("my Measurement")
            .fields(fields)
            .timestamp(SystemTime::UNIX_EPOCH)
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            measurement.to_string(),
            r#"my\ Measurement fieldKey="string value" 0"#,
        );
    }

    #[test]
    fn double_quotes_in_a_string_field_value() {
        let mut fields: BTreeMap<String, FieldValue> = Default::default();
        fields.insert(
            "fieldKey".to_string(),
            FieldValue::String("\"string\" within a string".to_string()),
        );
        let measurement = Measurement::builder("myMeasurement")
            .fields(fields)
            .timestamp(SystemTime::UNIX_EPOCH)
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            measurement.to_string(),
            r#"myMeasurement fieldKey="\"string\" within a string" 0"#,
        );
    }

    #[test]
    fn tag_keys_and_values_with_spaces() {
        let mut fields: BTreeMap<String, FieldValue> = Default::default();
        fields.insert("fieldKey".to_string(), FieldValue::UInteger(100));

        let mut tags: BTreeMap<String, String> = Default::default();
        tags.insert("tag Key1".to_string(), "tag Value1".to_string());
        tags.insert("tag Key2".to_string(), "tag Value2".to_string());

        let measurement = Measurement::builder("myMeasurement")
            .fields(fields)
            .tags(tags)
            .timestamp(SystemTime::UNIX_EPOCH)
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            measurement.to_string(),
            r#"myMeasurement,tag\ Key1=tag\ Value1,tag\ Key2=tag\ Value2 fieldKey=100u 0"#,
        );
    }

    #[test]
    fn emojis() {
        let mut fields: BTreeMap<String, FieldValue> = Default::default();
        fields.insert(
            "fieldKey".to_string(),
            FieldValue::String("Launch 🚀".to_string()),
        );

        let mut tags: BTreeMap<String, String> = Default::default();
        tags.insert("tagKey".to_string(), "🍭".to_string());

        let measurement = Measurement::builder("myMeasurement")
            .fields(fields)
            .tags(tags)
            .timestamp(SystemTime::UNIX_EPOCH)
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            measurement.to_string(),
            r#"myMeasurement,tagKey=🍭 fieldKey="Launch 🚀" 0"#,
        );
    }
}
