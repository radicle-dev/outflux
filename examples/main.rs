use std::{collections::BTreeMap, time::Duration};

use outflux::{FieldValue, Measurement};

#[tokio::main]
async fn main() {
    let influxdb_client =
        outflux::Client::new("http://127.0.0.1:8086", "my-influxdb-token").unwrap();
    let bucket = influxdb_client
        .make_bucket("my-org", "my-bucket")
        .unwrap();

    let mut interval = tokio::time::interval(Duration::from_secs(15));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;

        let mut fields: BTreeMap<String, FieldValue> = Default::default();
        fields.insert("my-masurement-field".to_string(), FieldValue::UInteger(123));

        let mut tags: BTreeMap<String, String> = Default::default();
        tags.insert("my-measurement-tag".to_string(), "foo".to_string());

        let measurement = Measurement::builder("my-measurement-name")
            .fields(fields)
            .tags(tags)
            .build()
            .unwrap();

        if let Err(e) = bucket.write(&[measurement], Duration::from_secs(5)).await {
            tracing::error!("Could not send metrics: {:?}", e);
        }
    }
}
