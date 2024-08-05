mod config;
mod sink;

use config::ElasticSearchConfig;


use futures::StreamExt;

use fluvio_connector_common::{connector, consumer::ConsumerStream, Result};

#[connector(sink)]
async fn start(config: ElasticSearchConfig, mut stream: impl ConsumerStream) -> Result<()> {
    println!("Starting es-sink-connector sink connector with {config:?}");
    while let Some(Ok(record)) = stream.next().await {
        let val = String::from_utf8_lossy(record.value());
        println!("{val}");
    }
    Ok(())
}

