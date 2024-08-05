mod config;
mod sink;

use config::ElasticSearchConfig;


use futures::StreamExt;

use fluvio_connector_common::{connector, consumer::ConsumerStream, Result, tracing};
use sink::ElasticSearchSink;

#[connector(sink)]
async fn start(config: ElasticSearchConfig, mut stream: impl ConsumerStream) -> Result<()> {
    tracing::debug!(?config);
    
    let sink = ElasticSearchSink::new(config)?;

    while let Some(Ok(record)) = stream.next().await {
        tracing::debug!("Received record in consumer");
        let val = String::from_utf8_lossy(record.value());
        
        println!("{val}");
    }
    Ok(())
}

