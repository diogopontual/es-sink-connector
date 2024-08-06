mod config;
mod sink;

use config::ElasticSearchConfig;

use futures::{SinkExt, StreamExt};

use fluvio_connector_common::{connector, consumer::ConsumerStream, tracing, Result, Sink};
use sink::ElasticSearchSink;

#[connector(sink)]
async fn start(config: ElasticSearchConfig, mut stream: impl ConsumerStream) -> Result<()> {
     tracing::debug!(?config);

    let sink = ElasticSearchSink::new(config)?;
    let mut sink = sink.connect(None).await?;

     while let Some(item) = stream.next().await {
    //     tracing::debug!("Received record in consumer");
         let str = String::from_utf8(item?.as_ref().to_vec())?;
         sink.send(str).await?;
    }
    // tracing::info!("Consumer loop finished");
    // while let Some(Ok(record)) = stream.next().await {
    //     let val = String::from_utf8_lossy(record.value());
    //     println!("{val}");
    // }

    Ok(())
}
