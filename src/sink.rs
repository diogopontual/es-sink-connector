use crate::config::ElasticSearchConfig;
use anyhow::Result;
use async_trait::async_trait;
use elasticsearch::{http::transport::Transport, Elasticsearch, Index, IndexParts};
use fluvio::Offset;
use fluvio_connector_common::{tracing, LocalBoxSink, Sink};
use serde_json::Value;

pub(crate) struct ElasticSearchSink {
    client: Elasticsearch,
    index: String,
}

impl ElasticSearchSink {
    pub(crate) fn new(config: ElasticSearchConfig) -> Result<Self> {
        let client: Elasticsearch;
        match config.secure_enabled {
            false => {
                let transport = Transport::single_node(&config.url)?;
                client = Elasticsearch::new(transport);
            }
            _ => {
                let concatenated = format!(
                    "http://{}:{}@{}",
                    config.username, config.password, config.url
                );
                let transport = Transport::single_node(&concatenated)?;
                client = Elasticsearch::new(transport);
            }
        }

        Ok(Self {
            client,
            index: config.index,
        })
    }
}

#[async_trait]
impl Sink<String> for ElasticSearchSink {
    async fn connect(self, _offset: Option<Offset>) -> Result<LocalBoxSink<String>> {
        let client = self.client;
        let unfold = futures::sink::unfold(
            (client, self.index),
            |(client, index_name), record: String| async move {
                tracing::trace!("{:?}", record);

                let value: Value = match serde_json::from_str(&record) {
                    Ok(value) => value,
                    Err(error) => {
                        //this error is not reversible
                        tracing::error!("Error parsing record: {:?}", error);
                        return Err(anyhow::anyhow!("Error parsing record: {:?}", error));
                    }
                };
                //this error is not reversible
                let request: Index<()> = client.index(IndexParts::Index(&index_name));
                let _response = request.body(value).send().await?;
                println!("{:#?}", _response);
                tracing::debug!("Record sent to Elasticsearch");

                Ok::<_, anyhow::Error>((client, index_name))
            },
        );
        Ok(Box::pin(unfold))
    }
}
