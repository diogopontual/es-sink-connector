use crate::config::ElasticSearchConfig;
use anyhow::Result;
use async_trait::async_trait;
use elasticsearch::{auth::Credentials, http::transport::Transport, Elasticsearch, IndexParts, Index};
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
                let credentials = Credentials::Basic(config.username, config.password);
                let transport = Transport::cloud(&config.url, credentials)?;
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
                let request: Index<()> = client.index(IndexParts::Index(&index_name));
                let parse_result: serde_json::error::Result<Value> = serde_json::from_str(&record);
                match parse_result {
                    Ok(obj) => {
                        request.clone()
                            .body(obj)
                            .send()
                            .await?;
                        tracing::debug!("Record sent to Elasticsearch");
                    },
                    Err(error) => {
                        tracing::error!("Error parsing record: {:?}", error);
                    }
                }
                Ok::<_, anyhow::Error>((client, index_name))
            },
        );
        Ok(Box::pin(unfold))
    }
}
