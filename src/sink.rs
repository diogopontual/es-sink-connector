use crate::config::ElasticSearchConfig;
use anyhow::Result;
use async_trait::async_trait;
use elasticsearch::{auth::Credentials, http::transport::Transport, Elasticsearch, IndexParts};
use fluvio::Offset;
use fluvio_connector_common::{tracing, LocalBoxSink, Sink};

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

        let request = client
            .index(IndexParts::Index(&self.index));

        let unfold = futures::sink::unfold(
            request,
            |request, record: String| async move {
                tracing::trace!("{:?}", record);
                request.clone()
                    .body(serde_json::from_str(&record)?)
                    .send()
                    .await?;
                Ok::<_, anyhow::Error>(request)
            },
        );
        Ok(Box::pin(unfold))
    }
}
