use anyhow::Result;
use elasticsearch::Elasticsearch;
use fluvio::Offset;
use fluvio_connector_common::{LocalBoxSink, Sink};
use async_trait::async_trait;
use crate::config::ElasticSearchConfig;

pub(crate) struct ElasticSearchSink{
    client: Elasticsearch
}

impl ElasticSearchSink{
    pub(crate) fn new(config: &ElasticSearchConfig) -> Result<Self>{
        let client = Elasticsearch::default();
        Ok(Self { client })
    }
}

#[async_trait]
impl Sink<String> for ElasticSearchSink {
    async fn connect(self, _offset: Option<Offset>) -> Result<LocalBoxSink<String>>{
        let client = self.client;
        let unfold = futures::sink::unfold(
        client,
        |mut client: Elasticsearch, record: String| async move{
            Ok::<_, anyhow::Error>(client)
        });
        Ok(Box::pin(unfold))
    }
}
