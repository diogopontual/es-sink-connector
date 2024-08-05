use anyhow::Result;
use elasticsearch::{auth::Credentials, http::transport::Transport, Elasticsearch};
use fluvio::Offset;
use fluvio_connector_common::{LocalBoxSink, Sink};
use async_trait::async_trait;
use crate::config::ElasticSearchConfig;

pub(crate) struct ElasticSearchSink{
    client: Elasticsearch,
    index: String
}

impl ElasticSearchSink{
    pub(crate) fn new(config: ElasticSearchConfig) -> Result<Self>{
        let client: Elasticsearch;
        match config.secure_enabled {
            false => {
                let transport = Transport::single_node(&config.url)?;
                client = Elasticsearch::new(transport);
            },
            _ => {
                let credentials = Credentials::Basic(config.username, config.password);
                let transport = Transport::cloud(&config.url, credentials)?;
                client = Elasticsearch::new(transport);
            } 
        }
        
           
        Ok(Self { client, 
            index: config.index,
        })
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
