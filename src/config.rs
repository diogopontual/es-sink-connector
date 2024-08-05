use fluvio_connector_common::connector;

#[connector(config)]
#[derive(Debug)]
pub(crate) struct ElasticSearchConfig {
    index: String,
    cloudId: String,
    username: String,
    password: String,
    url: String
}
