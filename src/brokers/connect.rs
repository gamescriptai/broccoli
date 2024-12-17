use super::{
    broker::{Broker, BrokerConfig},
    redis::broker::RedisBroker,
};
use crate::error::BroccoliError;

pub(crate) async fn connect_to_broker(
    broker_url: &str,
    config: Option<BrokerConfig>,
) -> Result<Box<dyn Broker>, BroccoliError> {
    if broker_url.starts_with("redis://") {
        let mut broker = match config {
            Some(cfg) => RedisBroker::new_with_config(cfg),
            None => RedisBroker::new(),
        };
        broker.connect(broker_url).await?;
        Ok(Box::new(broker))
    } else {
        Err(BroccoliError::Broker(format!(
            "Unsupported broker URL scheme: {}",
            broker_url
        )))
    }
}
