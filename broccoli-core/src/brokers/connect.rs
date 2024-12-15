use crate::error::BroccoliError;

use super::{broker::Broker, redis::broker::RedisBroker};

pub(crate) async fn connect_to_broker(broker_url: &str) -> Result<Box<dyn Broker>, BroccoliError> {
    if broker_url.starts_with("redis://") {
        let mut broker = RedisBroker::new();
        broker.connect(broker_url).await?;
        Ok(Box::new(broker))
    } else {
        Err(BroccoliError::BrokerError(
            "Unknown broker type".to_string(),
        ))
    }
}
