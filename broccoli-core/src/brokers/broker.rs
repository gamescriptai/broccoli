use crate::error::BroccoliError;

use super::redis::broker::RedisBroker;

#[async_trait::async_trait]
pub trait Broker {
    async fn connect(&mut self, broker_url: &str) -> Result<(), BroccoliError>;
    async fn publish<T: Clone + serde::Serialize>(
        &self,
        queue_name: &str,
        message: T,
    ) -> Result<(), BroccoliError>;
    async fn try_consume<T: Clone + serde::Serialize + serde::de::DeserializeOwned>(
        &self,
        queue_name: &str,
    ) -> Result<Option<BrokerMessage<T>>, BroccoliError>;
    async fn consume<T: Clone + serde::Serialize + serde::de::DeserializeOwned>(
        &self,
        queue_name: &str,
    ) -> Result<BrokerMessage<T>, BroccoliError>;
    async fn retry<T: Clone + serde::Serialize>(
        &self,
        queue_name: &str,
        message: &mut BrokerMessage<T>,
    ) -> Result<(), BroccoliError>;
}

pub struct BrokerConfig {
    pub retry_attempts: Option<u8>,
    pub retry_failed: Option<bool>,
    pub pool_connections: Option<u8>,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        BrokerConfig {
            retry_attempts: Some(3),
            retry_failed: Some(true),
            pool_connections: Some(10),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct BrokerMessage<T: Clone + serde::Serialize> {
    pub task_id: uuid::Uuid,
    pub payload: T,
    pub attempts: u8,
}

impl<T: Clone + serde::Serialize> BrokerMessage<T> {
    pub fn new(payload: T) -> Self {
        BrokerMessage {
            task_id: uuid::Uuid::new_v4(),
            payload,
            attempts: 0,
        }
    }
}

pub enum BrokerType {
    Redis,
}
