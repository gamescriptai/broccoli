use crate::error::BroccoliError;

#[async_trait::async_trait]
pub trait Broker: Send + Sync {
    async fn connect(&mut self, broker_url: &str) -> Result<(), BroccoliError>;
    async fn publish(&self, queue_name: &str, message: String) -> Result<(), BroccoliError>;
    async fn try_consume(&self, queue_name: &str) -> Result<Option<String>, BroccoliError>;
    async fn consume(&self, queue_name: &str) -> Result<String, BroccoliError>;
    async fn acknowledge(&self, queue_name: &str, message: String) -> Result<(), BroccoliError>;
    async fn reject(&self, queue_name: &str, message: String) -> Result<(), BroccoliError>;
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BrokerMessage<T: Clone + serde::Serialize> {
    pub payload: T,
    pub attempts: u8,
}

impl<T: Clone + serde::Serialize> BrokerMessage<T> {
    pub fn new(payload: T) -> Self {
        BrokerMessage {
            payload,
            attempts: 0,
        }
    }

    pub fn new_with_attempts(payload: T, attempts: u8) -> Self {
        BrokerMessage { payload, attempts }
    }
}

pub enum BrokerType {
    Redis,
}
