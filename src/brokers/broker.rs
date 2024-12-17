use crate::error::BroccoliError;

/// Trait for message broker implementations.
#[async_trait::async_trait]
pub trait Broker: Send + Sync {
    /// Connects to the broker using the provided URL.
    ///
    /// # Arguments
    /// * `broker_url` - The URL of the broker.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    async fn connect(&mut self, broker_url: &str) -> Result<(), BroccoliError>;

    /// Publishes a message to the specified queue.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    /// * `message` - The message to be published.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    async fn publish(&self, queue_name: &str, message: &[String]) -> Result<(), BroccoliError>;

    /// Attempts to consume a message from the specified queue.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    ///
    /// # Returns
    /// A `Result` containing an `Some(String)` with the message if available or `None`
    /// if no message is avaiable, and a `BroccoliError` on failure.
    async fn try_consume(&self, queue_name: &str) -> Result<Option<String>, BroccoliError>;

    /// Consumes a message from the specified queue, blocking until a message is available.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    ///
    /// # Returns
    /// A `Result` containing the message as a `String`, or a `BroccoliError` on failure.
    async fn consume(&self, queue_name: &str) -> Result<String, BroccoliError>;

    /// Acknowledges the processing of a message, removing it from the processing queue.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    /// * `message` - The message to be acknowledged.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    async fn acknowledge(&self, queue_name: &str, message: String) -> Result<(), BroccoliError>;

    /// Rejects a message, re-queuing it or moving it to a failed queue if the retry limit is reached.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    /// * `message` - The message to be rejected.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    async fn reject(&self, queue_name: &str, message: String) -> Result<(), BroccoliError>;
}

/// Configuration options for broker behavior.
pub struct BrokerConfig {
    /// Maximum number of retry attempts for failed messages
    pub retry_attempts: Option<u8>,
    /// Whether to retry failed messages
    pub retry_failed: Option<bool>,
    /// Number of connections to maintain in the connection pool
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

/// A wrapper for messages that includes metadata for processing.
///
/// # Type Parameters
/// * `T` - The type of the payload, must implement Clone and Serialize
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BrokerMessage<T: Clone + serde::Serialize> {
    /// Unique identifier for the message
    pub task_id: uuid::Uuid,
    /// The actual message content
    pub payload: T,
    /// Number of processing attempts made
    pub attempts: u8,
}

impl<T: Clone + serde::Serialize> BrokerMessage<T> {
    /// Creates a new `BrokerMessage` with the provided payload.
    pub fn new(payload: T) -> Self {
        BrokerMessage {
            task_id: uuid::Uuid::new_v4(),
            payload,
            attempts: 0,
        }
    }

    /// Creates a new `BrokerMessage` with the provided payload and number of attempts.
    pub fn new_with_attempts(payload: T, attempts: u8) -> Self {
        BrokerMessage {
            task_id: uuid::Uuid::new_v4(),
            payload,
            attempts,
        }
    }
}

/// Supported message broker implementations.
pub enum BrokerType {
    /// Redis-based message broker
    Redis,
}
