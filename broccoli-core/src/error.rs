#[derive(Debug, thiserror::Error)]
pub enum BroccoliError {
    #[error("Broker error: {0}")]
    Broker(String),

    #[error("Failed to publish message: {0}")]
    Publish(String),

    #[error("Failed to consume message: {0}")]
    Consume(String),

    #[error("Failed to acknowledge message: {0}")]
    Acknowledge(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("Job error: {0}")]
    Job(String),

    #[error("Connection timeout after {0} retries")]
    ConnectionTimeout(u32),
}
