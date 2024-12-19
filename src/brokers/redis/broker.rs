use redis::{AsyncCommands, LposOptions};

use crate::{
    brokers::broker::{Broker, BrokerConfig, InternalBrokerMessage},
    error::BroccoliError,
};

pub(crate) type RedisPool = bb8_redis::bb8::Pool<bb8_redis::RedisConnectionManager>;
type RedisConnection<'a> = bb8_redis::bb8::PooledConnection<'a, bb8_redis::RedisConnectionManager>;

#[derive(Default)]
/// A message broker implementation for Redis.
pub struct RedisBroker {
    pub(crate) redis_pool: Option<RedisPool>,
    pub(crate) connected: bool,
    pub(crate) config: Option<BrokerConfig>,
}

/// Retrieves a Redis connection from the pool, retrying with exponential backoff if necessary.
///
/// # Arguments
/// * `redis_pool` - A reference to the Redis connection pool.
///
/// # Returns
/// A `Result` containing a `RedisConnection` on success, or a `BroccoliError` on failure.
pub(crate) async fn get_redis_connection(
    redis_pool: &RedisPool,
) -> Result<RedisConnection, BroccoliError> {
    let mut redis_conn_sleep = std::time::Duration::from_secs(1);

    #[allow(unused_assignments)]
    let mut opt_redis_connection = None;

    loop {
        let borrowed_redis_connection = match redis_pool.get().await {
            Ok(redis_connection) => Some(redis_connection),
            Err(err) => {
                BroccoliError::Broker(format!("Failed to get redis connection: {:?}", err));
                None
            }
        };

        if borrowed_redis_connection.is_some() {
            opt_redis_connection = borrowed_redis_connection;
            break;
        }

        tokio::time::sleep(redis_conn_sleep).await;
        redis_conn_sleep = std::cmp::min(redis_conn_sleep * 2, std::time::Duration::from_secs(300));
    }

    let redis_connection =
        opt_redis_connection.expect("Failed to get redis connection outside of loop");

    Ok(redis_connection)
}

/// Implementation of the `Broker` trait for `RedisBroker`.
#[async_trait::async_trait]
impl Broker for RedisBroker {
    /// Connects to the Redis broker using the provided URL.
    ///
    /// # Arguments
    /// * `broker_url` - The URL of the Redis broker.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    async fn connect(&mut self, broker_url: &str) -> Result<(), BroccoliError> {
        let redis_manager = bb8_redis::RedisConnectionManager::new(broker_url).map_err(|e| {
            BroccoliError::Broker(format!("Failed to create redis manager: {:?}", e))
        })?;

        let redis_pool = bb8_redis::bb8::Pool::builder()
            .max_size(
                self.config
                    .as_ref()
                    .map(|config| config.pool_connections.unwrap_or(10))
                    .unwrap_or(10)
                    .into(),
            )
            .connection_timeout(std::time::Duration::from_secs(2))
            .build(redis_manager)
            .await
            .expect("Failed to create redis pool");

        self.redis_pool = Some(redis_pool);
        self.connected = true;
        Ok(())
    }

    /// Publishes a message to the specified queue.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    /// * `message` - The message to be published.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    async fn publish(
        &self,
        queue_name: &str,
        messages: &[InternalBrokerMessage],
    ) -> Result<Vec<InternalBrokerMessage>, BroccoliError> {
        let redis_pool = self.ensure_pool()?;
        let mut redis_connection = get_redis_connection(&redis_pool).await?;
        for msg in messages {
            let attempts = msg.attempts.to_string();
            let items: Vec<(&str, &str)> = vec![
                ("task_id", &msg.task_id),
                ("payload", &msg.payload),
                ("attempts", &attempts),
            ];

            redis_connection
                .hset_multiple::<&str, &str, &str, String>(&msg.task_id.to_string(), &items)
                .await?;
        }

        redis_connection
            .lpush::<&str, Vec<String>, String>(
                queue_name,
                messages
                    .iter()
                    .map(|msg| msg.task_id.clone())
                    .collect::<Vec<String>>(),
            )
            .await?;

        Ok(messages.to_vec())
    }

    /// Attempts to consume a message from the specified queue.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    ///
    /// # Returns
    /// A `Result` containing an `Some(String)` with the message if available or `None`
    /// if no message is avaiable, and a `BroccoliError` on failure.
    async fn try_consume(
        &self,
        queue_name: &str,
    ) -> Result<Option<InternalBrokerMessage>, BroccoliError> {
        let redis_pool = self.ensure_pool()?;
        let mut redis_connection = get_redis_connection(&redis_pool).await?;

        let task_id: String = redis_connection
            .blmove(
                queue_name,
                format!("{}_processing", queue_name),
                redis::Direction::Right,
                redis::Direction::Left,
                1.0,
            )
            .await?;

        let payload = redis_connection
            .hgetall(&task_id)
            .await
            .map_err(|e| BroccoliError::Consume(format!("Failed to consume message: {:?}", e)))?;

        Ok(Some(payload))
    }

    /// Consumes a message from the specified queue, blocking until a message is available.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    ///
    /// # Returns
    /// A `Result` containing the message as a `String`, or a `BroccoliError` on failure.
    async fn consume(&self, queue_name: &str) -> Result<InternalBrokerMessage, BroccoliError> {
        let redis_pool = self.ensure_pool()?;

        let mut redis_connection = get_redis_connection(&redis_pool).await?;
        let mut broken_pipe_sleep = std::time::Duration::from_secs(10);
        let mut message: Option<InternalBrokerMessage> = None;

        while message.is_none() {
            let task_id_result: Result<String, _> = redis_connection
                .blmove(
                    queue_name,
                    format!("{}_processing", queue_name),
                    redis::Direction::Right,
                    redis::Direction::Left,
                    1.0,
                )
                .await;

            let task_id = if let Ok(task_id) = task_id_result {
                broken_pipe_sleep = std::time::Duration::from_secs(10);

                if task_id.is_empty() {
                    continue;
                }

                task_id.clone()
            } else {
                if task_id_result.is_err_and(|err| err.is_io_error()) {
                    tokio::time::sleep(broken_pipe_sleep).await;
                    broken_pipe_sleep =
                        std::cmp::min(broken_pipe_sleep * 2, std::time::Duration::from_secs(300));
                }

                continue;
            };

            message = redis_connection.hgetall(&task_id).await?;
        }

        Ok(message.expect("Must have a message to exit loop"))
    }

    /// Acknowledges the processing of a message, removing it from the processing queue.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    /// * `message` - The message to be acknowledged.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    async fn acknowledge(
        &self,
        queue_name: &str,
        message: InternalBrokerMessage,
    ) -> Result<(), BroccoliError> {
        let redis_pool = self.ensure_pool()?;

        let mut redis_connection = get_redis_connection(&redis_pool).await?;

        redis_connection
            .lrem::<String, &str, String>(format!("{}_processing", queue_name), 1, &message.task_id)
            .await?;

        redis_connection
            .del::<&str, String>(&message.task_id)
            .await?;

        Ok(())
    }

    /// Rejects a message, re-queuing it or moving it to a failed queue if the retry limit is reached.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    /// * `message` - The message to be rejected.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    async fn reject(
        &self,
        queue_name: &str,
        message: InternalBrokerMessage,
    ) -> Result<(), BroccoliError> {
        let redis_pool = self.ensure_pool()?;

        let mut redis_connection = get_redis_connection(&redis_pool).await?;

        let attempts = message.attempts + 1;

        redis_connection
            .lrem::<String, &str, String>(format!("{}_processing", queue_name), 1, &message.task_id)
            .await?;

        if (attempts
            >= self
                .config
                .as_ref()
                .map(|config| config.retry_attempts.unwrap_or(3))
                .unwrap_or(3))
            || !self
                .config
                .as_ref()
                .map(|config| config.retry_failed.unwrap_or(true))
                .unwrap_or(true)
        {
            redis_connection
                .lpush::<String, &str, String>(format!("{}_failed", queue_name), &message.task_id)
                .await?;

            log::error!(
                "Message {} has reached max attempts and has been pushed to failed queue",
                message.task_id
            );

            return Ok(());
        }

        if self
            .config
            .as_ref()
            .map(|config| config.retry_failed.unwrap_or(true))
            .unwrap_or(true)
        {
            redis_connection
                .lpush::<String, &str, String>(queue_name.to_string(), &message.task_id)
                .await?;

            redis_connection
                .hset::<&str, &str, u8, String>(&message.task_id, "attempts", attempts)
                .await?;
        }

        Ok(())
    }

    /// Cancels a message, removing it from the queue.
    async fn cancel(&self, queue_name: &str, message_id: String) -> Result<(), BroccoliError> {
        let redis_pool = self.ensure_pool()?;

        let mut redis_connection = get_redis_connection(&redis_pool).await?;

        redis_connection
            .lrem::<&str, &str, String>(queue_name, 1, &message_id)
            .await?;

        redis_connection.del::<&str, String>(&message_id).await?;

        Ok(())
    }

    /// Gets the position of a message in the queue.
    async fn get_message_position(
        &self,
        queue_name: &str,
        message_id: String,
    ) -> Result<Option<usize>, BroccoliError> {
        let redis_pool = self.ensure_pool()?;

        let mut redis_connection = get_redis_connection(&redis_pool).await?;

        let position = redis_connection
            .lpos::<&str, &str, Option<usize>>(queue_name, &message_id, LposOptions::default())
            .await?;

        Ok(position)
    }
}
