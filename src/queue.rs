use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::stream::FuturesUnordered;

use crate::{
    brokers::{
        broker::{Broker, BrokerConfig, BrokerMessage},
        connect::connect_to_broker,
    },
    error::BroccoliError,
};

/// Configuration for message retry behavior.
///
/// This struct defines how failed messages should be handled,
/// including whether they should be retried and how many attempts should be made.
pub struct RetryStrategy {
    /// Whether failed messages should be retried
    pub retry_failed: bool,
    /// Maximum number of retry attempts, if None defaults to 3
    pub attempts: Option<u8>,
}

impl Default for RetryStrategy {
    /// Creates a default retry strategy with 3 attempts and retries enabled.
    fn default() -> Self {
        Self {
            retry_failed: true,
            attempts: Some(3),
        }
    }
}

impl RetryStrategy {
    /// Creates a new retry strategy with default settings (3 attempts, retry enabled).
    ///
    /// # Returns
    /// A new `RetryStrategy` instance with default configuration.
    pub fn new() -> Self {
        Self {
            retry_failed: true,
            attempts: Some(3),
        }
    }

    /// Sets the maximum number of retry attempts.
    ///
    /// # Arguments
    /// * `attempts` - The maximum number of times a failed message should be retried.
    ///
    /// # Returns
    /// The updated `RetryStrategy` instance.
    pub fn with_attempts(mut self, attempts: u8) -> Self {
        self.attempts = Some(attempts);
        self
    }

    /// Enables or disables message retrying.
    ///
    /// # Arguments
    /// * `retry_failed` - If true, failed messages will be retried according to the attempts configuration.
    ///                    If false, failed messages will be immediately moved to the failed queue.
    ///
    /// # Returns
    /// The updated `RetryStrategy` instance.
    pub fn retry_failed(mut self, retry_failed: bool) -> Self {
        self.retry_failed = retry_failed;
        self
    }
}

/// Builder for configuring and creating a BroccoliQueue instance.
///
/// This struct provides a fluent interface for setting up queue parameters
/// such as retry strategy and connection pool size.
pub struct BroccoliQueueBuilder {
    /// The URL of the message broker
    broker_url: String,
    /// Maximum number of retry attempts for failed messages
    retry_attempts: Option<u8>,
    /// Whether failed messages should be retried
    retry_failed: Option<bool>,
    /// Number of connections to maintain in the connection pool
    pool_connections: Option<u8>,
}

impl BroccoliQueueBuilder {
    /// Creates a new `BroccoliQueueBuilder` with the specified broker URL.
    ///
    /// # Arguments
    /// * `broker_url` - The URL of the broker.
    ///
    /// # Returns
    /// A new `BroccoliQueueBuilder` instance.
    pub fn new(broker_url: impl Into<String>) -> Self {
        Self {
            broker_url: broker_url.into(),
            retry_attempts: None,
            retry_failed: None,
            pool_connections: None,
        }
    }

    /// Sets the retry strategy for failed messages.
    ///
    /// # Arguments
    /// * `strategy` - The retry strategy to use.
    ///
    /// # Returns
    /// The updated `BroccoliQueueBuilder` instance.
    pub fn failed_message_retry_strategy(mut self, strategy: RetryStrategy) -> Self {
        self.retry_attempts = strategy.attempts;
        self.retry_failed = Some(strategy.retry_failed);
        self
    }

    /// Sets the number of connections in the connection pool.
    ///
    /// # Arguments
    /// * `connections` - The number of connections.
    ///
    /// # Returns
    /// The updated `BroccoliQueueBuilder` instance.
    pub fn pool_connections(mut self, connections: u8) -> Self {
        self.pool_connections = Some(connections);
        self
    }

    /// Builds the `BroccoliQueue` with the specified configuration.
    ///
    /// # Returns
    /// A `Result` containing the `BroccoliQueue` on success, or a `BroccoliError` on failure.
    pub async fn build(self) -> Result<BroccoliQueue, BroccoliError> {
        let config = BrokerConfig {
            retry_attempts: self.retry_attempts,
            retry_failed: self.retry_failed,
            pool_connections: self.pool_connections,
        };

        let broker = connect_to_broker(&self.broker_url, Some(config))
            .await
            .map_err(|e| BroccoliError::Broker(format!("Failed to connect to broker: {}", e)))?;

        Ok(BroccoliQueue {
            broker: Arc::new(broker),
        })
    }
}

/// Main queue interface for interacting with the message broker.
///
/// BroccoliQueue provides methods for publishing and consuming messages,
/// as well as processing messages with custom handlers.
pub struct BroccoliQueue {
    /// The underlying message broker implementation
    broker: Arc<Box<dyn Broker>>,
}

impl Clone for BroccoliQueue {
    fn clone(&self) -> Self {
        Self {
            broker: Arc::clone(&self.broker),
        }
    }
}

impl BroccoliQueue {
    /// Creates a new `BroccoliQueueBuilder` with the specified broker URL.
    ///
    /// # Arguments
    /// * `broker_url` - The URL of the broker.
    ///
    /// # Returns
    /// A new `BroccoliQueueBuilder` instance.
    pub fn builder(broker_url: impl Into<String>) -> BroccoliQueueBuilder {
        BroccoliQueueBuilder::new(broker_url)
    }

    /// Publishes a message to the specified topic.
    ///
    /// # Arguments
    /// * `topic` - The name of the topic.
    /// * `message` - The message to be published.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    pub async fn publish<T: Clone + serde::Serialize + serde::de::DeserializeOwned>(
        &self,
        topic: &str,
        message: &T,
    ) -> Result<(), BroccoliError> {
        let message = BrokerMessage::new(message.clone());
        let serialized_message = serde_json::to_string(&message)
            .map_err(|e| BroccoliError::Publish(format!("Failed to serialize message: {:?}", e)))?;

        self.broker
            .publish(topic, &[serialized_message])
            .await
            .map_err(|e| BroccoliError::Publish(format!("Failed to publish message: {:?}", e)))?;

        Ok(())
    }

    /// Publishes a batch of messages to the specified topic.
    ///
    /// # Arguments
    /// * `topic` - The name of the topic.
    /// * `messages` - An iterator over the messages to be published.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    pub async fn publish_batch<T: Clone + serde::Serialize + serde::de::DeserializeOwned>(
        &self,
        topic: &str,
        messages: impl IntoIterator<Item = T>,
    ) -> Result<(), BroccoliError> {
        let messages: Vec<BrokerMessage<T>> =
            messages.into_iter().map(BrokerMessage::new).collect();

        let serialized_messages = messages
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<String>, _>>()
            .map_err(|e| {
                BroccoliError::Publish(format!("Failed to serialize messages: {:?}", e))
            })?;

        self.broker
            .publish(topic, &serialized_messages)
            .await
            .map_err(|e| BroccoliError::Publish(format!("Failed to publish messages: {:?}", e)))?;

        Ok(())
    }

    /// Consumes a message from the specified topic.
    ///
    /// # Arguments
    /// * `topic` - The name of the topic.
    ///
    /// # Returns
    /// A `Result` containing the consumed message, or a `BroccoliError` on failure.
    pub async fn consume<T: Clone + serde::Serialize + serde::de::DeserializeOwned>(
        &self,
        topic: &str,
    ) -> Result<BrokerMessage<T>, BroccoliError> {
        let serialized_message =
            self.broker.consume(topic).await.map_err(|e| {
                BroccoliError::Consume(format!("Failed to consume message: {:?}", e))
            })?;

        let message: BrokerMessage<T> = serde_json::from_str(&serialized_message).map_err(|e| {
            BroccoliError::Consume(format!("Failed to deserialize message: {:?}", e))
        })?;

        Ok(message)
    }

    /// Consumes a batch of messages from the specified topic.
    ///
    /// # Arguments
    /// * `topic` - The name of the topic.
    /// * `batch_size` - The number of messages to consume.
    /// * `timeout` - The timeout duration for consuming messages.
    ///
    /// # Returns
    /// A `Result` containing a vector of consumed messages, or a `BroccoliError` on failure.
    pub async fn consume_batch<T: Clone + serde::Serialize + serde::de::DeserializeOwned>(
        &self,
        topic: &str,
        batch_size: usize,
        timeout: Duration,
    ) -> Result<Vec<BrokerMessage<T>>, BroccoliError> {
        let mut messages = Vec::with_capacity(batch_size);
        let deadline = Instant::now() + timeout;

        while messages.len() < batch_size && Instant::now() < deadline {
            if let Ok(Some(msg)) = self.try_consume::<T>(topic).await {
                messages.push(msg);
            }
        }

        Ok(messages)
    }

    /// Attempts to consume a message from the specified topic.
    ///
    /// # Arguments
    /// * `topic` - The name of the topic.
    ///
    /// # Returns
    /// A `Result` containing an `Option` with the consumed message if available, or a `BroccoliError` on failure.
    pub async fn try_consume<T: Clone + serde::Serialize + serde::de::DeserializeOwned>(
        &self,
        topic: &str,
    ) -> Result<Option<BrokerMessage<T>>, BroccoliError> {
        let serialized_message =
            self.broker.try_consume(topic).await.map_err(|e| {
                BroccoliError::Consume(format!("Failed to consume message: {:?}", e))
            })?;

        if let Some(serialized_message) = serialized_message {
            let message: BrokerMessage<T> =
                serde_json::from_str(&serialized_message).map_err(|e| {
                    BroccoliError::Consume(format!("Failed to deserialize message: {:?}", e))
                })?;

            Ok(Some(message))
        } else {
            Ok(None)
        }
    }

    /// Acknowledges the processing of a message, removing it from the processing queue.
    ///
    /// # Arguments
    /// * `topic` - The name of the topic.
    /// * `message` - The message to be acknowledged.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    pub async fn acknowledge<T: Clone + serde::Serialize + serde::de::DeserializeOwned>(
        &self,
        topic: &str,
        message: BrokerMessage<T>,
    ) -> Result<(), BroccoliError> {
        let serialized_message = serde_json::to_string(&message).map_err(|e| {
            BroccoliError::Acknowledge(format!("Failed to serialize message: {:?}", e))
        })?;

        self.broker
            .acknowledge(topic, serialized_message)
            .await
            .map_err(|e| {
                BroccoliError::Acknowledge(format!("Failed to acknowledge message: {:?}", e))
            })?;

        Ok(())
    }

    /// Processes messages from the specified topic with the provided handler function.
    ///
    /// # Example
    ///
    /// ```rust
    /// use broccoli_queue::queue::BroccoliQueue;
    /// use broccoli_queue::brokers::broker::BrokerMessage;
    ///
    /// #[derive(Debug, Clone, Serialize, Deserialize)]
    /// struct JobPayload {
    ///    id: String,
    ///    task_name: String,
    ///    parameters: Parameters,
    ///    created_at: chrono::DateTime<chrono::Utc>,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///    let queue = BroccoliQueue::builder("redis://localhost:6379")
    ///       .failed_message_retry_strategy(Default::default())
    ///       .pool_connections(5)
    ///       .build()
    ///       .await
    ///       .unwrap();
    ///
    ///   queue.process_messages("jobs", None, |message: BrokerMessage<JobPayload>| async move {
    ///         println!("Received message: {:?}", message);
    ///         Ok(())
    ///     }).await.unwrap();
    ///
    /// }
    /// ```
    ///
    /// # Arguments
    /// * `topic` - The name of the topic.
    /// * `concurrency` - The number of concurrent message handlers.
    /// * `handler` - The handler function to process messages. This function should return a `BroccoliError` on failure.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    pub async fn process_messages<T, F, Fut>(
        &self,
        topic: &str,
        concurrency: Option<usize>,
        handler: F,
    ) -> Result<(), BroccoliError>
    where
        T: serde::de::DeserializeOwned + Send + Clone + serde::Serialize + 'static,
        F: Fn(BrokerMessage<T>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<(), BroccoliError>> + Send + 'static,
    {
        let handles = FuturesUnordered::new();

        loop {
            if let Some(concurrency) = concurrency {
                while handles.len() < concurrency {
                    let broker = Arc::clone(&self.broker);
                    let topic = topic.to_string();
                    let handler = handler.clone();

                    let handle = tokio::spawn(async move {
                        loop {
                            let serialized_message = broker
                                .consume(&topic)
                                .await
                                .map_err(|e| {
                                    log::error!("Failed to consume message: {:?}", e);
                                })
                                .unwrap();

                            let message: BrokerMessage<T> =
                                serde_json::from_str(&serialized_message)
                                    .map_err(|e| {
                                        log::error!("Failed to deserialize message: {:?}", e);
                                    })
                                    .unwrap();

                            match handler(message).await {
                                Ok(_) => {
                                    // Message processed successfully
                                    let _ = broker
                                        .acknowledge(&topic, serialized_message)
                                        .await
                                        .map_err(|e| {
                                            log::error!("Failed to acknowledge message: {:?}", e);
                                        });
                                }
                                Err(_) => {
                                    // Message processing failed
                                    let _ = broker
                                        .reject(&topic, serialized_message)
                                        .await
                                        .map_err(|e| {
                                            log::error!("Failed to reject message: {:?}", e);
                                        });
                                }
                            }
                        }
                    });

                    handles.push(handle);
                }
            } else {
                loop {
                    let serialized_message = self
                        .broker
                        .consume(topic)
                        .await
                        .map_err(|e| {
                            log::error!("Failed to consume message: {:?}", e);
                        })
                        .unwrap();

                    let message: BrokerMessage<T> = serde_json::from_str(&serialized_message)
                        .map_err(|e| {
                            log::error!("Failed to deserialize message: {:?}", e);
                        })
                        .unwrap();

                    match handler(message).await {
                        Ok(_) => {
                            // Message processed successfully
                            let _ = self
                                .broker
                                .acknowledge(topic, serialized_message)
                                .await
                                .map_err(|e| {
                                    log::error!("Failed to acknowledge message: {:?}", e);
                                });
                        }
                        Err(_) => {
                            // Message processing failed
                            let _ =
                                self.broker
                                    .reject(topic, serialized_message)
                                    .await
                                    .map_err(|e| {
                                        log::error!("Failed to reject message: {:?}", e);
                                    });
                        }
                    }
                }
            }
        }
    }

    /// Processes messages from the specified topic with the provided handler functions for message processing, success, and error handling.
    ///
    /// # Example
    ///
    /// ```rust
    /// use broccoli_queue::queue::BroccoliQueue;
    /// use broccoli_queue::brokers::broker::BrokerMessage;
    /// use broccoli_queue::error::BroccoliError;
    ///
    /// #[derive(Debug, Clone, Serialize, Deserialize)]
    /// struct JobPayload {
    ///     id: String,
    ///     task_name: String,
    ///     created_at: chrono::DateTime<chrono::Utc>,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let queue = BroccoliQueue::builder("redis://localhost:6379")
    ///         .failed_message_retry_strategy(Default::default())
    ///         .pool_connections(5)
    ///         .build()
    ///         .await
    ///         .unwrap();
    ///
    ///     // Define handlers
    ///     async fn process_message(message: BrokerMessage<JobPayload>) -> Result<(), BroccoliError> {
    ///         println!("Processing message: {:?}", message);
    ///         Ok(())
    ///     }
    ///
    ///     async fn on_success(message: BrokerMessage<JobPayload>) -> Result<(), BroccoliError> {
    ///         println!("Successfully processed message: {}", message.task_id);
    ///         Ok(())
    ///     }
    ///
    ///     async fn on_error(message: BrokerMessage<JobPayload>, error: BroccoliError) -> Result<(), BroccoliError> {
    ///         println!("Failed to process message {}: {:?}", message.task_id, error);
    ///         Ok(())
    ///     }
    ///
    ///     // Process messages with 3 concurrent workers
    ///     queue.process_messages_with_handlers(
    ///         "jobs",
    ///         3,
    ///         process_message,
    ///         on_success,
    ///         on_error
    ///     ).await.unwrap();
    /// }
    /// ```
    ///
    /// # Arguments
    /// * `topic` - The name of the topic.
    /// * `concurrency` - The number of concurrent message handlers.
    /// * `message_handler` - The handler function to process messages. This function should return a `BroccoliError` on failure.
    /// * `on_success` - The handler function to call on successful message processing. This function should return a `BroccoliError` on failure.
    /// * `on_error` - The handler function to call on message processing failure. This function should return a `BroccoliError` on failure.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    pub async fn process_messages_with_handlers<T, F, MessageFut, SuccessFut, ErrorFut, S, E>(
        &self,
        topic: &str,
        concurrency: usize,
        message_handler: F,
        on_success: S,
        on_error: E,
    ) -> Result<(), BroccoliError>
    where
        T: serde::de::DeserializeOwned + Send + Clone + serde::Serialize + 'static,
        F: Fn(BrokerMessage<T>) -> MessageFut + Send + Sync + Clone + 'static,
        MessageFut: Future<Output = Result<(), BroccoliError>> + Send + 'static,
        S: Fn(BrokerMessage<T>) -> SuccessFut + Send + Sync + Clone + 'static,
        SuccessFut: Future<Output = Result<(), BroccoliError>> + Send + 'static,
        E: Fn(BrokerMessage<T>, BroccoliError) -> ErrorFut + Send + Sync + Clone + 'static,
        ErrorFut: Future<Output = Result<(), BroccoliError>> + Send + 'static,
    {
        let handles = FuturesUnordered::new();

        loop {
            while handles.len() < concurrency {
                let broker = Arc::clone(&self.broker);
                let topic = topic.to_string();
                let message_handler = message_handler.clone();
                let on_success = on_success.clone();
                let on_error = on_error.clone();

                let handle = tokio::spawn(async move {
                    loop {
                        let serialized_message = broker
                            .consume(&topic)
                            .await
                            .map_err(|e| {
                                log::error!("Failed to consume message: {:?}", e);
                            })
                            .unwrap();

                        let message: BrokerMessage<T> = serde_json::from_str(&serialized_message)
                            .map_err(|e| {
                                log::error!("Failed to deserialize message: {:?}", e);
                            })
                            .unwrap();

                        match message_handler(message.clone()).await {
                            Ok(_) => {
                                let _ = on_success(message).await.map_err(|e| {
                                    log::error!("Success Handler to process message: {:?}", e)
                                });
                                let _ = broker
                                    .acknowledge(&topic, serialized_message)
                                    .await
                                    .map_err(|e| {
                                        log::error!("Failed to acknowledge message: {:?}", e)
                                    });
                            }
                            Err(e) => {
                                let _ = on_error(message, e).await.map_err(|e| {
                                    log::error!("Error Handler to process message: {:?}", e)
                                });
                                let _ = broker
                                    .reject(&topic, serialized_message)
                                    .await
                                    .map_err(|e| log::error!("Failed to reject message: {:?}", e));
                            }
                        }
                    }
                });

                handles.push(handle);
            }
        }
    }
}
