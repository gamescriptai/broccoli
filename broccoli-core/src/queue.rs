use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::{stream::FuturesUnordered, StreamExt};

use crate::{
    brokers::{
        broker::{Broker, BrokerConfig, BrokerMessage},
        connect::connect_to_broker,
    },
    error::BroccoliError,
};

pub struct BroccoliQueueBuilder {
    broker_url: String,
    retry_attempts: Option<u8>,
    retry_failed: Option<bool>,
    pool_connections: Option<u8>,
}

impl BroccoliQueueBuilder {
    pub fn new(broker_url: impl Into<String>) -> Self {
        Self {
            broker_url: broker_url.into(),
            retry_attempts: None,
            retry_failed: None,
            pool_connections: None,
        }
    }

    pub fn retry_attempts(mut self, attempts: u8) -> Self {
        self.retry_attempts = Some(attempts);
        self
    }

    pub fn retry_failed(mut self, retry: bool) -> Self {
        self.retry_failed = Some(retry);
        self
    }

    pub fn pool_connections(mut self, connections: u8) -> Self {
        self.pool_connections = Some(connections);
        self
    }

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

pub struct BroccoliQueue {
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
    pub fn builder(broker_url: impl Into<String>) -> BroccoliQueueBuilder {
        BroccoliQueueBuilder::new(broker_url)
    }

    pub async fn publish<T: Clone + serde::Serialize + serde::de::DeserializeOwned>(
        &self,
        topic: &str,
        message: &T,
    ) -> Result<(), BroccoliError> {
        let message = BrokerMessage::new(message.clone());
        let serialized_message = serde_json::to_string(&message)
            .map_err(|e| BroccoliError::Publish(format!("Failed to serialize message: {:?}", e)))?;

        self.broker
            .publish(topic, serialized_message)
            .await
            .map_err(|e| BroccoliError::Publish(format!("Failed to publish message: {:?}", e)))?;

        Ok(())
    }

    pub async fn publish_batch<T: Clone + serde::Serialize + serde::de::DeserializeOwned>(
        &self,
        topic: &str,
        messages: impl IntoIterator<Item = T>,
    ) -> Result<(), BroccoliError> {
        for msg in messages {
            self.publish(topic, &msg).await?;
        }
        Ok(())
    }

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

    pub async fn process_messages<T, F, Fut>(
        &self,
        topic: &str,
        concurrency: usize,
        mut handler: F,
    ) -> Result<(), BroccoliError>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Clone + 'static,
        F: FnMut(BrokerMessage<T>) -> Fut + Send + Sync + Copy + 'static,
        Fut: Future<Output = Result<(), BroccoliError>> + Send + 'static,
    {
        let mut handles = FuturesUnordered::new();

        loop {
            while handles.len() < concurrency {
                let broker = Arc::clone(&self.broker);
                let topic = topic.to_string();

                let handle = tokio::spawn(async move {
                    loop {
                        let serialized_message = broker.consume(&topic).await.map_err(|e| {
                            BroccoliError::Consume(format!("Failed to consume message: {:?}", e))
                        })?;

                        let message: BrokerMessage<T> = serde_json::from_str(&serialized_message)
                            .map_err(|e| {
                            BroccoliError::Consume(format!(
                                "Failed to deserialize message: {:?}",
                                e
                            ))
                        })?;

                        match handler(message).await {
                            Ok(_) => {
                                // Message processed successfully
                                broker.acknowledge(&topic, serialized_message).await?;
                            }
                            Err(e) => {
                                // Message processing failed
                                broker.reject(&topic, serialized_message).await?;
                                return Err(e);
                            }
                        }
                    }
                });

                handles.push(handle);
            }

            if let Some(result) = handles.next().await {
                result
                    .map_err(|e| {
                        BroccoliError::Consume(format!("Failed to process message: {:?}", e))
                    })?
                    .map_err(|e: BroccoliError| {
                        BroccoliError::Consume(format!("Failed to process message: {:?}", e))
                    })?;
            }
        }
    }
}
