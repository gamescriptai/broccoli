use std::{collections::HashMap, time::Duration};

use dashmap::DashMap;
use deadpool::managed::QueueMode;
use deadpool_lapin::{Config, Pool, PoolConfig, Runtime};
use futures::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicCancelOptions, BasicConsumeOptions, BasicGetOptions,
        BasicPublishOptions, BasicRejectOptions,
    },
    types::{AMQPValue, FieldTable},
    BasicProperties, Channel,
};

#[cfg(feature = "rabbitmq-delay")]
use time::OffsetDateTime;

use crate::{
    brokers::broker::{Broker, BrokerConfig, InternalBrokerMessage},
    error::BroccoliError,
    queue::{ConsumeOptions, PublishOptions},
};

pub(crate) type RabbitPool = Pool;

#[derive(Default)]
/// A message broker implementation for RabbitMQ.
pub struct RabbitMQBroker {
    pub(crate) pool: Option<RabbitPool>,
    pub(crate) consume_channels: DashMap<String, Channel>,
    pub(crate) connected: bool,
    pub(crate) config: Option<BrokerConfig>,
}

#[async_trait::async_trait]
impl Broker for RabbitMQBroker {
    async fn connect(&mut self, broker_url: &str) -> Result<(), BroccoliError> {
        let pool_config = PoolConfig {
            max_size: self
                .config
                .as_ref()
                .and_then(|c| c.pool_connections)
                .unwrap_or(10) as usize,
            timeouts: deadpool::managed::Timeouts {
                wait: Some(Duration::from_secs(2)),
                create: Some(Duration::from_secs(2)),
                recycle: Some(Duration::from_secs(2)),
            },
            queue_mode: QueueMode::Fifo,
        };

        let config = Config {
            url: Some(broker_url.to_string()),
            pool: Some(pool_config),
            ..Default::default()
        };

        let pool = config.create_pool(Some(Runtime::Tokio1)).map_err(|e| {
            BroccoliError::Broker(format!("Failed to create connection pool: {}", e))
        })?;

        let conn = pool.get().await.map_err(|e| {
            BroccoliError::Broker(format!("Failed to get connection from pool: {}", e))
        })?;

        let channel = conn
            .create_channel()
            .await
            .map_err(|e| BroccoliError::Broker(format!("Failed to create channel: {}", e)))?;

        self.setup_exchange(&channel, "broccoli").await?;

        self.pool = Some(pool);
        self.connected = true;
        Ok(())
    }

    async fn publish(
        &self,
        queue_name: &str,
        messages: &[InternalBrokerMessage],
        options: Option<PublishOptions>,
    ) -> Result<Vec<InternalBrokerMessage>, BroccoliError> {
        let pool = self.ensure_pool().await?;
        let conn = pool.get().await.map_err(|e| {
            BroccoliError::Broker(format!("Failed to get connection from pool: {}", e))
        })?;

        let channel = conn
            .create_channel()
            .await
            .map_err(|e| BroccoliError::Broker(format!("Failed to create channel: {}", e)))?;

        self.setup_queue(&channel, queue_name).await?;

        let mut published_messages = Vec::new();

        for message in messages {
            let mut table = FieldTable::default();

            let mut properties = BasicProperties::default()
                .with_message_id(message.task_id.clone().into())
                .with_delivery_mode(2); // persistent

            if let Some(ref opts) = options {
                if let Some(ttl) = opts.ttl {
                    properties = properties.with_expiration(ttl.whole_seconds().to_string().into());
                }

                #[cfg(feature = "rabbitmq-delay")]
                if let Some(delay) = opts.delay {
                    table.insert(
                        "x-delay".to_string().into(),
                        AMQPValue::LongLongInt(delay.whole_milliseconds() as i64),
                    );
                }

                #[cfg(feature = "rabbitmq-delay")]
                if let Some(schedule) = opts.scheduled_at {
                    table.insert(
                        "x-delay".to_string().into(),
                        AMQPValue::LongLongInt(
                            (schedule - OffsetDateTime::now_utc()).whole_milliseconds() as i64,
                        ),
                    );
                }
            }

            table.insert(
                "attempts".to_string().into(),
                AMQPValue::ShortShortUInt(message.attempts),
            );

            properties = properties.with_headers(table);

            channel
                .basic_publish(
                    "broccoli",
                    queue_name,
                    BasicPublishOptions {
                        mandatory: false,
                        immediate: false,
                    },
                    message.payload.as_bytes(),
                    properties,
                )
                .await
                .map_err(|e| BroccoliError::Publish(format!("Failed to publish message: {}", e)))?;

            published_messages.push(message.clone());
        }

        Ok(published_messages)
    }

    async fn try_consume(
        &self,
        queue_name: &str,
        options: Option<ConsumeOptions>,
    ) -> Result<Option<InternalBrokerMessage>, BroccoliError> {
        let pool = self.ensure_pool().await?;
        let conn = pool.get().await.map_err(|e| {
            BroccoliError::Broker(format!("Failed to get connection from pool: {}", e))
        })?;

        let channel = conn
            .create_channel()
            .await
            .map_err(|e| BroccoliError::Broker(format!("Failed to create channel: {}", e)))?;

        self.setup_queue(&channel, queue_name).await?;

        let auto_ack = options.is_some_and(|x| x.auto_ack.unwrap_or(false));

        if let Ok(Some(delivery)) = channel
            .basic_get(queue_name, BasicGetOptions { no_ack: auto_ack })
            .await
        {
            let task_id = delivery
                .properties
                .message_id()
                .as_ref()
                .ok_or_else(|| BroccoliError::Consume("Missing message ID".to_string()))?
                .to_string();

            let payload = String::from_utf8_lossy(&delivery.data).to_string();
            let attempts = delivery
                .properties
                .headers()
                .as_ref()
                .and_then(|h| h.inner().get("attempts"))
                .and_then(|v| v.as_short_short_uint())
                .unwrap_or(0);

            let mut metadata = HashMap::new();
            metadata.insert(
                "delivery_tag".to_string(),
                delivery.delivery_tag.to_string(),
            );

            if !auto_ack {
                self.consume_channels
                    .insert(delivery.delivery_tag.to_string(), channel);
            }

            Ok(Some(InternalBrokerMessage {
                task_id,
                payload,
                attempts,
                metadata: Some(metadata),
            }))
        } else {
            Ok(None)
        }
    }

    async fn consume(
        &self,
        queue_name: &str,
        options: Option<ConsumeOptions>,
    ) -> Result<InternalBrokerMessage, BroccoliError> {
        let pool = self.ensure_pool().await?;
        let conn = pool.get().await.map_err(|e| {
            BroccoliError::Broker(format!("Failed to get connection from pool: {}", e))
        })?;

        let channel = conn
            .create_channel()
            .await
            .map_err(|e| BroccoliError::Broker(format!("Failed to create channel: {}", e)))?;

        self.setup_queue(&channel, queue_name).await?;
        let auto_ack = options.is_some_and(|x| x.auto_ack.unwrap_or(false));

        let mut consumer = channel
            .basic_consume(
                queue_name,
                "",
                BasicConsumeOptions {
                    no_ack: auto_ack,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| BroccoliError::Consume(format!("Failed to create consumer: {}", e)))?;

        if let Some(delivery) = consumer.next().await {
            let delivery = delivery.map_err(|e| {
                BroccoliError::Consume(format!("Failed to receive delivery: {}", e))
            })?;

            let task_id = delivery
                .properties
                .message_id()
                .as_ref()
                .ok_or_else(|| BroccoliError::Consume("Missing message ID".to_string()))?
                .to_string();

            let payload = String::from_utf8_lossy(&delivery.data).to_string();
            let attempts = delivery
                .properties
                .headers()
                .as_ref()
                .and_then(|h| h.inner().get("attempts"))
                .and_then(|v| v.as_short_short_uint())
                .unwrap_or(0);

            let mut metadata = HashMap::new();
            metadata.insert(
                "delivery_tag".to_string(),
                delivery.delivery_tag.to_string(),
            );
            if !auto_ack {
                self.consume_channels.insert(task_id.clone(), channel);
            }

            Ok(InternalBrokerMessage {
                task_id,
                payload,
                attempts,
                metadata: Some(metadata),
            })
        } else {
            Err(BroccoliError::Consume("Consumer cancelled".to_string()))
        }
    }

    async fn acknowledge(
        &self,
        _queue_name: &str,
        message: InternalBrokerMessage,
    ) -> Result<(), BroccoliError> {
        let delivery_tag = message
            .metadata
            .as_ref()
            .and_then(|m| m.get("delivery_tag"))
            .ok_or_else(|| BroccoliError::Acknowledge("Missing delivery tag".to_string()))?
            .parse::<u64>()
            .map_err(|e| BroccoliError::Acknowledge(format!("Invalid delivery tag: {}", e)))?;

        let channel = {
            let channel = self
                .consume_channels
                .get(&message.task_id)
                .ok_or_else(|| BroccoliError::Acknowledge("Missing channel".to_string()))?
                .clone();
            self.consume_channels.remove(&message.task_id);
            channel
        };

        channel
            .basic_ack(delivery_tag, BasicAckOptions::default())
            .await
            .map_err(|e| {
                BroccoliError::Acknowledge(format!("Failed to acknowledge message: {}", e))
            })?;

        Ok(())
    }

    async fn reject(
        &self,
        queue_name: &str,
        message: InternalBrokerMessage,
    ) -> Result<(), BroccoliError> {
        let delivery_tag = message
            .metadata
            .as_ref()
            .and_then(|m| m.get("delivery_tag"))
            .ok_or_else(|| BroccoliError::Acknowledge("Missing delivery tag".to_string()))?
            .parse::<u64>()
            .map_err(|e| BroccoliError::Acknowledge(format!("Invalid delivery tag: {}", e)))?;

        let channel = {
            let channel = self
                .consume_channels
                .get(&message.task_id)
                .ok_or_else(|| BroccoliError::Acknowledge("Missing channel".to_string()))?
                .clone();
            self.consume_channels.remove(&message.task_id);
            channel
        };
        let mut message = message.clone();
        message.attempts += 1;
        dbg!(message.attempts);

        channel
            .basic_reject(delivery_tag, BasicRejectOptions::default())
            .await
            .map_err(|e| BroccoliError::Cancel(format!("Failed to cancel message: {}", e)))?;
        if message.attempts < 3 {
            self.publish(queue_name, &[message], None).await?;
        }

        Ok(())
    }

    async fn cancel(&self, queue_name: &str, message_id: String) -> Result<(), BroccoliError> {
        let pool = self.ensure_pool().await?;
        let conn = pool.get().await.map_err(|e| {
            BroccoliError::Broker(format!("Failed to get connection from pool: {}", e))
        })?;

        let channel = conn
            .create_channel()
            .await
            .map_err(|e| BroccoliError::Broker(format!("Failed to create channel: {}", e)))?;

        let mut consumer = channel
            .basic_consume(
                queue_name,
                "temp_consumer",
                BasicConsumeOptions {
                    no_ack: false,   // Important: manual ack needed
                    exclusive: true, // Prevent other consumers while we search
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| BroccoliError::Consume(format!("Failed to create consumer: {}", e)))?;

        while let Some(delivery) = consumer.next().await {
            match delivery {
                Ok(delivery) => {
                    if let Some(rabbit_message_id) = delivery.properties.message_id() {
                        if rabbit_message_id.to_string() == message_id {
                            // Found our message, acknowledge it to remove it
                            channel
                                .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                                .await
                                .map_err(|e| {
                                    BroccoliError::Consume(format!("Failed to ack message: {}", e))
                                })?;

                            // Cancel the consumer since we're done
                            channel
                                .basic_cancel("temp_consumer", BasicCancelOptions::default())
                                .await
                                .map_err(|e| {
                                    BroccoliError::Consume(format!(
                                        "Failed to cancel consumer: {}",
                                        e
                                    ))
                                })?;

                            return Ok(());
                        } else {
                            // Not our message, put it back in the queue
                            channel
                                .basic_reject(
                                    delivery.delivery_tag,
                                    BasicRejectOptions { requeue: true },
                                )
                                .await
                                .map_err(|e| {
                                    BroccoliError::Consume(format!(
                                        "Failed to reject message: {}",
                                        e
                                    ))
                                })?;
                        }
                    }
                }
                Err(e) => {
                    return Err(BroccoliError::Consume(format!("Consumer error: {}", e)));
                }
            }
        }

        Ok(()) // Message not found
    }

    async fn get_message_position(
        &self,
        _queue_name: &str,
        _message_id: String,
    ) -> Result<Option<usize>, BroccoliError> {
        Err(BroccoliError::NotImplemented)
    }
}
