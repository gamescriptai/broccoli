//! Utility functions for Redis broker implementation.
//!
//! This module provides helper methods for managing Redis message operations,
//! including message parsing and manipulation of message metadata.

use std::collections::HashMap;

use super::broker::{RedisBroker, RedisConnection, RedisPool};
use crate::{
    brokers::broker::{BrokerConfig, InternalBrokerMessage, MetadataTypes},
    error::BroccoliError,
    queue::ConsumeOptions,
};
use redis::{AsyncCommands, FromRedisValue};

impl RedisBroker {
    /// Creates a new `RedisBroker` instance with default configuration.
    pub fn new() -> Self {
        RedisBroker {
            redis_pool: None,
            connected: false,
            config: None,
        }
    }

    /// Creates a new `RedisBroker` instance with the specified configuration.
    ///
    /// # Arguments
    /// * `config` - The broker configuration to use
    pub fn new_with_config(config: BrokerConfig) -> Self {
        RedisBroker {
            redis_pool: None,
            connected: false,
            config: Some(config),
        }
    }

    pub(crate) fn ensure_pool(&self) -> Result<RedisPool, BroccoliError> {
        if !self.connected {
            return Err(BroccoliError::Broker(
                "Redis broker not connected".to_string(),
            ));
        }
        match &self.redis_pool {
            Some(pool) => Ok(pool.clone()),
            None => Err(BroccoliError::Broker(
                "Redis pool not initialized".to_string(),
            )),
        }
    }

    pub(crate) async fn get_task_id(
        &self,
        queue_name: &str,
        redis_connection: &mut RedisConnection<'_>,
        options: Option<ConsumeOptions>,
    ) -> Result<Option<String>, BroccoliError> {
        let popped_message: Option<(String, f64)> = redis_connection
            .zpopmin::<&str, Vec<(String, f64)>>(queue_name, 1)
            .await?
            .first()
            .cloned();

        if let Some((message, score)) = &popped_message {
            if score > &(5.0 * time::OffsetDateTime::now_utc().unix_timestamp_nanos() as f64) {
                redis_connection
                    .zadd::<&str, f64, String, ()>(queue_name, message.clone(), *score)
                    .await?;
                return Ok(None);
            }
        }

        if options.is_some_and(|x| x.auto_ack.unwrap_or(false)) {
            Ok(popped_message.map(|(popped_message, _)| popped_message))
        } else if let Some((popped_message, _)) = popped_message {
            redis_connection
                .lpush::<String, &String, ()>(format!("{}_processing", queue_name), &popped_message)
                .await?;
            Ok(Some(popped_message))
        } else {
            Ok(None)
        }
    }
}

pub struct OptionalInternalBrokerMessage(pub Option<InternalBrokerMessage>);

impl FromRedisValue for OptionalInternalBrokerMessage {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let map: std::collections::HashMap<String, String> = redis::from_redis_value(v)?;
        if map.is_empty() {
            return Ok(OptionalInternalBrokerMessage(None));
        }

        let task_id = map.get("task_id").ok_or_else(|| {
            redis::RedisError::from((redis::ErrorKind::TypeError, "Missing field: task_id"))
        })?;

        let payload = map.get("payload").ok_or_else(|| {
            redis::RedisError::from((redis::ErrorKind::TypeError, "Missing field: payload"))
        })?;

        let attempts = map.get("attempts").ok_or_else(|| {
            redis::RedisError::from((redis::ErrorKind::TypeError, "Missing field: attempts"))
        })?;

        let priority = map.get("priority").ok_or_else(|| {
            redis::RedisError::from((redis::ErrorKind::TypeError, "Missing field: priority"))
        })?;

        let mut metadata = HashMap::new();
        metadata.insert(
            "priority".to_string(),
            MetadataTypes::String(priority.to_string()),
        );

        Ok(OptionalInternalBrokerMessage(Some(InternalBrokerMessage {
            task_id: task_id.to_string(),
            payload: payload.to_string(),
            attempts: attempts.parse().unwrap_or_default(),
            metadata: Some(metadata),
        })))
    }
}
