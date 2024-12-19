//! Utility functions for Redis broker implementation.
//!
//! This module provides helper methods for managing Redis message operations,
//! including message parsing and manipulation of message metadata.

use super::broker::{RedisBroker, RedisPool};
use crate::{
    brokers::broker::{BrokerConfig, InternalBrokerMessage},
    error::BroccoliError,
};
use redis::FromRedisValue;

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
        match &self.redis_pool {
            Some(pool) => Ok(pool.clone()),
            None => Err(BroccoliError::Broker(
                "Redis pool not initialized".to_string(),
            )),
        }
    }
}

impl FromRedisValue for InternalBrokerMessage {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let map: std::collections::HashMap<String, String> = redis::from_redis_value(v)?;

        let task_id = map.get("task_id").ok_or_else(|| {
            redis::RedisError::from((redis::ErrorKind::TypeError, "Missing field: task_id"))
        })?;

        let payload = map.get("payload").ok_or_else(|| {
            redis::RedisError::from((redis::ErrorKind::TypeError, "Missing field: payload"))
        })?;

        let attempts = map.get("attempts").ok_or_else(|| {
            redis::RedisError::from((redis::ErrorKind::TypeError, "Missing field: attempts"))
        })?;

        Ok(InternalBrokerMessage {
            task_id: task_id.to_string(),
            payload: payload.to_string(),
            attempts: attempts.parse().unwrap(),
        })
    }
}
