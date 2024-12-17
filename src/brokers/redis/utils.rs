//! Utility functions for Redis broker implementation.
//!
//! This module provides helper methods for managing Redis message operations,
//! including message parsing and manipulation of message metadata.

use super::broker::RedisBroker;
use crate::brokers::broker::BrokerConfig;
use serde_json::Value;

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

    pub(crate) fn extract_message_attempts(message: &str) -> u8 {
        let message: Value = serde_json::from_str(message).unwrap();

        message
            .get("attempts")
            .and_then(Value::as_u64)
            .map(|attempts| attempts as u8)
            .unwrap_or(0)
    }

    pub(crate) fn extract_task_id(message: &str) -> String {
        let message: Value = serde_json::from_str(message).unwrap();

        message
            .get("task_id")
            .and_then(Value::as_str)
            .map(String::from)
            .unwrap_or_else(|| String::from(""))
    }

    pub(crate) fn update_attempts(message: String, new_attempts: u8) -> String {
        let mut message: Value = serde_json::from_str(&message).unwrap();
        message["attempts"] = Value::from(new_attempts);
        message.to_string()
    }
}
