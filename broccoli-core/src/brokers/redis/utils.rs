use serde_json::Value;

use crate::brokers::broker::BrokerConfig;

use super::broker::RedisBroker;

impl RedisBroker {
    pub fn new() -> Self {
        RedisBroker {
            redis_pool: None,
            connected: false,
            config: None,
        }
    }

    pub fn new_with_config(config: BrokerConfig) -> Self {
        RedisBroker {
            redis_pool: None,
            connected: false,
            config: Some(config),
        }
    }

    pub fn extract_message_attempts(message: &str) -> u8 {
        let message: Value = serde_json::from_str(message).unwrap();

        message
            .get("attempts")
            .and_then(Value::as_u64)
            .map(|attempts| attempts as u8)
            .unwrap_or(0)
    }

    pub fn update_attempts(message: String, new_attempts: u8) -> String {
        let mut message: Value = serde_json::from_str(&message).unwrap();
        message["attempts"] = Value::from(new_attempts);
        message.to_string()
    }
}
