use crate::{brokers::broker::BrokerConfig, error::BroccoliError};

use super::broker::{get_redis_connection, RedisBroker};

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

    pub async fn remove_from_processing<T: for<'a> serde::Serialize>(
        &self,
        queue_name: &str,
        message: T,
    ) -> Result<(), BroccoliError> {
        if let Some(redis_pool) = &self.redis_pool {
            let mut redis_connection = get_redis_connection(redis_pool).await?;
            let serialized_message = rmp_serde::to_vec(&message).map_err(|e| {
                BroccoliError::PublishError(format!("Failed to serialize message: {:?}", e))
            })?;

            let _ = redis::cmd("LREM")
                .arg(format!("{}_processing", queue_name))
                .arg(1)
                .arg(serialized_message)
                .query_async::<String>(&mut *redis_connection)
                .await;
        }
        Ok(())
    }
}
