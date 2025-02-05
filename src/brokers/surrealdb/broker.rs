use crate::{
    brokers::broker::{Broker, BrokerConfig, InternalBrokerMessage},
    error::BroccoliError,
    queue::{ConsumeOptions, PublishOptions},
};

use surrealdb::{engine::any::Any, RecordId};
use surrealdb::{Notification, Surreal};
use time::Duration;

use super::utils;

/// SurrealDB state struct
pub struct SurrealDBBroker {
    pub(crate) db: Option<Surreal<Any>>,
    pub(crate) connected: bool,
    pub(crate) config: Option<BrokerConfig>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct InternalSurrealDBBrokerMessage {
    /// Actual record id in surrealDB (topicname,task_id)
    pub id: RecordId,
    /// Unique identifier for the message (external version, without table name)
    pub task_id: String,
    /// The actual message content stringified
    pub payload: String,
    /// Number of processing attempts made
    pub attempts: u8,
    /// Additional metadata for the message
    #[serde(skip)]
    pub(crate) metadata:
        Option<std::collections::HashMap<String, crate::brokers::broker::MetadataTypes>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct InternalSurrealDBBrokerQueuedMessage {
    pub(crate) message_id: RecordId,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct InternalSurrealDBBrokerProcessingMessage {
    pub(crate) message_id: RecordId,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct InternalSurrealDBBrokerFailedMessage {
    pub(crate) original_msg: InternalSurrealDBBrokerMessage,
}

/// Implementation of the `Broker` trait for `SurrealDBBroker`.
#[async_trait::async_trait]
impl Broker for SurrealDBBroker {
    /// Connects to the broker using the provided URL.
    ///
    /// # Arguments
    /// * `broker_url` - The URL of the broker, in URL format with params, namely:
    /// `<protocol>://<host[:port]/?ns=<namespace>&db=<database>...` with the following params
    /// *Mandatory*
    /// - username
    /// - password
    /// - ns
    /// - db
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    ///
    ///
    async fn connect(&mut self, broker_url: &str) -> Result<(), BroccoliError> {
        let db = SurrealDBBroker::client_from_url(broker_url).await?;
        self.db = db;
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
        queue_name: &'static str,
        messages: &[InternalBrokerMessage],
        publish_options: Option<PublishOptions>,
    ) -> Result<Vec<InternalBrokerMessage>, BroccoliError> {
        self.check_connected()?;

        let publish_options = publish_options.unwrap_or_default();
        let priority = publish_options.priority.unwrap_or(5) as i64;

        if !(1..=5).contains(&priority) {
            return Err(BroccoliError::Broker(
                "Priority must be between 1 and 5".to_string(),
            ));
        }
        let _priority_str = priority.to_string();
        // if we have a delay and scheduling is enabled
        let delay: Option<Duration> = self
            .config
            .as_ref()
            .map(|c| {
                c.enable_scheduling
                    .map_or_else(|| None, |_| publish_options.delay)
            })
            .unwrap();
        let scheduled_at: Option<time::OffsetDateTime> = self
            .config
            .as_ref()
            .map(|c| {
                c.enable_scheduling
                    .map_or_else(|| None, |_| publish_options.scheduled_at)
            })
            .unwrap();
        let mut published: Vec<InternalBrokerMessage> = Vec::new();
        for msg in messages {
            // TODO: look into priority

            let db = <std::option::Option<Surreal<surrealdb::engine::any::Any>> as Clone>::clone(
                &self.db,
            )
            .unwrap();
            // 1: insert actual message //
            let inserted =
                utils::add_message(&db, queue_name, &msg, "Could not publish (add msg)").await?;
            published.push(inserted);

            // 2: add to queue //
            if delay.is_none() && scheduled_at.is_none() {
                utils::add_to_queue(&db, queue_name, &msg.task_id, "Could not publish (enqueue)")
                    .await?;
            } else {
                // we either have a delay or a schedule, schedule takes priority
                if scheduled_at.is_some() {
                    utils::add_to_queue_scheduled(
                        &db,
                        queue_name,
                        &msg.task_id,
                        scheduled_at.unwrap(),
                        "Could not publish scheduled (enqueue)",
                    )
                    .await?
                } else {
                    utils::add_to_queue_delayed(
                        &db,
                        queue_name,
                        &msg.task_id,
                        delay.unwrap(),
                        "Could not publish delayed (enqueue)",
                    )
                    .await?
                }
            }
        }
        Ok(published)
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
        queue_name: &'static str,
        options: Option<ConsumeOptions>,
    ) -> Result<Option<InternalBrokerMessage>, BroccoliError> {
        self.check_connected()?;

        let db =
            <std::option::Option<Surreal<surrealdb::engine::any::Any>> as Clone>::clone(&self.db)
                .unwrap();

        //// 1: get message from queue ////
        // unfortunately cannot have a surrealdb session open haven an open transaction
        let queued_message =
            utils::get_queued(&db, queue_name, "Could not try consume (get queued)").await?;
        if queued_message.is_none() {
            return Ok(None);
        }
        let queued_message = queued_message.unwrap();

        //// 2: delete it from queue ////
        let queued_msg_id = queued_message.message_id;
        utils::remove_from_queue(
            &db,
            queue_name,
            queued_msg_id.clone(),
            "Could not try consume (removing from queue)",
        )
        .await?;

        //// 3: if not autoack then add it to processing ////
        let auto_ack = options.is_some_and(|x| x.auto_ack.unwrap_or(false));
        if !auto_ack {
            //// 4: add to processing queue ////
            utils::add_to_processing(
                &db,
                queue_name,
                queued_msg_id.clone(),
                "Could not try consume (add to processing)",
            )
            .await?;
        }

        //// 4: return the actual payload ////
        let msg = utils::get_message(
            &db,
            queued_msg_id,
            "Could not try consume (get actual message)",
        )
        .await;
        if msg.is_ok() {
            Ok(Some(msg.unwrap()))
        } else {
            Err(msg.err().unwrap())
        }
    }

    /// Consumes a message from the specified queue, blocking until a message is available.
    /// TODO: once enough time has passed that messages are relevant, we should awaken and return the message
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    ///
    /// # Returns
    /// A `Result` containing the message as a `String`, or a `BroccoliError` on failure.
    async fn consume(
        &self,
        queue_name: &'static str,
        options: Option<ConsumeOptions>,
    ) -> Result<InternalBrokerMessage, BroccoliError> {
        // first of all, we try to consume without blocking, and return if we have messages
        let resp = Self::try_consume(&self, queue_name, options).await?;
        if resp.is_some() {
            return Ok(resp.unwrap());
        }

        // if there were no messages, we block and wait
        self.check_connected()?;
        let db =
            <std::option::Option<Surreal<surrealdb::engine::any::Any>> as Clone>::clone(&self.db);

        let mut stream =
            db.unwrap()
                .select(queue_name)
                .live()
                .await
                .map_err(|err: surrealdb::Error| {
                    BroccoliError::Broker(format!("Could not consume: {:?}", err))
                })?;

        // let msg: Arc<Result<InternalBrokerMessage, BroccoliError>> = Arc::new();
        // let msg = Arc::new(Mutex::new(Err(BroccoliError::NotImplemented)));
        let mut msg: Result<InternalBrokerMessage, BroccoliError> =
            Err(BroccoliError::NotImplemented);
        // this should block in theory
        while let Some(notification) = futures::StreamExt::next(&mut stream).await {
            // we have a notification, we decide if it's relevant (create) and if it's not an element scheduled in the future
            let payload: Option<Result<InternalBrokerMessage, BroccoliError>> = match notification {
                Ok(notification) => {
                    let notification: Notification<InternalBrokerMessage> = notification;
                    let payload = notification.data;
                    match notification.action {
                        surrealdb::Action::Create => {
                            eprintln!("********** Created record **********");
                            Some(Ok(payload))
                        }
                        surrealdb::Action::Update => {
                            eprintln!("********** Updated record **********");
                            None
                        }
                        _ => None,
                    }
                }
                Err(error) => Some(Err(BroccoliError::Broker(format!(
                    "Could not consume: {:?}",
                    error
                )))),
            };
            if payload.is_some() {
                msg = payload.unwrap();
                break;
            }
        }
        msg
        // The returned stream implements `futures::Stream` so we can
        // use it with `futures::StreamExt`, for example.
        //while let Some(notification) = stream.next().await {}
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
        queue_name: &'static str,
        message: InternalBrokerMessage,
    ) -> Result<(), BroccoliError> {
        let db =
            <std::option::Option<Surreal<surrealdb::engine::any::Any>> as Clone>::clone(&self.db)
                .unwrap();

        // 1: remove from processing queue
        utils::remove_from_processing(
            &db,
            queue_name,
            &message.task_id,
            "Could not acknowledge (remove from processing)",
        )
        .await?;

        // 2: remove actual message
        utils::remove_message(
            &db,
            queue_name,
            &message.task_id,
            "Could not acknowledge (remove message)",
        )
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
        queue_name: &'static str,
        message: InternalBrokerMessage,
    ) -> Result<(), BroccoliError> {
        self.check_connected()?;

        let db =
            <std::option::Option<Surreal<surrealdb::engine::any::Any>> as Clone>::clone(&self.db)
                .unwrap();

        let attempts = message.attempts + 1;

        //// 1: remove from processing ////
        let rejected = utils::remove_from_processing(
            &db,
            queue_name,
            &message.task_id,
            "Could not reject (remove from processed)",
        )
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
            let msg =
                utils::get_message(&db, rejected.message_id, "Could not reject (get original)")
                    .await?;
            //// 2: add to failed if excceded all attempts ////
            utils::add_to_failed(&db, queue_name, msg, "Could not reject (adding to failed)")
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
            // TODO: handle priority here
            //// 3: if retry is configured, we increase attempts ////
            let mut message = message;
            message.attempts = attempts;
            let task_id = message.task_id.clone();
            utils::update_message(&db, queue_name, message, "Could not reject (attempts+1)")
                .await?;
            //// 4: and reenqueue ////
            utils::add_to_queue(&db, queue_name, &task_id, "Could not reject (reenqueue)").await?
        }

        Ok(())
    }

    /// Cancels a message, removing it from the processing queue.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue.
    /// * `message_id` - The ID of the message to be canceled.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    async fn cancel(
        &self,
        queue_name: &'static str,
        message_id: String,
    ) -> Result<(), BroccoliError> {
        self.check_connected()?;

        let db =
            <std::option::Option<Surreal<surrealdb::engine::any::Any>> as Clone>::clone(&self.db)
                .unwrap();

        //// 1: remove from queue ////
        utils::remove_from_queue_str(
            &db,
            queue_name,
            &message_id,
            "Could not cancel (remove from queue)",
        )
        .await?;

        //// 2: remove message  ////
        utils::remove_message(
            &db,
            queue_name,
            &message_id,
            "Could not cancel (remove actual message)",
        )
        .await
    }
}
