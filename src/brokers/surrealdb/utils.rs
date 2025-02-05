use std::str::FromStr;
use surrealdb::engine::any::connect;
use surrealdb::engine::any::Any;
use surrealdb::RecordId;
use surrealdb::Surreal;
use time::format_description::well_known::Rfc3339;
use time::Duration;
use time::OffsetDateTime;
use url::Url;

use crate::brokers::broker::BrokerConfig;
use crate::brokers::broker::InternalBrokerMessage;
use crate::error::BroccoliError;

use super::broker::InternalSurrealDBBrokerFailedMessage;
use super::broker::InternalSurrealDBBrokerMessage;
use super::broker::InternalSurrealDBBrokerProcessingMessage;
use super::broker::InternalSurrealDBBrokerQueuedMessage;
use super::SurrealDBBroker;

#[derive(Default)]
struct SurrealDBConnectionConfig {
    username: String,
    password: String,
    ns: String,
    database: String,
}

impl SurrealDBBroker {
    /// Creates a new `SurrealDBBroker` instance with default configuration.
    pub fn new() -> Self {
        SurrealDBBroker {
            db: None,
            connected: false,
            config: None,
        }
    }

    pub(crate) fn new_with_config(config: BrokerConfig) -> Self {
        Self {
            db: None,
            connected: false,
            config: Some(config),
        }
    }

    pub(crate) fn check_connected(&self) -> Result<(), BroccoliError> {
        if self.db.is_none() {
            return Err(BroccoliError::Broker("Not connected".to_string()));
        }
        Ok(())
    }

    /// we create a surreadlb connection from the url configuration
    pub async fn client_from_url(
        broker_url: &str,
    ) -> Result<std::option::Option<Surreal<Any>>, BroccoliError> {
        let url = Url::parse(broker_url).map_err(|e| {
            BroccoliError::Broker(format!("Failed to parse connection URL: {:?}", e))
        })?;
        let config = SurrealDBConnectionConfig {
            username: Self::get_param_value(&url, "username")?,
            password: Self::get_param_value(&url, "password")?,
            ns: Self::get_param_value(&url, "ns")?,
            database: Self::get_param_value(&url, "database")?,
        };
        if !url.has_host() || !url.has_host() {
            return Err(BroccoliError::Broker(
                "Failed to coonect to SurrealDB: Missing scheme:://host".to_string(),
            ));
        }
        let scheme = url.scheme();
        if scheme != "ws" {
            return Err(BroccoliError::Broker(
                "Failed to connect to SurrealDB: only ws:// is supported".to_string(),
            ));
        }
        let port = url.port();
        if port.is_none() {
            return Err(BroccoliError::Broker(
                "Failed to connect to SurrealDB: missing port number".to_string(),
            ));
        }
        let connection_url = format!("ws://{}:{}/rpc", url.host_str().unwrap(), port.unwrap());
        let db = connect(connection_url).await.map_err(|e| {
            BroccoliError::Broker(format!("Failed to connect to SurrealDB: {:?}", e))
        })?;

        db.signin(surrealdb::opt::auth::Root {
            username: &config.username,
            password: &config.password,
        })
        .await
        .map_err(|e| {
            BroccoliError::Broker(format!("Incorrect credentials for SurrealDB: {:?}", e))
        })?;

        // Select a specific namespace / database
        db.use_ns(config.ns)
            .use_db(config.database)
            .await
            .map_err(|e| {
                BroccoliError::Broker(format!("NS/DB not found for SurrealDB: {:?}", e))
            })?;

        Ok(Some(db))
    }

    pub(crate) fn get_param_value(url: &Url, name: &str) -> Result<String, BroccoliError> {
        url.query_pairs()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.into_owned().to_string())
            .map_or_else(
                || {
                    Err(BroccoliError::Broker(format!(
                        "Missing connection param: {}",
                        name
                    )))
                },
                |v| Ok(v),
            )
    }
}

impl Into<InternalBrokerMessage> for InternalSurrealDBBrokerMessage {
    fn into(self) -> InternalBrokerMessage {
        InternalBrokerMessage {
            task_id: self.task_id,
            payload: self.payload,
            attempts: self.attempts,
            metadata: self.metadata,
        }
    }
}

impl InternalSurrealDBBrokerMessage {
    fn from(queue_name: &'static str, msg: InternalBrokerMessage) -> Self {
        let id: RecordId = (queue_name, msg.task_id.clone()).into();
        InternalSurrealDBBrokerMessage {
            id,
            task_id: msg.task_id,
            payload: msg.payload,
            attempts: msg.attempts,
            metadata: msg.metadata,
        }
    }
}

fn queue_table(queue_name: &'static str) -> String {
    format!("{}___queue", queue_name)
}

fn processing_table(queue_name: &'static str) -> String {
    format!("{}___processing", queue_name)
}

fn failed_table(queue_name: &'static str) -> String {
    format!("{}___failed", queue_name)
}

pub(crate) async fn add_to_queue(
    db: &Surreal<Any>,
    queue_name: &'static str,
    message_id: &String,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let message_id: RecordId = (queue_name, message_id).into();
    // TODO: try to pass a vector of Value, Value instead of parsing strings which is brittle
    let now = "time::now()".to_string();
    let _ = self::add_record_to_queue(db, queue_name, message_id, now, err_msg).await?;
    Ok(())
}

pub(crate) async fn add_to_queue_delayed(
    db: &Surreal<Any>,
    queue_name: &'static str,
    message_id: &String,
    delay: Duration,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    // TODO: try to pass a vector of Value, Value instead of parsing a string
    let when = OffsetDateTime::now_utc() + delay;
    let _ = self::add_to_queue_scheduled(&db, queue_name, message_id, when, err_msg).await?;
    Ok(())
}

pub(crate) async fn add_to_queue_scheduled(
    db: &Surreal<Any>,
    queue_name: &'static str,
    message_id: &String,
    when: OffsetDateTime,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let _ = self::add_to_queue_impl(&db, queue_name, message_id, when, err_msg).await?;
    Ok(())
}

async fn add_to_queue_impl(
    db: &Surreal<Any>,
    queue_name: &'static str,
    message_id: &String,
    when: OffsetDateTime,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let message_id: RecordId = (queue_name, message_id).into();
    let time = to_rfc3339(when);
    if time.is_err() {
        return Err(BroccoliError::Broker(format!(
            "{}: could not convert delay",
            err_msg,
        )));
    }
    // this explicit casting will make the query fail if the datetime is not correct
    let when = format!("<datetime>\"{}\"", time.unwrap());
    let _ = self::add_record_to_queue(db, queue_name, message_id, when, err_msg).await?;
    Ok(())
}

async fn add_record_to_queue(
    db: &Surreal<Any>,
    queue_name: &'static str,
    message_id: RecordId,
    when: String,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let queue_table = self::queue_table(queue_name);
    let queue_record_str = format!("{}:[{},`{}`]", queue_table, when, message_id);
    let queue_record_id = RecordId::from_str(&queue_record_str).unwrap();
    let qm: Option<InternalSurrealDBBrokerQueuedMessage> = db
        .create(queue_record_id)
        .content(InternalSurrealDBBrokerQueuedMessage { message_id })
        .await
        .map_err(|err: surrealdb::Error| {
            BroccoliError::Broker(format!("{}: {:?}", err_msg, err))
        })?;
    if qm.is_none() {
        return Err(BroccoliError::Broker(format!(
            "{}: could not add to queue (silent)",
            err_msg,
        )));
    }
    Ok(())
}

// get first queued message if any
pub(crate) async fn get_queued(
    db: &Surreal<Any>,
    queue_name: &'static str,
    err_msg: &'static str,
) -> Result<Option<InternalSurrealDBBrokerQueuedMessage>, BroccoliError> {
    let queue_table = self::queue_table(queue_name);
    let q = "SELECT * FROM ONLY type::table($queue_table) WHERE id[0]<time::now() LIMIT 1";
    let mut resp = db
        .query(q)
        .bind(("queue_table", queue_table.clone())) // ordered time series
        .await
        .map_err(|err: surrealdb::Error| {
            BroccoliError::Broker(format!("{}: {:?}", err_msg, err))
        })?;
    let resp: Option<InternalSurrealDBBrokerQueuedMessage> = resp
        .take(0)
        .map_err(|e| BroccoliError::Broker(format!("{}: {},{}", err_msg, e, queue_name)))?;
    if resp.is_none() {
        return Ok(None); // nothing was queued
    }
    let queued_message = resp.unwrap();
    Ok(Some(queued_message))
}

pub(crate) async fn remove_from_queue_str(
    db: &Surreal<Any>,
    queue_name: &'static str,
    message_id: &String,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let message_id: RecordId = (queue_name, message_id).into();
    let _ = self::remove_from_queue(&db, queue_name, message_id, err_msg).await?;
    Ok(())
}

// remove from ordered queue
pub(crate) async fn remove_from_queue(
    db: &Surreal<Any>,
    queue_name: &'static str,
    message_id: RecordId,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let queue_table = self::queue_table(queue_name);
    let q = "DELETE type::table($queue_table) WHERE message_id=$message_id RETURN BEFORE";
    let mut resp = db
        .query(q)
        .bind(("queue_table", queue_table))
        .bind(("message_id", message_id))
        .await
        .map_err(|err| BroccoliError::Broker(format!("{}: {:?}", err_msg, err)))?;
    let deleted: Option<InternalSurrealDBBrokerQueuedMessage> = resp
        .take(0)
        .map_err(|e| BroccoliError::Broker(format!("{}: (removing from queue) {}", err_msg, e)))?;
    if deleted.is_none() {
        return Err(BroccoliError::Broker(format!(
            "{}: (silently did not remove from queue)",
            err_msg
        )));
    }
    Ok(())
}

pub(crate) async fn add_message(
    db: &Surreal<Any>,
    queue_name: &'static str,
    msg: &InternalBrokerMessage,
    err_msg: &'static str,
) -> Result<InternalBrokerMessage, BroccoliError> {
    let added: Option<InternalBrokerMessage> = db
        .create((queue_name, &msg.task_id))
        .content(InternalSurrealDBBrokerMessage::from(
            queue_name,
            msg.to_owned(),
        ))
        .await
        .map_err(|err: surrealdb::Error| {
            BroccoliError::Broker(format!("{}: {:?}", err_msg, err))
        })?;
    if added.is_none() {
        return Err(BroccoliError::Broker(format!(
            "{}: could not add message (silent)",
            err_msg,
        )));
    }
    let added: InternalBrokerMessage = added.unwrap();
    Ok(added)
}

pub(crate) async fn update_message(
    db: &Surreal<Any>,
    queue_name: &'static str,
    msg: InternalBrokerMessage,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let updated: Option<InternalSurrealDBBrokerMessage> = db
        .update((queue_name, &msg.task_id))
        .content(InternalSurrealDBBrokerMessage::from(queue_name, msg))
        .await
        .map_err(|err: surrealdb::Error| {
            BroccoliError::Broker(format!("{}: {:?}", err_msg, err))
        })?;
    if updated.is_none() {
        return Err(BroccoliError::Broker(format!(
            "{}: could not update message (silent)",
            err_msg,
        )));
    }
    Ok(())
}

pub(crate) async fn get_message(
    db: &Surreal<Any>,
    message_id: RecordId,
    err_msg: &'static str,
) -> Result<InternalBrokerMessage, BroccoliError> {
    let msg: Option<InternalSurrealDBBrokerMessage> = db
        .select(message_id)
        .await
        .map_err(|err| BroccoliError::Broker(format!("{}: {:?}", err_msg, err)))?;
    if msg.is_some() {
        Ok(msg.unwrap().into())
    } else {
        Err(BroccoliError::Broker(format!(
            "{}: could not get message (silent)",
            err_msg
        )))
    }
}

// remove actual message (the one with the payload)
pub(crate) async fn remove_message(
    db: &Surreal<Any>,
    queue_name: &'static str,
    message_id: &String,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let msg_id: RecordId = (queue_name, message_id).into();
    let q = "DELETE type::table($queue_name) WHERE id=$msg_id RETURN BEFORE";
    let mut resp = db
        .query(q)
        .bind(("queue_name", queue_name))
        .bind(("msg_id", msg_id))
        .await
        .map_err(|err| BroccoliError::Broker(format!("{}: {}", err_msg, err)))?;
    let deleted: Option<InternalSurrealDBBrokerMessage> = resp
        .take(0)
        .map_err(|e| BroccoliError::Broker(format!("{}: (removing message) {}", err_msg, e)))?;
    if deleted.is_none() {
        return Err(BroccoliError::Broker(format!(
            "{}: (silently did not remove message)",
            err_msg
        )));
    }

    Ok(())
}

pub(crate) async fn add_to_processing(
    db: &Surreal<Any>,
    queue_name: &'static str,
    message_id: RecordId,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let processing_table = self::processing_table(queue_name);
    let _: Option<InternalSurrealDBBrokerProcessingMessage> = db
        .create(processing_table)
        .content(InternalSurrealDBBrokerProcessingMessage { message_id })
        .await
        .map_err(|err| BroccoliError::Broker(format!("{}: {:?}", err_msg, err)))?;
    Ok(())
}

pub(crate) async fn remove_from_processing(
    db: &Surreal<Any>,
    queue_name: &'static str,
    message_id: &String,
    err_msg: &'static str,
) -> Result<InternalSurrealDBBrokerProcessingMessage, BroccoliError> {
    let message_id: RecordId = (queue_name, message_id).into();
    let processing_table = self::processing_table(queue_name);
    let q = "DELETE type::table($processing_table) WHERE message_id=$message_id RETURN BEFORE";
    let mut resp = db
        .query(q)
        .bind(("processing_table", processing_table))
        .bind(("message_id", message_id))
        .await
        .map_err(|err| BroccoliError::Broker(format!("{}: {}", err_msg, err)))?;
    let processed: Option<InternalSurrealDBBrokerProcessingMessage> =
        resp.take(0).map_err(|e| {
            BroccoliError::Broker(format!("{}: (removing from processing) {}", err_msg, e))
        })?;
    if processed.is_none() {
        return Err(BroccoliError::Broker(format!(
            "{}: (silently did not remove from processing)",
            err_msg
        )));
    }
    Ok(processed.unwrap())
}

pub(crate) async fn add_to_failed(
    db: &Surreal<Any>,
    queue_name: &'static str,
    msg: InternalBrokerMessage,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let failed_table = self::failed_table(queue_name);
    let _: Option<InternalSurrealDBBrokerFailedMessage> = db
        .create((failed_table, msg.task_id.clone()))
        .content(InternalSurrealDBBrokerFailedMessage {
            original_msg: InternalSurrealDBBrokerMessage::from(queue_name, msg),
        })
        .await
        .map_err(|err| BroccoliError::Broker(format!("{}: {:?}", err_msg, err)))?;
    Ok(())
}

fn to_rfc3339<T>(dt: T) -> Result<std::string::String, time::error::Format>
where
    T: Into<OffsetDateTime>,
{
    dt.into().format(&Rfc3339)
}

#[cfg(test)]
#[tokio::test]
async fn new_with_url_test() {
    let url = Url::parse("ws://127.0.0.1/?username=user&password=passwd&ns=namespace&dtabase=db")
        .unwrap();
    let username = SurrealDBBroker::get_param_value(&url, "username").unwrap();
    assert_eq!("user", username);
    let password = SurrealDBBroker::get_param_value(&url, "password").unwrap();
    assert_eq!("passwd", password);
    let err = SurrealDBBroker::get_param_value(&url, "foo");
    assert!(err.is_err());
}
