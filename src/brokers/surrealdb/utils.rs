use std::str::FromStr;

use surrealdb::engine::any::connect;
use surrealdb::engine::any::Any;
use surrealdb::types::{SurrealValue, RecordId};
use surrealdb::Surreal;
use time::Duration;
use url::Url;

use crate::brokers::broker::BrokerConfig;
use crate::brokers::broker::InternalBrokerMessage;
use crate::error::BroccoliError;

use super::broker::InternalSurrealDBBrokerFailedMessage;
use super::broker::InternalSurrealDBBrokerMessage;
use super::broker::InternalSurrealDBBrokerMessageEntry;
use super::SurrealDBBroker;

#[derive(Default)]
struct SurrealDBConnectionConfig {
    username: String,
    password: String,
    ns: String,
    database: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, SurrealValue)]
struct InternalSurrealDBBrokerQueueIndex {
    pub queue_id: RecordId, // points to queue:[priority,timestamp,messageid]
    pub timestamp: surrealdb::types::Datetime,
}

impl Default for SurrealDBBroker {
    fn default() -> Self {
        Self::new()
    }
}

impl SurrealDBBroker {
    /// Creates a new `SurrealDBBroker` instance with default configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            db: None,
            connected: false,
            config: None,
        }
    }

    /// new with passed configuration
    #[must_use]
    pub(crate) const fn new_with_config(config: BrokerConfig) -> Self {
        Self {
            db: None,
            connected: false,
            config: Some(config),
        }
    }

    /// check and return current active connection
    pub(crate) fn check_connected(&self) -> Result<Surreal<Any>, BroccoliError> {
        self.db.as_ref().map_or_else(
            || Err(BroccoliError::Broker("Not connected".to_string())),
            |db| Ok(db.to_owned()),
        )
    }

    /// we create a surreadlb connection from the url configuration
    /// URL parameters after ? are username, password, ns, database
    /// if unspecified, will default back to root,root,test,test (only for testing!)
    /// note that only 'ws' is supported RTN to allow for live querying
    ///
    /// # Arguments
    /// * `broker_url` - The URL of the broker.
    ///
    /// # Returns
    /// A `Result` containing a boxed broker implementation, or a `BroccoliError` on failure.
    ///
    /// # Errors
    /// * `BroccoliError::Broker` - If the connection URL is invalid.
    pub async fn client_from_url(
        broker_url: &str,
    ) -> Result<std::option::Option<Surreal<Any>>, BroccoliError> {
        client_from_url(broker_url).await
    }

}

/// helper: public so we can call it from testing
pub async fn client_from_url(
        broker_url: &str,
) -> Result<std::option::Option<Surreal<Any>>, BroccoliError> {
    let url = Url::parse(broker_url)
        .map_err(|e| BroccoliError::Broker(format!("Failed to parse connection URL: {e:?}")))?;
    let config = SurrealDBConnectionConfig {
        username: get_param_value(&url, "username")
            .unwrap_or_else(|_| "root".to_string()),
        password: get_param_value(&url, "password")
            .unwrap_or_else(|_| "root".to_string()),
        ns: get_param_value(&url, "ns").unwrap_or_else(|_| "test".to_string()),
        database: get_param_value(&url, "database")
            .unwrap_or_else(|_| "test".to_string()),
    };

    let scheme = url.scheme();
    if scheme == "ws" && !url.has_host() {
        return Err(BroccoliError::Broker(
            "Failed to coonect to SurrealDB: Missing ws://host or mem://".to_string(),
        ));
    }
    if scheme != "ws" && scheme != "mem" {
        return Err(BroccoliError::Broker(
            "Failed to connect to SurrealDB: only ws:// or mem:// are supported".to_string(),
        ));
    }
    let port = url.port();
    if scheme == "ws" && port.is_none() {
        return Err(BroccoliError::Broker(
            "Failed to connect to SurrealDB: missing port number".to_string(),
        ));
    }
    let connection_url = if scheme == "ws" {
        format!(
            "ws://{}:{}/rpc",
            url.host_str().unwrap_or("localhost"),
            port.unwrap_or(8000)
        )
    } else {
        "mem://".to_string()
    };

    let db = connect(connection_url)
        .await
        .map_err(|e| BroccoliError::Broker(format!("Failed to connect to SurrealDB: {e:?}")))?;

    if scheme == "ws" {
        // credentials not relevant for mem://
        db.signin(surrealdb::opt::auth::Root {
            username: config.username,
            password: config.password,
        })
        .await
        .map_err(|e| {
            BroccoliError::Broker(format!("Incorrect credentials for SurrealDB: {e:?}"))
        })?;
    }
    // Select a specific namespace / database
    db.use_ns(config.ns)
        .use_db(config.database)
        .await
        .map_err(|e| BroccoliError::Broker(format!("NS/DB not found for SurrealDB: {e:?}")))?;
    log::info!("fully connected.");

    Ok(Some(db))
}

/// helper: given a url get a named parameter
pub(crate) fn get_param_value(url: &Url, name: &str) -> Result<String, BroccoliError> {
    url.query_pairs()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.into_owned())
        .map_or_else(
            || {
                Err(BroccoliError::Broker(format!(
                    "Missing connection param: {name}"
                )))
            },
            Ok,
        )
}


// convenience into and from conversion between the broccoli and the surrealdb layer
impl From<InternalSurrealDBBrokerMessage> for InternalBrokerMessage {
    fn from(val: InternalSurrealDBBrokerMessage) -> Self {
        Self {
            task_id: val.task_id.to_raw(), // converts without prefixes or quotations
            payload: val.payload,
            attempts: val.attempts,
            disambiguator: None,
            metadata: val.metadata,
        }
    }
}

impl InternalSurrealDBBrokerMessage {
    fn from(queue_name: &str, msg: InternalBrokerMessage) -> Result<Self, BroccoliError> {
        let id: RecordId = message_record_id(queue_name, &msg.task_id)?;
        let uuid = match surrealdb::types::Uuid::from_str(&msg.task_id) {
            Ok(uuid) => Ok(uuid),
            Err(e) => Err(BroccoliError::Broker(format!(
                "Incorrect uuid {}: {}",
                &msg.task_id,
                e
            ))),
        }?;
        Ok(Self {
            id,
            task_id: uuid,
            payload: msg.payload,
            attempts: msg.attempts,
            metadata: msg.metadata,
        })
    }
}

// // helpers that get the table names for from a queue_name
// // ___queue: time series, bucketed by priority, timestamp, and task id
// // ___processing: in process
// // ___index: index to go from message to the ___queue, useful to cancel
// // ___failed: failed messages table

/// this table holds a timesorted timeseries, `<queue_name>`:[<priority>,<timestamp>,u<taskid>]
/// and acts as the queue
#[must_use] pub fn queue_table(queue_name: &str) -> String {
    format!("{queue_name}___queue")
}

/// table holds messages in process
fn processing_table(queue_name: &str) -> String {
    format!("{queue_name}___processing")
}

/// this is an index to go from <queue_name>___index:[u<taskid>,<queuename>] to the queue table
/// in O(k) time
fn index_table(queue_name: &str) -> String {
    format!("{queue_name}___index")
}

/// this is the failed messages table
fn failed_table(queue_name: &str) -> String {
    format!("{queue_name}___failed")
}


/// `queue_name` + task id : namely `queue_name:`task_id``
fn message_record_id(queue_name: &str, task_id: &str) -> Result<RecordId, BroccoliError> {
    // we ensure it's a uuid but we use a backticked string as q:u'uuid' does not work anymore
    match surrealdb::types::Uuid::from_str(task_id) {
        Ok(uuid) => {
            let record_id = surrealdb::types::RecordId::new(queue_name, uuid.to_string());
            // let record_id = RecordId::from_inner(message_id);
            Ok(record_id)
        }
        Err(e) => Err(BroccoliError::Broker(format!(
            "{} is not a valid uuid: {}",
            &task_id,
            e
        ))),
    }
}



/// time+id range record id, namely `queue_table:[priority, when, task_id]`
fn queue_record_id(
    queue_name: &str,
    priority: i64,
    when: surrealdb::types::Datetime,
    task_id: surrealdb::types::Uuid,
) -> Result<RecordId, BroccoliError> {
    let queue_table = self::queue_table(queue_name);
    let priority  =  surrealdb::types::Value::from_i64(priority);
    let task_id_uuid_sql_val = surrealdb::types::Value::from_string(task_id.to_string());
    let datetime: surrealdb::types::Datetime = when.into();
    let datetime = surrealdb::types::Value::from_datetime(datetime);
    let vec_id = surrealdb::types::Value::from_vec(vec![priority, datetime, task_id_uuid_sql_val]);
    let queue_thing = surrealdb::types::RecordIdKey::from_value(vec_id).map_err(|e| BroccoliError::Broker(format!(
            "could not build queue id for {} queue: {}",
            &queue_name,
            e
        )))?;
    let queue_record_id = RecordId::new(queue_table, queue_thing);
    Ok(queue_record_id)
}

/// `index_table`:[`task_id`, `queue_name`]
/// could simplify to index only by `task_id` but the queue name is useful for observability purposes
fn index_record_id(task_id: &str, queue_name: &str) -> Result<RecordId, BroccoliError> {
    let index_table = self::index_table(queue_name);
    let uuid = surrealdb::types::Uuid::from_str(task_id)
        .map_err(|e| BroccoliError::Broker(format!("{task_id} is not a uuid: {}", e)))?;
    let uuid_val  = surrealdb::types::Value::from_string(uuid.to_string());
    let queue_name_val = surrealdb::types::Value::from_string(queue_name.to_owned());
    let vec_id = surrealdb::types::Value::from_vec(vec![uuid_val, queue_name_val]);
    let index_thing = surrealdb::types::RecordIdKey::from_value(vec_id).map_err(|e| BroccoliError::Broker(format!(
            "could not build index id for {}: {}",
            &queue_name,
            e
        )))?;
    let index_record_id = RecordId::new(index_table, index_thing);
    Ok(index_record_id)
}

/// add to the end of the timeseries queue without scheduling
pub async fn add_to_queue(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &String,
    priority: i64,
    ts: chrono::DateTime<chrono::Utc>,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let now = surrealdb::types::Datetime::default(); // this is now()
    self::add_to_queue_scheduled(db, queue_name, task_id, priority, now, ts, err_msg).await
}

/// add to the end of the timeseries queue with a delay duration
pub async fn add_to_queue_delayed(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &String,
    priority: i64,
    delay: Duration,
    ts: chrono::DateTime<chrono::Utc>,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    // this is a convoluted conversion as surrealdb::sql::Datetime does not support adding
    // we convert to chrono structures and add, and then convert back
    let now: chrono::DateTime<chrono::Utc> = surrealdb::types::Datetime::default().into();
    let secs = delay.whole_seconds();
    let ns = delay.subsec_nanoseconds();
    let ns: u32 = ns.try_into().unwrap_or(0);
    let delay = chrono::TimeDelta::new(secs, ns).unwrap_or_default();
    let when = now.checked_add_signed(delay).unwrap_or(now);
    let when: surrealdb::types::Datetime = when.into();
    self::add_to_queue_scheduled(db, queue_name, task_id, priority, when, ts, err_msg).await
}

/// add to the timeseries at a scheduled time, can be in the past and it will be triggered immediately
pub async fn add_to_queue_scheduled(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &String,
    priority: i64,
    when: surrealdb::types::Datetime,
    ts: chrono::DateTime<chrono::Utc>,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    self::add_record_to_queue(db, queue_name, task_id, priority, when, ts, err_msg).await
}

// internal implementation to add the record to the timeseries queue + index
// 1) we add the index first
// 2) we add the queue entry last, as that is what consumers see
async fn add_record_to_queue(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &String,
    priority: i64,
    when: surrealdb::types::Datetime,
    ts: chrono::DateTime<chrono::Utc>,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let uuid = match surrealdb::types::Uuid::from_str(task_id) {
        Ok(uuid) => Ok(uuid),
        Err(e) => Err(BroccoliError::Broker(format!("{} is not a uuid: {}", &task_id, e))),
    }?;
    let queue_record_id = queue_record_id(queue_name, priority, when, uuid)?;
    let () =  self::add_to_queue_index(db, queue_name, task_id, queue_record_id.clone(), ts, err_msg).await?;
    
    let message_record_id = message_record_id(queue_name, task_id)?;
    let msg = InternalSurrealDBBrokerMessageEntry {
        id: queue_record_id.clone(),
        message_id: message_record_id.clone(),
        priority,
        timestamp: ts.into(),
    };
    let mut retryable = RetriableSurrealDBResult::new(format!("{err_msg}:'{queue_name}': adding to queue"));
    while !retryable.is_done() {
        let result: Result<Option<InternalSurrealDBBrokerMessageEntry>, surrealdb::Error> = db
            .create(&queue_record_id)
            .content(msg.clone())
            .await;
        retryable = retryable.step(result).await;
    }
    let qm = retryable.wrapup()?;
    match qm {
        Some(_) =>Ok(()),
        None => Err(BroccoliError::Broker(format!(
            "{err_msg}:'{queue_name}': adding to queue (silently did not add)",
        ))),
    }
}

// we add an entry into a queue index, index:[messageid,queue_name] {queue_id} where
// queue_id is basically: queue:[timestamp,messageid].
// We can use this index we can do cancellations in O(k) time and workaround parser issues
async fn add_to_queue_index(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &str,
    queue_id: RecordId, // queue:[timestamp, task_id]
    ts: chrono::DateTime<chrono::Utc>,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    // we create the index record and add to the index
    // we upsert because if re reenqueue we will be re-setting and not creating from scratch
    let index_record_id = index_record_id(task_id, queue_name)?;
    let qm: Option<InternalSurrealDBBrokerQueueIndex> = db
        .upsert(index_record_id)
        .content(InternalSurrealDBBrokerQueueIndex { queue_id, timestamp: ts.into() })
        .await
        .map_err(|e: surrealdb::Error| {
            BroccoliError::Broker(format!("{err_msg}:'{queue_name}': {e}"))
        })?;
    match qm {
        Some(_) => Ok(()), // happy path
        None => Err(BroccoliError::Broker(format!(
            "{err_msg}:'{queue_name}': adding to index (silently did not add)",
        ))),
    }
}

/// get the index message given a queue name and task id, in O(k) time
/// None is a valid return value if the message was never in the system in the first place
async fn get_queue_index(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &str,
    err_msg: &'static str,
) -> Result<Option<InternalSurrealDBBrokerQueueIndex>, BroccoliError> {
    let index_record_id = index_record_id(task_id, queue_name)?;
    let queue_index: Option<InternalSurrealDBBrokerQueueIndex> = db
        .select(index_record_id)
        .await
        .map_err(
        |e: surrealdb::Error| BroccoliError::Broker(format!("{err_msg}:'{queue_name}': {e}")),
    )?;
    Ok(queue_index)
}

// clear the index table
async fn remove_from_queue_index(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &str,
    err_msg: &'static str,
) -> Result<InternalSurrealDBBrokerQueueIndex, BroccoliError> {
    // we build {index_table}:[`{task_id}`,'{queue_name}']
    let index_record_id = index_record_id(task_id, queue_name).map_err(|e| {
            BroccoliError::Broker(format!("{err_msg}:'{queue_name}': {e}"))
        })?;

    let deleted: Option<InternalSurrealDBBrokerQueueIndex> = db
        .delete(index_record_id)
        .await
        .map_err(|e: surrealdb::Error| {
            BroccoliError::Broker(format!("{err_msg}:'{queue_name}': {e}"))
        })?;
    match deleted {
        Some(deleted) => Ok(deleted), // happy path
        None => Err(BroccoliError::Broker(format!(
            "{err_msg}:'{queue_name}': removing from queue index (silently did not delete)",
        ))),
    }
}

/// get first queued message payload if any, non-blocking, as part of a transaciton
/// 1) get queued
/// 2
pub async fn get_queued_transaction(
    db: &Surreal<Any>,
    queue_name: &str,
    auto_ack: bool,
    batch_size: usize,
    err_msg: &'static str,
) -> Result<Vec<InternalBrokerMessage>, BroccoliError> {
    let mut retryable = RetriableSurrealDBResult::new(format!("{err_msg}:'{queue_name}': transaction consume"));
    while !retryable.is_done() {
        let transaction = get_queued_transaction_impl(
             db,
            queue_name,
            auto_ack,
            batch_size,
            "{err_msg}:'{queue_name}' transaction consume (removing from queue and adding to processed)",
        ).await;
        retryable = retryable.step(transaction).await;
    }
    // if we had a typed race condition error, we return empty, as someone else
    // got the queued transaction first (optimistic locking)
     match retryable.wrapup() {
        Ok(r) => Ok(r),
        Err(e) => match e {
            BroccoliError::BrokerNonIdempotentOp(_) => Ok(vec![]),
            e => Err(e)
        },
    }
}

pub(crate) async fn get_queued_transaction_impl(
    db: &Surreal<Any>,
    queue_name: &str,
    auto_ack: bool,
    batch_size: usize,
    err_msg: &'static str,
) -> Result<Vec<InternalBrokerMessage>, BroccoliError> {
    let queue_table = self::queue_table(queue_name);
    let processing_table = self::processing_table(queue_name);
    //panic!("AAA");
    let q = r#"
        BEGIN TRANSACTION;
        {
            -- first of all, we  extract messages up to the batch size limit, in priority order
            -- this implementation is only for surrealdb 2.1.0+
            LET $msgs = [1,2,3,4,5].fold({out_: [], remaining_: $batch_size, t_: $queue_table}, |$acc, $p| { 
                IF $acc.remaining_>0 {
                    LET $output = SELECT * FROM type::record($acc.t_,[$p,None]..=[$p,time::now()]) LIMIT $acc.remaining_;
                    LET $size = IF type::is_none($output) {0} ELSE {array::len($output)};
                    IF $size>0 {
                        RETURN {out_: array::concat($acc.out_, $output), remaining_: $acc.remaining_-$size, t_: $acc.t_};
                    } ELSE {
                        RETURN $acc;
                    }
                } ELSE {
                        RETURN $acc;
                }
            }).out_;
            IF !type::is_array($msgs) OR array::is_empty($msgs) { -- nothing on the queue
                RETURN []
            };
            -- next we iterate over the messages, create the in process if needed, delete and get payload
            LET $payloads = array::fold($msgs, {out_: [], t_: $processing_table, auto_ack_: $auto_ack}, |$acc, $e|  {
                -- remove from queue and return payload
                -- remember we don't delete from index, instead acknowledge/reject/cancel will do it

                LET $deleted = DELETE ONLY $e.id RETURN BEFORE;
                IF !$deleted {
                    -- if it was not deleted we will not abort the transaction, we just won't return the payload
                    RETURN $acc
                };
                IF !$acc.auto_ack_ {
                    -- upserting will be more robust and not freeze the queue if there is a duplicate
                    UPSERT type::record($acc.t_, $e.id[2]) CONTENT { // id[2] is the uuid
                        message_id: $e.message_id,
                        priority: $e.priority,
                        timestamp: time::now()
                    };
                };
                LET $payload = SELECT * FROM ONLY $e.message_id;
                {out_: array::append($acc.out_, $payload), t_: $acc.t_, auto_ack_: $acc.auto_ack_};
            });
            $payloads.out_
        };
        COMMIT TRANSACTION;
    "#;
    let result = db
        .query(q)
        .bind(("queue_table", queue_table))
        .bind(("processing_table", processing_table))
        .bind(("auto_ack", auto_ack))
        .bind(("batch_size", batch_size))
        .await;

    match result {
        Ok(mut resp) => {
            let returned: Result<
                Vec<InternalSurrealDBBrokerMessage>,
                surrealdb::Error,
            > = resp.take(resp.num_statements() - 2); // changed behaviour in 3.x
            let transaction = resp.check(); //take(0 as usize);
            match transaction {
                Ok(_) => {
                    match returned {
                        Ok(returned) =>
                            Ok(returned.into_iter().map(|im| {
                                let m: InternalBrokerMessage = im.into();
                                m // from internal to external rep
                            }).collect()), 
                        Err(e) => 
                        Err(transaction_error(&e,format!(
                    "{err_msg}:'{queue_name}' Could not remove+read from queue in transaction (taking deleted value): {e}"
                )))
                        }

                },
                Err(e) =>  {
                    let e: surrealdb::Error = e.into(); 
                    Err(transaction_error(&e,format!(
                        "{err_msg}:'{queue_name}' Could not remove+read from queue in transaction (transaction failed): {e}"
                    )))},
           }
        }
        Err(e) => Err(transaction_error(
            &e,
            format!("{err_msg}:'{queue_name}' Could not get queued+read in transaction: {e}"),
        )),
    }
}

/// remove from ordered queue
/// `queued_message_id` must be: queue:[priority, timestamp, `task_id`]
pub(crate) async fn remove_from_queue(
    db: &Surreal<Any>,
    queue_name: &str,
    queued_message_id: RecordId, // queue:[priority, timestamp, task_id]
    err_msg: &'static str,
) -> Result<InternalSurrealDBBrokerMessageEntry, BroccoliError> {
    let deleted: Option<InternalSurrealDBBrokerMessageEntry> =
        db.delete(queued_message_id)
            .await
            .map_err(|e| BroccoliError::Broker(format!("{err_msg}:'{queue_name}': {e}")))?;
    deleted.map_or_else(
        || {
            Err(BroccoliError::BrokerNonIdempotentOp(format!(
                "{err_msg}:'{queue_name}': Removing from queue (silently nothing was removed, potentially a CONCURRENT_READ)",
            )))
        },
        Ok,
    )
}

async fn remove_from_queue_add_to_processed_transaction_impl(
    db: &Surreal<Any>,
    queue_name: &str,
    queued_message: InternalSurrealDBBrokerMessageEntry,
    err_msg: &'static str,
) -> Result<InternalSurrealDBBrokerMessageEntry, BroccoliError> {
    let queue_table = self::queue_table(queue_name);
    let processing_table = self::processing_table(queue_name);
    let message_id = queued_message.message_id; // reference to the original message
    let queued_message_id = queued_message.id; // what gets deleted
    let priority = queued_message.priority;
    let q = "
            BEGIN TRANSACTION;
            {
                // loses the uuid, see https://github.com/surrealdb/surrealdb/issues/6104
                LET $processing_id = type::record($processing_table, $message_id),
                // we forcefully set it
                //LET $processing_id = type::record($processing_table+':u\\''+<string>record::id($message_id)+'\\'');
                -- upserting will be more robust and not freeze the queue if there is a duplicate
                LET $c = UPSERT type::table($processing_table) CONTENT {
                    id: $processing_id,                            
                    message_id: $message_id,
                    priority: $priority,
                    timestamp: time::now()
                } RETURN AFTER;
                IF !$c {
                    THROW 'Transaction failed adding to processing, '+<string>$processing_id;
                };
                -- if message is still in the queue, remove it and return payload
                -- otherwise we explicitly abort the transaction
                -- (remember we don't delete from index, instead acknowledge/reject/cancel will do it)
                LET $m = DELETE ONLY $queued_message_id RETURN BEFORE;
                IF !$m {
                    THROW 'Transaction failed removing from queue, '+<string>$queued_message_id+' already deleted (CONCURRENT_READ)';
                };
                $m
            };
            COMMIT TRANSACTION;
    ";
    let result = db
        .query(q)
        .bind(("queue_table", queue_table))
        .bind(("processing_table", processing_table))
        .bind(("message_id", message_id))
        .bind(("queued_message_id", queued_message_id))
        //.bind(("uuid", uuid))
        .bind(("priority", priority))
        .await;
    // capture the result, note that if there is an error that is related to a 'transaction' that can be 'retried''
    // error scenarios:
    // - likely concurrent reads:
    //   - response.check() failed: any of the statements failed, which is namely the transaction or the deleted retrieval, in this case
    //   - returned value error: getting $m itself failed, should not happen, but type safety
    //   - returned value retrieved but none: DELETE did not return anything even though the transaction was successful
    // - not concurrent read related:
    //   - reponse error: most likely transport or infra related, not related to concurrent reads
    match result {
        Ok(mut resp) => {
            let returned: Result<
                Option<InternalSurrealDBBrokerMessageEntry>,
                surrealdb::Error,
            > = resp.take(resp.num_statements() - 1);
            let transaction = resp.check(); //take(0 as usize);
            match transaction {
                Ok(_) => {
                    match returned {
                        Ok(returned) => match returned {
                            Some(returned) => Ok(returned),
                            None => Err(BroccoliError::Broker(format!(
                    "{err_msg}:'{queue_name}' Could not remove from queue in transaction (nothing was deleted but no error)"
                ))),
                        },
                        Err(e) => 
                        Err(transaction_error(&e,format!(
                    "{err_msg}:'{queue_name}' Could not remove from queue in transaction (taking deleted value): {e}"
                )))
                        }

                },
                Err(e) =>  {
                    let e: surrealdb::Error = e.into();
                    Err(transaction_error(&e,format!(
                        "{err_msg}:'{queue_name}' Could not remove from queue in transaction (transaction failed): {e}"
                    )))},
           }
        }
        Err(e) => Err(transaction_error(
            &e,
            format!("{err_msg}:'{queue_name}' Could not get queued in transaction: {e}"),
        )),
    }
}

/// remove from ordered queue and add to in process list within the same transaction
/// (unused at the moment as it allows for concurrent reads)
/// `queued_message_id` must be: queue:[timestamp, `task_id`]
/// returns None if the record is not in the queue anymore, most likely due to a concurrent read
pub(crate) async fn remove_from_queue_add_to_processed_transaction(   
    db: &Surreal<Any>,
    queue_name: &str,
    queued_message: InternalSurrealDBBrokerMessageEntry,
    err_msg: &'static str,
) -> Result<Option<InternalSurrealDBBrokerMessageEntry>, BroccoliError> {
    let mut retryable = RetriableSurrealDBResult::new(format!("{err_msg}:'{queue_name}': consume transaction"));
    while !retryable.is_done() {
        let transaction = remove_from_queue_add_to_processed_transaction_impl(
             db,
            queue_name,
            queued_message.clone(),
            "{err_msg}:'{queue_name}' consume transaction (removing from queue and adding to processed)",
        ).await;
        retryable = retryable.step(transaction).await;
    }
     match retryable.wrapup() {
        Ok(r) => Ok(Some(r)),
        Err(e) => match e {
            BroccoliError::BrokerNonIdempotentOp(_) => Ok(None),
            e => Err(e)
        },
        
    }
}

fn is_transaction_error(e: &surrealdb::Error) -> bool {
    is_transaction_error_str(&format!("{}", &e))
    
}

fn is_transaction_error_str(e: &str) -> bool {
    e.contains("This transaction can be retried") 
        || e.contains("The query was not executed due to a failed transaction") 
        || e.contains("Resource busy")
}

fn transaction_error(e: &surrealdb::Error, msg: String) -> BroccoliError {
    let e_str = format!("{}", &e);
    if is_transaction_error(e) {
        BroccoliError::BrokerNonIdempotentRetriableOp(msg)
    } else if e_str.contains("CONCURRENT_READ") {
        BroccoliError::BrokerNonIdempotentOp(msg)
    } else {
        BroccoliError::Broker(msg)
    }
}

/// given the user facing task id, remove from the queue
pub(crate) async fn remove_queued_from_index(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &str,
    err_msg: &'static str,
) -> Result<Option<InternalSurrealDBBrokerMessageEntry>, BroccoliError> {
    let queue_index = self::get_queue_index(db, queue_name, task_id, err_msg).await?;
    log::warn!("{:?}", &queue_index);
    match queue_index {
        Some(queue_index) => {
            let removed =
                self::remove_from_queue(db, queue_name, queue_index.queue_id, err_msg).await?;
            Ok(Some(removed))
        }
        None => Ok(None), // message was not in the system
    }
}

/// add the message itself with it's payload
pub async fn add_message(
    db: &Surreal<Any>,
    queue_name: &str,
    msg: &InternalBrokerMessage,
    err_msg: &'static str,
) -> Result<InternalBrokerMessage, BroccoliError> {
    let message_record_id = message_record_id(queue_name, &msg.task_id)?;
    let message = InternalSurrealDBBrokerMessage::from(queue_name, msg.to_owned())?;
    let mut retries: u64 = 0;
    let mut status: Option<Result<Option<InternalSurrealDBBrokerMessage>, BroccoliError>> = None;
    let max: u64 = 10;
    while status.is_none() && retries < max {
        let result = db
            .create(message_record_id.clone())
            .content(message.clone())
            .await;
        status = match result {
            Ok(added) => Some(Ok(added)), // happy path
            Err(e) => {
                if is_transaction_error(&e) {
                    retries += 1;
                    tokio::time::sleep(tokio::time::Duration::from_millis(retries)).await;
                    None
                } else {
                    Some(Err(BroccoliError::Broker(format!(
                        "{err_msg}: adding message: {e}"
                    ))))
                }
            }
        }
    }
    let added: Option<InternalSurrealDBBrokerMessage> = match status {
        Some(result) => result,
        None => Err(BroccoliError::Broker(format!(
            "{err_msg}: adding message (too many retries)",
        ))),
    }?;
    added.map_or_else(
        || {
            Err(BroccoliError::Broker(format!(
                "{err_msg}: adding message (silently did not add anything)",
            )))
        },
        |added| Ok(added.into()),
    )
}

/// update the message, done to update the number of attempts, leaving rest unchanged
pub async fn update_message(
    db: &Surreal<Any>,
    queue_name: &str,
    msg: InternalBrokerMessage,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    let message_record_id = message_record_id(queue_name, &msg.task_id)?;
    let message = InternalSurrealDBBrokerMessage::from(queue_name, msg.clone())?;
    let updated: Option<InternalSurrealDBBrokerMessage> = db
        .update(message_record_id)
        .content(message)
        .await
        .map_err(|e: surrealdb::Error| {
            BroccoliError::Broker(format!("{err_msg}:'{queue_name}': {e}"))
        })?;
    match updated {
        Some(_) => Ok(()),
        None => Err(BroccoliError::Broker(format!(
            "{err_msg}:'{queue_name}': Updating message (silently no update)"
        ))),
    }
}

/// get the message payload given the queue record
pub(crate) async fn get_message_from(
    db: &Surreal<Any>,
    queue_name: &str,
    queued_message: InternalSurrealDBBrokerMessageEntry,
    err_msg: &'static str,
) -> Result<InternalBrokerMessage, BroccoliError> {
    let message_id = queued_message.message_id;
    self::get_message(db, queue_name, message_id, err_msg).await
}

/// get the actual message
/// `message_id` <`queue_table>`:[<`task_id`>]
pub async fn get_message(
    db: &Surreal<Any>,
    queue_name: &str,
    message_id: RecordId,
    err_msg: &'static str,
) -> Result<InternalBrokerMessage, BroccoliError> {
    let message: Option<InternalSurrealDBBrokerMessage> = db
        .select(message_id)
        .await
        .map_err(|e| BroccoliError::Broker(format!("{err_msg}:'{queue_name}': {e}")))?;
    message.map_or_else(
        || {
            Err(BroccoliError::Broker(format!(
                "{err_msg}:'{queue_name}': getting message (silently did not get anything)"
            )))
        },
        |message| Ok(message.into()),
    )
}

/// remove actual message (the one with the payload)
/// we also remove it from the internal index
/// `message_id`: `<queue_table>`:[<`task_id`>]
pub(crate) async fn remove_message(
    db: &Surreal<Any>,
    queue_name: &str,
    message_id: RecordId, //<queue_table>:[<task_id>]
    task_id: &str,
    err_msg: &'static str,
) -> Result<InternalSurrealDBBrokerMessage, BroccoliError> {
    let resp: Option<InternalSurrealDBBrokerMessage> = db
        .delete(message_id)
        .await
        .map_err(|e| BroccoliError::Broker(format!("{err_msg}:'{queue_name}': {e}")))?;
    match resp {
        Some(removed) => {
            // remove from index
            let _ = self::remove_from_queue_index(db, queue_name, task_id, err_msg).await?;
            Ok(removed)
        }
        None => Err(BroccoliError::Broker(format!(
            "{err_msg}: removing message (silently did not remove anything)"
        ))),
    }
}

/// remove from the processing queue
pub(crate) async fn remove_from_processing(
    db: &Surreal<Any>,
    queue_name: &str,
    message_id: &String,
    err_msg: &'static str,
) -> Result<InternalSurrealDBBrokerMessageEntry, BroccoliError> {
    let processing_table = self::processing_table(queue_name);
    let uuid = match surrealdb::types::Uuid::from_str(message_id) {
            Ok(uuid) => Ok(uuid),
            Err(e) => Err(BroccoliError::Broker(format!(
                "Incorrect uuid {}: {}",
                &message_id,
                e
            ))),
        }?;
    //let uuid: surrealdb::sql::Id = uuid.into();
    // let uuid: surrealdb::types::Id = uuid.into();
    //let message_id = surrealdb::expr::Thing::from((processing_table.as_str(), uuid));
    let record_id_str = format!("{processing_table}:`{uuid}`");
    let record_id = RecordId::new(processing_table, uuid.to_string());
    let processed: Option<InternalSurrealDBBrokerMessageEntry> = db
        .delete(record_id.clone())
        .await
        .map_err(|e| BroccoliError::Broker(format!("{err_msg}:'{queue_name}' {e}")))?;
    processed.map_or_else(
        || {
            Err(BroccoliError::Broker(format!(
            "{err_msg}:'{queue_name}':{record_id_str} removing from processing (silently nothing removed)"
        )))
        },
        Ok,
    )
}

pub(crate) async fn remove_message_and_from_processing_transaction(
    db: &Surreal<Any>,
    queue_name: &str,
    task_id: &str,
        err_msg: &'static str,
) -> Result<InternalSurrealDBBrokerMessageEntry, BroccoliError> {
//) -> Result<(), BroccoliError> {
    let processing_table = processing_table(queue_name);
    let index_table = index_table(queue_name);
    let q = "
        BEGIN TRANSACTION;
        {
            -- see https://github.com/surrealdb/surrealdb/issues/6104 for context on the weird conversions
            -- delete payload
            LET $message_id = type::record($queue_name, $task_id);
            LET $m = DELETE $message_id RETURN BEFORE;
            IF !$m {
                THROW 'Transaction failed removing payload '+<string>$message_id+ ' (CONCURRENT_READ)';
            };

            -- remove from index
            LET $index_id = type::record($index_table, [$task_id, $queue_name]);
            LET $idx = DELETE $index_id RETURN BEFORE;
            IF !$idx {
                THROW 'Transaction failed removing index '+<string>$index_id+' (CONCURRENT_READ)';
            };
            -- remove from processing
            LET $processing_id = type::record($processing_table,$task_id);
            LET $p = DELETE ONLY $processing_id RETURN BEFORE;
            IF !$p {
                THROW 'Transaction failed removing from processing '+<string>$processing_id+' (CONCURRENT_READ)';
            };
            $p
        };
        COMMIT TRANSACTION;
    ";

        let result = db
        .query(q)
        .bind(("queue_name", queue_name.to_owned()))
        .bind(("processing_table", processing_table))
        .bind(("index_table", index_table))
        .bind(("task_id", task_id.to_owned()))
        .await;
    match result {
        Ok(mut resp) => {
            let returned: Result< Option<InternalSurrealDBBrokerMessageEntry>, surrealdb::Error> = resp.take(resp.num_statements()-2);
            let transaction = resp.check();
            match transaction {
                Ok(_) => match returned {
                    Ok(returned) => match returned {
                        Some(entry) => Ok(entry),
                        None => Err( BroccoliError::Broker(
                            format!("'{queue_name}' Could not ack in transaction (nothing returned)"),
                        )),
                    },
                    Err(e) => Err(transaction_error(
            &e,
            format!("{err_msg}:'{queue_name}' Could not ack in transaction (response issue): {e}"),
        )),
                },
        Err(e) => {
            let e: surrealdb::Error = e.into();
            Err(transaction_error(
            &e,
            format!("{err_msg}:'{queue_name}' Could not ack in transaction (could not take): {e}"),
        ))},

            }
        },
        Err(e) => Err(transaction_error(
            &e,
            format!("{err_msg}:'{queue_name}' Could not ack in transaction: {e}"),
        )),

    }
    //Ok(())

}

/// add to the failed queue, will also remove from index
pub async fn add_to_failed(
    db: &Surreal<Any>,
    queue_name: &str,
    message_id: surrealdb::types::Uuid,
    msg: InternalBrokerMessage,
    err_msg: &'static str,
) -> Result<(), BroccoliError> {
    // we need:
    // - failed table name
    // - surrealdb uuid to reference in the id attribute
    // - plan uuid (non-surrealdb, reexported by surrealdb as surrealdb::uuid) as that is the only
    //   one that record id accepts (amazingly)
    let failed_table = self::failed_table(queue_name);
    // none of these conversions work, here for reference
    // let uuid_value: Value = message_id.into();
    // let uuid_value: Value = plain_uuid.into();
    // let uuid_key: RecordIdKey = uuid_value.into();
    // let thing: surrealdb::sql::Thing = message_id.into();
    // let thing: surrealdb::sql::Thing = plain_uuid.into();
    // let id: surrealdb::sql::Id = (failed_table, message_id).into();
    //let uuid_value: Value = id.into();
    //let uuid_key: RecordIdKey = id.into();
    //let failed_record_id: RecordId = (failed_table, id).into();
    // these work but depend on the exact environment/version and are flakey
    // let uuid_value: surrealdb::sql::Value = message_id.into();
    // let _plain_uuid: surrealdb::Uuid = message_id.into();
    // let id: surrealdb::sql::Id = message_id.into();

    let failed_record = InternalSurrealDBBrokerFailedMessage {
        id: None, // it will be added by serde
        original_msg: InternalSurrealDBBrokerMessage::from(queue_name, msg)?,
        timestamp: chrono::Utc::now().into(),
    };
    let q = "CREATE type::thing($failed_table, $message_id) CONTENT $failed_record";
    let _ = db
        .query(q)
        .bind(("failed_table", failed_table))
        .bind(("message_id", message_id.to_string()))
        .bind(("failed_record", failed_record))
        .await
        .map_err(|e| BroccoliError::Broker(format!("{err_msg}:'{queue_name}' {e}")))?;
    Ok(())
}

/// helper to process results from surrealdb that are retriable
pub(crate) struct RetriableSurrealDBResult<T> {
    retries: u8,
    status: Option<Result<T, BroccoliError>>,
    max: u8,
    prefix: String,
} 

impl<T> RetriableSurrealDBResult<T> {
    
    // create a new retriable result with this error message prefix
    const fn new(prefix: String) -> Self{
        Self{ retries: 0, status: None, max: 10, prefix } 
    }

    // we either have a result or we have exhausted the number of retries
    fn is_done(&self) -> bool {
        self.status.is_some() || self.retries >= self.max
    }

    fn retries(&self) -> u8 {
        self.retries
    }

    // take a surrealdb result and process it, updating internal state
    // if the result is a retriable transaction, sleep for a small number of ms
    async fn step<E>(mut self, result: Result<T, E>) -> Self
    where E: std::fmt::Display, {
        match result {
            Ok(r) => 
                self.status = Some(Ok(r)),
            Err(e) => if is_transaction_error_str(&format!("{}", &e)) {
                    tokio::time::sleep(tokio::time::Duration::from_millis(u64::from(self.retries))).await;
                    self.retries += 1;
                } else {
                    self.status = Some(Err(BroccoliError::Broker(format!(
                        "{}: {}",
                        self.prefix,
                        e,
                    ))));
                }
        }
        self
    }

    // wrapup, either get the original result, wrapped in a broccoli error
    // or a too many retries error
    fn wrapup(self) -> Result<T, BroccoliError> {
        match self.status {
            Some(result) => result,
            None => Err(BroccoliError::Broker(format!(
                "{} (max number of retries)",self.prefix
            ))),
        }
    }
}
