use broccoli::{error::BroccoliError, queue::BroccoliQueue};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JobPayload {
    id: String,
    task_name: String,
    parameters: Parameters,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Parameters {
    input_data: String,
    priority: u8,
    timeout_seconds: u32,
}

#[tokio::main]
async fn main() -> Result<(), BroccoliError> {
    let queue = BroccoliQueue::builder("redis://localhost:6379")
        .failed_message_retry_strategy(Default::default())
        .pool_connections(5)
        .build()
        .await?;

    // Create some example jobs
    let jobs = vec![
        JobPayload {
            id: "job-1".to_string(),
            task_name: "process_data".to_string(),
            parameters: Parameters {
                input_data: "data-1".to_string(),
                priority: 1,
                timeout_seconds: 300,
            },
            created_at: chrono::Utc::now(),
        },
        JobPayload {
            id: "job-2".to_string(),
            task_name: "generate_report".to_string(),
            parameters: Parameters {
                input_data: "data-2".to_string(),
                priority: 2,
                timeout_seconds: 600,
            },
            created_at: chrono::Utc::now(),
        },
    ];

    // Publish jobs in batch
    println!("Publishing jobs...");
    queue.publish_batch("jobs", jobs).await?;

    Ok(())
}
