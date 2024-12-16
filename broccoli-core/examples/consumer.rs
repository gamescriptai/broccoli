use broccoli_core::{error::BroccoliError, queue::BroccoliQueue};
use serde::{Deserialize, Serialize};
use std::{error::Error, time::Duration};
use tokio::task::JoinError;

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

async fn process_job(job: JobPayload) -> Result<(), BroccoliError> {
    println!("Processing job: {} ({})", job.id, job.task_name);

    // Simulate some work
    tokio::time::sleep(Duration::from_secs(1)).await;

    println!("Thowing error: {}", job.id);

    Err(BroccoliError::Job("Failed to process job".to_string()))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize the queue
    let queue = BroccoliQueue::builder("redis://localhost:6379")
        .retry_attempts(3)
        .pool_connections(5)
        .build()
        .await?;

    // Process regular jobs
    let regular_handler = tokio::spawn(async move {
        queue
            .process_messages(
                "jobs",
                2,
                |msg| async move { process_job(msg.payload).await },
            )
            .await
    });

    // Handle shutdown signals
    let result = regular_handler.await?;

    dbg!(result);

    println!("Consumer shutdown complete");
    Ok(())
}
