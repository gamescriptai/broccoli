use broccoli_queue::queue::BroccoliQueue;

pub async fn clear_redis() {
    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut conn = client.get_connection().unwrap();
    redis::cmd("FLUSHALL").query::<String>(&mut conn).unwrap();
}

pub async fn setup_queue() -> BroccoliQueue {
    clear_redis().await;
    BroccoliQueue::builder("redis://localhost:6379")
        .pool_connections(5)
        .build()
        .await
        .unwrap()
}

pub async fn setup_queue_with_url(
    url: &str,
) -> Result<BroccoliQueue, broccoli_queue::error::BroccoliError> {
    BroccoliQueue::builder(url)
        .pool_connections(5)
        .build()
        .await
}
