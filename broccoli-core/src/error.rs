#[derive(Debug)]
pub enum BroccoliError {
    BrokerError(String),
    PublishError(String),
    ConsumeError(String),
}
