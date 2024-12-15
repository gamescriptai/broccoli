pub mod brokers;
pub mod error;

use brokers::{broker::Broker, connect::connect_to_broker};

pub struct BroccoliQueue {
    broker: Box<dyn Broker>,
}

impl BroccoliQueue {
    async fn new(broker_url: String) -> Self {
        let broker = connect_to_broker(&broker_url)
            .await
            .map_err(|e| {
                panic!("Failed to connect to broker: {:?}", e);
            })
            .unwrap();
        BroccoliQueue { broker }
    }

    pub async fn publish(&self, topic: &str, message: &str) {
        self.broker
            .publish(topic, message)
            .await
            .map_err(|e| {
                panic!("Failed to publish message: {:?}", e);
            })
            .unwrap();
    }

    pub async fn consume(&self, topic: &str) -> String {
        self.broker
            .consume(topic)
            .await
            .map_err(|e| {
                panic!("Failed to consume message: {:?}", e);
            })
            .unwrap()
    }

    pub async fn try_consume(&self, topic: &str) -> Option<String> {
        self.broker
            .try_consume(topic)
            .await
            .map_err(|e| {
                panic!("Failed to consume message: {:?}", e);
            })
            .unwrap()
    }

    pub async fn retry(&self, topic: &str, message: &str) {
        self.broker
            .retry(topic, message)
            .await
            .map_err(|e| {
                panic!("Failed to retry message: {:?}", e);
            })
            .unwrap();
    }
}
