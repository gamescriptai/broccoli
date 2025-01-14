#![warn(missing_docs)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

//! Broccoli is a message broker library that provides a simple API for sending and receiving messages.
//! It currently supports Redis as a message broker, with plans to support additional brokers in the future, such as RabbitMQ and Kafka.
/// Contains the interfaces for brokers
pub mod brokers;
/// Contains the error types for the Broccoli system
pub mod error;
/// Contains the message queue implementation
pub mod queue;
