#![warn(missing_docs)]

//! Broccoli is a message broker library that provides a simple API for sending and receiving messages.
//! It currently supports Redis as a message broker, with plans to support additional brokers in the future, such as RabbitMQ and Kafka.
/// Broccoli core module
pub mod brokers;
/// Broccoli error module
pub mod error;
/// Broccoli message queue module
pub mod queue;
