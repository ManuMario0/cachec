//! High-performance, lock-free channel primitives.
//!
//! This module provides a family of bounded channels built on top of a single generic
//! [`mpmc`] implementation. The channels are optimised for low-latency, high-throughput
//! communication between threads.
//!
//! Four flavours are available, each exposed as a submodule with convenient type aliases:
//!
//! | Module   | Producers | Consumers |
//! |----------|-----------|-----------|
//! | [`spsc`] | 1         | 1         |
//! | [`mpsc`] | many      | 1         |
//! | [`spmc`] | 1         | many      |
//! | [`mpmc`] | many      | many      |
//!
//! # Example
//!
//! ```rust
//! # use cachec::channel::spsc::Channel;
//! # use std::thread;
//!
//! let channel = Channel::<u64, 16>::new();
//! let (mut producer, mut consumer) = channel.split();
//!
//! let writer = thread::spawn(move || {
//!     producer.write(42);
//! });
//!
//! let reader = thread::spawn(move || {
//!     assert_eq!(consumer.read(), 42);
//! });
//!
//! writer.join().unwrap();
//! reader.join().unwrap();
//! ```
//!
//! # Guarantees
//!
//! The implementation ensures the following properties:
//! - For every value `V` written by a producer, `V` can eventually be read by a consumer;
//! - Each value `V` written by a producer can only be read once;
//! - For every value `V` and `V'`, if `V` was written by a producer before `V'`, and if `V` and `V'` are read by the same consumer, then `V` will be read before `V'`;
//! - For every value `V`, if a consumer read `V`, then `V` was sent by a producer.
//!
//! # Performances
//!
//! Currently, only the push-to-pop latency of the spsc channel is measured. Because of the available tools of the
//! author, the latency could not be measured precisely. The OS noise, CPU throttling (especially on laptop) and hyperthreading
//! cannot be removed, so take the following measurements with a grain of salt and run the experiment yourself on your
//! own OS/CPU. The measurement provided is the best achieved latency.
//!
//! - Intel Core i5-8257U, MacOS Sequoia 15.8.1: 151 cycles (P99)

mod inner;
pub mod mpmc;
pub mod mpsc;
pub mod spmc;
pub mod spsc;
