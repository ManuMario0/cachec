//! Single Producer Multiple Consumer channel.
//!
//! This module provides type aliases that specialise the generic [`super::mpmc`] channel
//! to a single producer and multiple consumers (`P = 1`).

use super::mpmc;

pub type Channel<T, const N: usize, const C: usize> = mpmc::Channel<T, N, 1, C>;
pub type Producer<T, const N: usize, const C: usize> = mpmc::Producer<T, N, C>;
pub type Consumer<T, const N: usize> = mpmc::Consumer<T, N, 1>;
