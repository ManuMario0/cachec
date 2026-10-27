//! Single Producer Single Consumer channel.
//!
//! This module provides type aliases that specialise the generic [`super::mpmc`] channel
//! to a single producer and a single consumer (`P = 1`, `C = 1`).

use super::mpmc;

pub type Channel<T, const N: usize> = mpmc::Channel<T, N, 1, 1>;
pub type Producer<T, const N: usize> = mpmc::Producer<T, N, 1>;
pub type Consumer<T, const N: usize> = mpmc::Consumer<T, N, 1>;
