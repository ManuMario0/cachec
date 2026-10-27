//! Multiple Producer Single Consumer channel.
//!
//! This module provides type aliases that specialise the generic [`super::mpmc`] channel
//! to multiple producers and a single consumer (`C = 1`).

use super::mpmc;

pub type Channel<T, const N: usize, const P: usize> = mpmc::Channel<T, N, P, 1>;
pub type Producer<T, const N: usize> = mpmc::Producer<T, N, 1>;
pub type Consumer<T, const N: usize, const P: usize> = mpmc::Consumer<T, N, P>;
