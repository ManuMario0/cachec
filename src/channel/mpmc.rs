//! Multiple Producer Multiple Consumer channel.
//!
//! An MPMC channel is a one-way communication pipe between a pool of producer threads and a pool of
//! consumer threads. The messages are written to a fixed-size ring buffer by the producers and read
//! by the consumers.
//!
//! Most of the time, the solution for a concurrency problem will be to use a mutex. A mutex provides
//! a simple synchronisation primitive: it ensures a thread cannot acquire the mutex if another thread already
//! holds it. Using this, you can safely read and write on shared slices of memory.
//!
//! However, this is not the fastest mechanism to implement a channel. If you try to write a channel
//! using a mutex, you would have the following situation occurring:
//! - Thread A is writing to slot 1 and holds the mutex;
//! - Thread B wants to read slot 0, and is waiting for the mutex.
//!
//! It would be much better if B could read slot 0 while A is writing to slot 1.
//!
//! To achieve this, a solution is to use a lock-free data structure. A lock-free data structure is a structure
//! that does not make use of a global locking mechanism as described above, but rather locks data more locally.
//! By leveraging cache line alignment and atomics, we can reduce the push-to-pop latency drastically for high-volume, low-latency
//! workloads.
//!
//! # Example
//!
//! Here is a simple example of an asynchronous logger. Two hot threads send log values through a
//! cloned producer, demonstrating the MPSC pattern. The consumer collects all values and verifies
//! that none were lost.
//!
//! ```rust
//! # use cachec::channel::mpmc::Channel;
//! # use std::thread;
//!
//! // 2 producers, 1 consumer, ring-buffer of 16 slots.
//! let channel = Channel::<u64, 16, 2, 1>::new();
//! let (producer, mut consumer) = channel.split();
//!
//! // Clone the producer so each hot thread owns its own handle.
//! let mut producer_a = producer.clone();
//! let mut producer_b = producer;
//!
//! let hot_a = thread::spawn(move || {
//!     for i in 0..4 {
//!         producer_a.write(i);
//!     }
//! });
//!
//! let hot_b = thread::spawn(move || {
//!     for i in 10..14 {
//!         producer_b.write(i);
//!     }
//! });
//!
//! hot_a.join().unwrap();
//! hot_b.join().unwrap();
//!
//! // Collect all values — every write must be received exactly once.
//! let mut values = Vec::new();
//! while let Some(v) = consumer.try_read() {
//!     values.push(v);
//! }
//! values.sort();
//! assert_eq!(values, vec![0, 1, 2, 3, 10, 11, 12, 13]);
//! ```
//!
//! # Guarantees
//!
//! You can refer to the [`channel`](super) module documentation for the precise guarantees
//! provided by this primitive.
//!
//! # Performances
//!
//! You can refer to the [`channel`](super) module documentation for the available performance
//! measurements.

use std::hint::spin_loop;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::channel::inner::spsc;

/// The sender side of the channel.
///
/// A `Producer` does not have a `.new()` procedure. This is because, to create a `Producer`, you first
/// need to create a [`Channel`].
///
/// A `Channel` is the wrapper that holds the allocation to the shared ring-buffer. Once you have created a channel,
/// you call [`.split()`] to generate a `Producer` and a [`Consumer`] out of the channel.
///
/// Additional `Producer` instances are obtained by [cloning](Clone) the original one. The maximum number of
/// producers is bounded by `P`, the const generic parameter of the `Channel`.
///
/// [`.split()`]: Channel::split()
///
/// # Thread safety
///
/// An instance of `Producer` is not meant to be shared between threads. Each thread should own its own
/// `Producer`, obtained via [`Clone`].
pub struct Producer<T, const N: usize, const C: usize> {
    producers: Arc<Mutex<Vec<[SpscProducerWrapped<T, N>; C]>>>,
    producer: [SpscProducerWrapped<T, N>; C],
    next_index: usize,
}

impl<T, const N: usize, const C: usize> Producer<T, N, C> {
    /// Blocks the thread until some data can be written to the channel.
    ///
    /// The implementation for now is not optimal as we use a `spin_loop()` inside the waiting loop.
    /// This is fine if one wants to allow the CPU to make time for hyperthreading.
    /// However, it is less efficient if one is dealing with pinned
    /// and isolated threads, as it will introduce a hard execution stall that will stop the CPU from executing
    /// an instruction for up to 120 cycles on modern Intel microarchitecture (it used to be only 20 cycles).
    /// This cost can be substantial for some applications, and so it is recommended in that case to make use of
    /// [`try_write()`](Self::try_write) in a tight busy loop.
    ///
    /// # Guarantees
    ///
    /// You can refer to the [`channel`](super) module documentation for the precise guarantees provided by this primitive.
    #[inline(always)]
    pub fn write(&mut self, mut data: T) {
        if C == 1 {
            self.producer[0].write_no_checks(data);
            return;
        }
        let mut index = self.next_index;
        loop {
            data = match self.producer[index].try_write(data) {
                Some(data) => data,
                None => {
                    self.next_index = Self::mask(index + 1);
                    return;
                }
            };

            index = Self::mask(index + 1);
            spin_loop();
        }
    }

    /// Tries to write data to the channel.
    ///
    /// The operation may fail and return `Some(data)` — where `data` is the value provided as argument —
    /// if the underlying ring-buffer is full. If there is an active consumer, the operation can be retried
    /// until a slot in the channel is freed.
    ///
    /// # Returns
    ///
    /// `None` if the operation was successful and the data was written to the channel.
    /// `Some(data)` if the channel is full and the data could not be written; the original value is
    /// returned so it can be retried without cloning.
    ///
    /// # Guarantees
    ///
    /// You can refer to the [`channel`](super) module documentation for the precise guarantees provided by this primitive.
    #[inline(always)]
    pub fn try_write(&mut self, mut data: T) -> Option<T> {
        for i in 0..C {
            data = match self.producer[Self::mask(self.next_index + i)].try_write(data) {
                Some(data) => data,
                None => {
                    self.next_index = Self::mask(self.next_index + i + 1);
                    return None;
                }
            };
        }
        Some(data)
    }

    const fn mask(index: usize) -> usize {
        index & (C - 1)
    }
}

impl<T, const N: usize, const C: usize> Clone for Producer<T, N, C> {
    fn clone(&self) -> Self {
        let mut producers = self.producers.lock().expect("mutex has been poisonned");
        let producer = producers
            .pop()
            .expect("cloned too many times, increase the number of producers");
        Self {
            producers: self.producers.clone(),
            producer,
            next_index: 0,
        }
    }
}

/// The receiver side of the channel.
///
/// A `Consumer` does not have a `.new()` procedure. This is because, to create a `Consumer`, you first
/// need to create a [`Channel`].
///
/// A `Channel` is the wrapper that holds the allocation to the shared ring-buffer. Once you have created a channel,
/// you call [`.split()`] to generate a [`Producer`] and a `Consumer` out of the channel.
///
/// Additional `Consumer` instances are obtained by [cloning](Clone) the original one. The maximum number of
/// consumers is bounded by `C`, the const generic parameter of the `Channel`.
///
/// [`.split()`]: Channel::split()
///
/// # Thread safety
///
/// An instance of `Consumer` is not meant to be shared between threads. Each thread should own its own
/// `Consumer`, obtained via [`Clone`].
pub struct Consumer<T, const N: usize, const P: usize> {
    consumers: Arc<Mutex<Vec<[SpscConsumerWrapped<T, N>; P]>>>,
    consumer: [SpscConsumerWrapped<T, N>; P],
    next_index: usize,
}

impl<T, const N: usize, const P: usize> Consumer<T, N, P> {
    /// Blocks the thread until some data can be read from the channel.
    ///
    /// The implementation for now is not optimal as we use a `spin_loop()` inside the waiting loop.
    /// This is fine if one wants to allow the CPU to make time for hyperthreading.
    /// However, it is less efficient if one is dealing with pinned
    /// and isolated threads, as it will introduce a hard execution stall that will stop the CPU from executing
    /// an instruction for up to 120 cycles on modern Intel microarchitecture (it used to be only 20 cycles).
    /// This cost can be substantial for some applications, and so it is recommended in that case to make use of
    /// [`try_read()`](Self::try_read) in a tight busy loop.
    ///
    /// # Returns
    ///
    /// A valid value read from the channel.
    ///
    /// # Guarantees
    ///
    /// You can refer to the [`channel`](super) module documentation for the precise guarantees provided by this primitive.
    #[inline(always)]
    pub fn read(&mut self) -> T {
        if P == 1 {
            return self.consumer[0].read();
        }
        let mut index = self.next_index;
        loop {
            match self.consumer[index].try_read() {
                Some(data) => {
                    self.next_index = Self::mask(index + 1);
                    return data;
                }
                None => {}
            };

            index = Self::mask(index + 1);
            spin_loop();
        }
    }

    /// Tries to read data from the channel.
    ///
    /// The operation may fail and return `None` if the underlying ring-buffer is empty. If there
    /// is an active producer, the operation can be retried until a slot in the channel is available.
    ///
    /// # Returns
    ///
    /// `Some(value)` if the operation was successful and the data could be read from the channel.
    /// `None` if the channel is empty and no data could be read.
    ///
    /// # Guarantees
    ///
    /// You can refer to the [module documentation](self) for the precise guarantees provided by this primitive.
    #[inline(always)]
    pub fn try_read(&mut self) -> Option<T> {
        for i in 0..P {
            match self.consumer[Self::mask(self.next_index + i)].try_read() {
                Some(data) => {
                    self.next_index = Self::mask(self.next_index + i + 1);
                    return Some(data);
                }
                None => {}
            };
        }
        None
    }

    const fn mask(index: usize) -> usize {
        index & (P - 1)
    }
}

impl<T, const N: usize, const P: usize> Clone for Consumer<T, N, P> {
    fn clone(&self) -> Self {
        let mut consumers = self.consumers.lock().expect("mutex has been poisonned");
        let mut consumer = consumers
            .pop()
            .expect("cloned too many times, increase the number of consumers");
        for c in consumer.iter_mut() {
            c.activate();
        }
        Self {
            consumers: self.consumers.clone(),
            consumer,
            next_index: 0,
        }
    }
}

impl<T, const N: usize, const P: usize> Drop for Consumer<T, N, P> {
    fn drop(&mut self) {
        for c in self.consumer.iter_mut() {
            c.deactivate();
        }
    }
}

/// A channel wraps all the resources required for a set of [`Producer`]s and [`Consumer`]s.
///
/// The const generic parameters are:
/// - `T`: the type of data transmitted through the channel;
/// - `N`: the size of each internal ring-buffer (must be a power of two);
/// - `P`: the maximum number of producers (must be a power of two);
/// - `C`: the maximum number of consumers (must be a power of two).
///
/// It creates `P * C` internal ring-buffers and so uses `sizeof(T) * N * P * C` bytes in memory.
///
/// To enforce safe concurrent access, the reading and writing functions are not exposed directly on
/// the `Channel`. Instead, call [`.split()`](Self::split) to obtain a [`Producer`] and a [`Consumer`].
pub struct Channel<T, const N: usize, const P: usize, const C: usize> {
    channels: [[spsc::Channel<T, N>; P]; C],
}

impl<T, const N: usize, const P: usize, const C: usize> Channel<T, N, P, C> {
    /// Creates a set of new ring buffers of size `N`.
    ///
    /// It creates `P * C` ring buffers and so uses `sizeof(T) * N * P * C` bytes in memory.
    /// The type constants have to follow some properties:
    /// - `N` must be a power of two;
    /// - `P` must be a power of two;
    /// - `C` must be a power of two.
    pub fn new() -> Self {
        const {
            assert!(
                P.is_power_of_two(),
                "the number of producers must be a power of two"
            );
            assert!(
                C.is_power_of_two(),
                "the number of consumers must be a power of two"
            );
        }
        let mut channels = [const { [const { MaybeUninit::uninit() }; P] }; C];
        for c in 0..C {
            for p in 0..P {
                channels[c][p] = MaybeUninit::new(spsc::Channel::new());
            }
        }
        Channel {
            channels: unsafe { MaybeUninit::assume_init(channels.map(|c| c.into()).into()) },
        }
    }

    /// Splits the channel into a [`Producer`] and a [`Consumer`].
    ///
    /// The returned producer and consumer can each be [cloned](Clone) up to `P - 1` and `C - 1`
    /// additional times respectively, to obtain the full pool of producers and consumers.
    pub fn split(self) -> (Producer<T, N, C>, Consumer<T, N, P>) {
        let mut setup_producers = [const { [const { MaybeUninit::uninit() }; C] }; P];
        let mut setup_consumers = [const { [const { MaybeUninit::uninit() }; P] }; C];
        for (c, channel_bucket) in self.channels.into_iter().enumerate() {
            for (p, channel) in channel_bucket.into_iter().enumerate() {
                let (producer, consumer) = channel.split();
                let is_active = Arc::new(AtomicBool::new(false));
                let producer = SpscProducerWrapped {
                    producer,
                    is_active: is_active.clone(),
                };
                let consumer = SpscConsumerWrapped {
                    consumer,
                    is_active,
                };
                setup_producers[p][c] = MaybeUninit::new(producer);
                setup_consumers[c][p] = MaybeUninit::new(consumer);
            }
        }

        let mut producers: Vec<[SpscProducerWrapped<T, N>; C]> = setup_producers
            .map(|p| unsafe { MaybeUninit::assume_init(p.into()) })
            .into();
        producers.reverse();
        let producer = producers.pop().unwrap();

        let mut consumers: Vec<[SpscConsumerWrapped<T, N>; P]> = setup_consumers
            .map(|c| unsafe { MaybeUninit::assume_init(c.into()) })
            .into();
        consumers.reverse();
        let mut consumer = consumers.pop().unwrap();
        for c in consumer.iter_mut() {
            c.activate();
        }
        (
            Producer {
                producers: Arc::new(Mutex::new(producers)),
                producer,
                next_index: 0,
            },
            Consumer {
                consumers: Arc::new(Mutex::new(consumers)),
                consumer,
                next_index: 0,
            },
        )
    }
}

/// A small wrapper to get notified when a new consumer gets online.
struct SpscProducerWrapped<T, const N: usize> {
    producer: spsc::Producer<T, N>,
    is_active: Arc<AtomicBool>,
}

impl<T, const N: usize> SpscProducerWrapped<T, N> {
    /// To use when we are sure the consumer is active.
    ///
    /// This is useful when we implement *PSC because in that situation there is only
    /// one possible active consumer, and so make the overall code faster.
    #[inline(always)]
    pub(super) fn write_no_checks(&mut self, data: T) {
        self.producer.write(data)
    }

    /// Fast forward the call to the spsc producer if the consumer is known to be active.
    #[inline(always)]
    pub(super) fn try_write(&mut self, data: T) -> Option<T> {
        if self.is_active.load(Ordering::Relaxed) {
            self.producer.try_write(data)
        } else {
            Some(data)
        }
    }
}

struct SpscConsumerWrapped<T, const N: usize> {
    pub(self) consumer: spsc::Consumer<T, N>,
    pub(self) is_active: Arc<AtomicBool>,
}

impl<T, const N: usize> SpscConsumerWrapped<T, N> {
    /// Marks the consumer as active.
    #[inline(always)]
    pub(super) fn activate(&mut self) {
        self.is_active.store(true, Ordering::Relaxed);
    }

    /// Marks the consumer as inactive.
    #[inline(always)]
    pub(super) fn deactivate(&mut self) {
        self.is_active.store(false, Ordering::Relaxed);
    }

    /// Fast forward the call to the spsc consumer.
    #[inline(always)]
    pub(super) fn read(&mut self) -> T {
        self.consumer.read()
    }

    /// Fast forward the call to the spsc consumer.
    #[inline(always)]
    pub(super) fn try_read(&mut self) -> Option<T> {
        self.consumer.try_read()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;

    #[test]
    fn create_channel() {
        let channel = Channel::<usize, 16, 4, 4>::new();
        let (mut producer, mut consumer) = channel.split();

        producer.write(0);
        producer.clone().write(1);
        producer.clone().write(2);
        producer.clone().write(3);

        assert_eq!(consumer.read(), 0);
        assert_eq!(consumer.clone().try_read(), None);
        assert_eq!(consumer.clone().try_read(), None);
        assert_eq!(consumer.clone().try_read(), None);
    }

    #[test]
    fn send_and_recv_all() {
        let channel = Channel::<usize, 16, 4, 4>::new();
        let (producer, consumer) = channel.split();

        let mut producers = vec![
            producer.clone(),
            producer.clone(),
            producer.clone(),
            producer,
        ];
        let mut consumers = vec![
            consumer.clone(),
            consumer.clone(),
            consumer.clone(),
            consumer,
        ];

        for i in 0..4 {
            for j in 0..4 {
                producers[i].write(j);
            }
        }

        for i in 0..4 {
            for _ in 0..4 {
                assert_eq!(consumers[i].read(), (i + 1) & 3);
            }
        }
    }

    #[test]
    fn missing_consumers() {
        let channel = Channel::<usize, 16, 2, 4>::new();
        let (producer, consumer) = channel.split();

        let producers = vec![producer.clone(), producer];
        // Only 2 consumers are cloned and active out of the 4 configured
        let consumers = vec![consumer.clone(), consumer];

        let stop = Arc::new(AtomicBool::new(false));

        let producer_thread: Vec<_> = producers
            .into_iter()
            .map(|mut p| {
                thread::spawn(move || {
                    for i in 1..=1_000_000 {
                        p.write(i);
                    }
                })
            })
            .collect();

        let consumer_thread: Vec<_> = consumers
            .into_iter()
            .map(|mut c| {
                let stop = stop.clone();
                thread::spawn(move || {
                    let mut sum: usize = 0;
                    while !stop.load(Ordering::Relaxed) {
                        if let Some(val) = c.try_read() {
                            sum += val;
                        }
                        spin_loop();
                    }

                    while let Some(val) = c.try_read() {
                        sum += val;
                    }

                    sum
                })
            })
            .collect();

        producer_thread.into_iter().for_each(|h| h.join().unwrap());
        stop.store(true, Ordering::Relaxed);

        let total: usize = consumer_thread
            .into_iter()
            .map(|h| {
                let val = h.join().unwrap();
                assert!(val != 0);
                val
            })
            .sum();

        let expected_per_producer = (1_000_000 * 1_000_001) / 2;
        assert_eq!(total, 2 * expected_per_producer);
    }

    macro_rules! build_test {
        ( $name:ident, $N:tt, $P:tt, $C:tt ) => {
            #[test]
            fn $name() {
                let channel = Channel::<usize, $N, $P, $C>::new();
                let (producer, consumer) = channel.split();

                let mut producers = Vec::with_capacity($P);
                for _ in 1..$P {
                    producers.push(producer.clone());
                }
                producers.push(producer);

                let mut consumers = Vec::with_capacity($P);
                for _ in 1..$C {
                    consumers.push(consumer.clone());
                }
                consumers.push(consumer);

                let stop = Arc::new(AtomicBool::new(false));

                let producer_thread: Vec<_> = producers
                    .into_iter()
                    .map(|mut p| {
                        thread::spawn(move || {
                            for i in 1..=1_000_000 {
                                p.write(i);
                            }
                        })
                    })
                    .collect();

                let consumer_thread: Vec<_> = consumers
                    .into_iter()
                    .map(|mut c| {
                        let stop = stop.clone();
                        thread::spawn(move || {
                            let mut sum: usize = 0;
                            while !stop.load(Ordering::Relaxed) {
                                if let Some(val) = c.try_read() {
                                    sum += val;
                                }
                                spin_loop();
                            }

                            while let Some(val) = c.try_read() {
                                sum += val;
                            }

                            return sum;
                        })
                    })
                    .collect();

                producer_thread.into_iter().for_each(|h| h.join().unwrap());

                stop.store(true, Ordering::Relaxed);

                assert_eq!(
                    consumer_thread
                        .into_iter()
                        .map(|h| {
                            let val = h.join().unwrap();
                            assert!(val != 0);
                            val as usize
                        })
                        .sum::<usize>(),
                    $P * (1_000_000 * 1_000_001) / 2
                );
            }
        };
    }

    // 1. Pure SPSC
    build_test!(test_spsc, 2, 1, 1);

    // 2. MPSC
    build_test!(test_mpsc, 4, 4, 1);

    // 3. SPMC
    build_test!(test_spmc, 4, 1, 4);

    // 4. Asymmetric MPMC (P != C)
    build_test!(test_mpmc_more_producers, 4, 8, 2);
    build_test!(test_mpmc_more_consumers, 4, 2, 8);

    // 5. Symmetric MPMC
    build_test!(test_mpmc_balanced, 4, 4, 4);
}
