//! Single Producer Single Consumer channel.
//!
//! An SPSC channel is a one-way communication pipe between two threads. The messages are written to a fixed-size
//! ring buffer by the producer and read by the consumer.
//!
//! Most of the time, the solution for a concurrency problem will be to use a mutex. A mutex provides
//! a simple synchronisation primitive: it ensures a thread cannot acquire the mutex if another thread already
//! holds it. Using this, you can safely read and write on shared slices of memory.
//!
//! However, this is not the fastest mechanism to implement an SPSC channel. If you try to write an SPSC channel
//! using a mutex, you would have the following situation occurring:
//! - Thread A is writing to slot 1 and holds the mutex;
//! - Thread B wants to read slot 0, and is waiting for the mutex.
//!
//! It would be much better if B could read slot 0 while A is writing to slot 1.
//!
//! To achieve this, a solution is to use a lock-free data structure. A lock-free data structure is a structure
//! that does not make use of a global locking mechanism as described above, but rather locks data more locally.
//! This implementation is a lock-free SPSC channel.
//! By leveraging cache line alignment and atomics, we can reduce the push-to-pop latency drastically for high-volume, low-latency
//! workloads.

use std::array;
use std::cell::UnsafeCell;
use std::hint::spin_loop;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// The sender side of the channel.
///
/// A `Producer` does not have a `.new()` procedure. This is because, to create a `Producer`, you first
/// need to create a `Channel`.
///
/// A `Channel` is the wrapper that holds the allocation to the shared ring-buffer. Once you have created a channel,
/// you call [`.split()`] to generate a `Producer` and a `Consumer` out of the channel.
///
/// [`.split()`]: `Channel::split()`
///
/// # Thread safety
///
/// Although an SPSC channel is thread safe by design, an instance of `Producer` is not. It cannot be safely
/// shared between threads. If you need multiple instances of `Producer`, you should look at the primitives
/// `mpsc` and `mpmc`.
pub struct Producer<T, const N: usize> {
    channel: Arc<Channel<T, N>>,
}

unsafe impl<T, const N: usize> Sync for Producer<T, N> {}
unsafe impl<T, const N: usize> Send for Producer<T, N> {}

impl<T, const N: usize> Producer<T, N> {
    /// Tries to write data to the channel.
    ///
    /// The operation may fail and return `Some(data)` - where `data` is the value provided as argument -
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
    /// You can refer to the [`channel`](crate::channel) module documentation for the precise guarantees provided by this primitive.
    #[inline(always)]
    pub fn try_write(&mut self, data: T) -> Option<T> {
        self.channel.try_write(data)
    }

    /// Blocks the thread until some data can be written to the channel.
    ///
    /// The implementation for now is not optimal as we use a `spin_loop()` inside the waiting loop.
    /// This is fine if one wants to allow the CPU to make time for hyperthreading.
    /// However, it is less efficient if one is dealing with pinned
    /// and isolated threads, as it will introduce a hard execution stall that will stop the CPU from executing
    /// an instruction for up to 120 cycles on modern Intel microarchitecture (it used to be only 20 cycles).
    /// This cost can be substantial for some applications, and so it is recommended in that case to make use of
    /// `try_write()` in a tight busy loop.
    ///
    /// # Guarantees
    ///
    /// You can refer to the [`channel`](crate::channel) module documentation for the precise guarantees provided by this primitive.
    #[inline(always)]
    pub fn write(&mut self, data: T) {
        let mut written = Some(data);
        while written.is_some() {
            written = self.try_write(written.unwrap());
            spin_loop();
        }
    }
}

/// The receiver side of the channel.
///
/// A `Consumer` does not have a `.new()` procedure. This is because, to create a `Consumer`, you first
/// need to create a `Channel`.
///
/// A `Channel` is the wrapper that holds the allocation to the shared ring-buffer. Once you have created a channel,
/// you call [`.split()`] to generate a `Producer` and a `Consumer` out of the channel.
///
/// [`.split()`]: `Channel::split()`
///
/// # Thread safety
///
/// Although an SPSC channel is thread safe by design, an instance of `Consumer` is not. It cannot be safely
/// shared between threads. If you need multiple instances of `Consumer`, you should look at the primitives
/// `spmc` and `mpmc`.
pub struct Consumer<T, const N: usize> {
    channel: Arc<Channel<T, N>>,
}

unsafe impl<T, const N: usize> Sync for Consumer<T, N> {}
unsafe impl<T, const N: usize> Send for Consumer<T, N> {}

impl<T, const N: usize> Consumer<T, N> {
    /// Tries to read data from the channel.
    ///
    /// The operation may fail and return `None` if the underlying ring-buffer is empty. If there
    /// is an active producer, the operation can be retried until a slot in the channel is available.
    ///
    /// # Returns
    ///
    /// `Some(value)` if the operation was successful and the data could be read from the channel.
    ///
    /// # Guarantees
    ///
    /// You can refer to the [`channel`](crate::channel) module documentation for the precise guarantees provided by this primitive.
    #[inline(always)]
    pub fn try_read(&mut self) -> Option<T> {
        self.channel.try_read()
    }

    /// Blocks the thread until some data can be read from the channel.
    ///
    /// The implementation for now is not optimal as we use a `spin_loop()` inside the waiting loop.
    /// This is fine if one wants to allow the CPU to make time for hyperthreading.
    /// However, it is less efficient if one is dealing with pinned
    /// and isolated threads, as it will introduce a hard execution stall that will stop the CPU from executing
    /// an instruction for up to 120 cycles on modern Intel microarchitecture (it used to be only 20 cycles).
    /// This cost can be substantial for some applications, and so it is recommended in that case to make use of
    /// `try_read()` in a tight busy loop.
    ///
    /// # Returns
    ///
    /// A valid value read from the channel.
    ///
    /// # Guarantees
    ///
    /// You can refer to the [`channel`](crate::channel) module documentation for the precise guarantees provided by this primitive.
    #[inline(always)]
    pub fn read(&mut self) -> T {
        let mut value = None;
        while value.is_none() {
            value = self.try_read();
            spin_loop();
        }
        value.unwrap()
    }
}

/// An index in the channel ring-buffer.
///
/// We introduce an intermediate `Index` structure to align thread-local values and prevent false sharing.
#[repr(align(128))]
struct Index {
    inner: UnsafeCell<usize>,
}

impl Index {
    pub fn incr(&self) {
        unsafe {
            let inner = self.inner.get();
            *inner += 1;
        }
    }

    pub fn as_usize(&self) -> usize {
        unsafe {
            let inner = self.inner.get();
            *inner
        }
    }
}

impl Into<usize> for Index {
    fn into(self) -> usize {
        self.inner.into_inner()
    }
}

impl From<usize> for Index {
    fn from(value: usize) -> Self {
        Self {
            inner: UnsafeCell::new(value),
        }
    }
}

/// A channel wraps all the resources required for a `Producer` and a `Consumer`.
///
/// `N` represents the size of the channel and should be a static value.
///
/// There should be exactly two references to the channel in a process at any given time.
/// One reference is allowed to write, and the other is allowed to read. Any other use of this structure will result in
/// undefined behaviour.
///
/// To enforce this behaviour, we do not expose the reading and writing functions directly, but force the use of
/// the `Producer` and `Consumer` interface by calling [`.split()`].
///
/// [`.split()`]: `Channel::split()`
#[repr(align(128))]
pub struct Channel<T, const N: usize> {
    buffer: Box<[Slot<T> ; N]>,
    head: Index,
    tail: Index,
}

impl<T, const N: usize> Channel<T, N> {
    /// Creates a new ring buffer.
    pub fn new() -> Self {
        const {
            assert!(
                N.is_power_of_two(),
                "the channel size should be a power of two"
            );
        };

        let buffer = Box::new(array::from_fn(|_| Slot::new()));

        Channel {
            buffer,
            head: 0.into(),
            tail: 0.into(),
        }
    }

    /// Splits the channel into a `Producer` and a `Consumer`.
    ///
    /// This enforces the requirement that at most one producer and one consumer access the channel
    /// concurrently.
    pub fn split(self) -> (Producer<T, N>, Consumer<T, N>) {
        let channel = Arc::new(self);
        (
            Producer {
                channel: channel.clone(),
            },
            Consumer { channel },
        )
    }

    /// Returns the size of the ring-buffer.
    #[inline(always)]
    pub const fn size(&self) -> usize {
        N
    }

    #[inline(always)]
    const fn mask(&self, index: usize) -> usize {
        index & (self.size() - 1)
    }

    #[inline(always)]
    pub(self) fn try_write(&self, data: T) -> Option<T> {
        let clean_index = self.mask(self.head.as_usize());
        let is_written = self.buffer[clean_index].try_write(data);
        if is_written.is_none() {
            self.head.incr();
        }
        is_written
    }

    #[inline(always)]
    pub(self) fn try_read(&self) -> Option<T> {
        let clean_index = self.mask(self.tail.as_usize());
        let read_val = self.buffer[clean_index].try_read();
        if read_val.is_some() {
            self.tail.incr()
        }
        read_val
    }
}

/// A slot in the ring buffer. It holds a data point and an epoch counter.
///
/// The epoch counter is used to check for data availability (instead of global head/tail atomic pointers).
/// In general, it provides better performance for two reasons:
/// - In the two-pointer setup, even if the reader is far from the writer, accessing the head/tail atomic would
///   invalidate the cache of the specific pointer. Having local epoch counters reduces contention by simply looking
///   at the next slot to check availability.
/// - It loads information about availability at the same time as the data itself, which provides slightly better
///   performance.
///
/// Usually, slots in the ring buffer are aligned to the cache line of the CPU cache, 64 bytes. However, modern
/// Intel CPUs have a mechanism that automatically pulls cache lines in pairs, so aligning to 128 bytes by default
/// avoids false sharing.
#[repr(align(128))]
struct Slot<T> {
    epoch: AtomicU64,
    data: UnsafeCell<Option<T>>,
}

impl<T> Slot<T> {
    pub fn new() -> Slot<T> {
        Slot {
            epoch: 0.into(),
            data: UnsafeCell::new(None),
        }
    }

    /// Try to write `data` in the slot.
    ///
    /// If the operation is not possible, it returns `Some(data)` to allow for a busy loop.
    pub fn try_write(&self, data: T) -> Option<T> {
        let epoch = self.epoch.load(Ordering::Acquire);
        // We are only allowed to write on cells with an even epoch
        if (epoch & 1) == 1 {
            return Some(data);
        }

        // Here we are allowed to write
        unsafe {
            let own_data = self.data.get();
            *own_data = Some(data);
        }

        // Release slot
        self.epoch.store(epoch + 1, Ordering::Release);

        None
    }

    pub fn try_read(&self) -> Option<T> {
        let epoch = self.epoch.load(Ordering::Acquire);
        // We are only allowed to read from cells with an odd epoch
        if (epoch & 1) == 0 {
            return None;
        }

        // Here we are allowed to read
        let data = unsafe {
            let own_data = self.data.get();
            (*own_data).take()
        };

        // Release slot
        self.epoch.store(epoch + 1, Ordering::Release);

        data
    }
}

#[cfg(test)]
mod test {
    pub use super::*;

    #[test]
    fn slot_write_then_read() {
        let slot = Slot::new();
        assert_eq!(slot.try_write(1), None);
        assert_eq!(slot.try_read(), Some(1));
    }

    #[test]
    fn slot_double_write() {
        let slot = Slot::new();
        assert_eq!(slot.try_write(1), None);
        assert_eq!(slot.try_write(2), Some(2));
        assert_eq!(slot.try_read(), Some(1));
    }

    #[test]
    fn slot_double_read() {
        let slot = Slot::new();
        assert_eq!(slot.try_write(1), None);
        assert_eq!(slot.try_read(), Some(1));
        assert_eq!(slot.try_read(), None);
    }

    #[test]
    fn channel_size_is_power_two() {
        assert_eq!(Channel::<bool, 4>::new().size(), 4);
    }

    #[test]
    fn channel_empty_try_read() {
        let channel = Channel::<u64, 16>::new();
        let (_, mut consumer) = channel.split();
        assert_eq!(consumer.try_read(), None);
    }

    #[test]
    fn channel_empty_try_write() {
        let channel = Channel::<u64, 16>::new();
        let (mut producer, _) = channel.split();
        assert_eq!(producer.try_write(1), None);
    }

    #[test]
    fn channel_write_and_read_sync() {
        let channel = Channel::<u64, 16>::new();
        let (mut producer, mut consumer) = channel.split();
        assert_eq!(producer.try_write(1), None);
        assert_eq!(consumer.try_read(), Some(1));
    }

    use std::thread;

    #[test]
    fn channel_write_and_read_async() {
        let channel = Channel::<u64, 1024>::new();
        let (mut producer, mut consumer) = channel.split();

        let producer_thread = thread::spawn(move || {
            for i in 0..1_000_000 {
                while producer.try_write(i).is_some() {}
            }
        });

        let consumer_thread = thread::spawn(move || {
            for i in 0..1_000_000 {
                let mut data = None;
                while data.is_none() {
                    data = consumer.try_read();
                }
                assert_eq!(data, Some(i));
            }
        });

        producer_thread.join().unwrap();
        consumer_thread.join().unwrap();
    }
}
