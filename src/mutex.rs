//! Spin-lock implementation of a Rust mutex.
//!
//! It enables better performance at the cost of wasted CPU cycles.
//! If you are working with isolated and pinned threads, this is fine, but if you are working
//! in a highly multithreaded environment, you should preferably use the default implementation.

use std::cell::UnsafeCell;
use std::hint;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};

/// A mutual exclusion primitive
///
/// This primitive protects data from being accessed and modified by different threads at the same time. You can refer to
/// the [module documentation](self) for more details on the difference between this implementation and the
/// standard library implementation.
pub struct Mutex<T> {
    pub(self) lock: AtomicBool,
    pub(self) data: UnsafeCell<T>,
}

unsafe impl<T> Send for Mutex<T> {}
unsafe impl<T> Sync for Mutex<T> {}

impl<T> Mutex<T> {
    /// Creates a new mutex.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use cachec::mutex::Mutex;
    ///
    /// let mutex = Mutex::new(0);
    /// ```
    #[inline(always)]
    pub const fn new(value: T) -> Mutex<T> {
        Mutex {
            lock: AtomicBool::new(false),
            data: UnsafeCell::new(value),
        }
    }

    /// Locks the mutex to gain exclusive ownership of the underlying data.
    ///
    /// The ownership is guaranteed with an RAII implementation, which means other threads
    /// gain access to the data when the returned [`MutexGuard`] is dropped.
    ///
    /// The function blocks the thread with a spin-loop until the mutex becomes available, which
    /// might cause several undesirable effects:
    /// - fast consecutive locks might be unfair to other threads (and prevent them from ever getting access to the mutex);
    /// - calling [`Mutex::lock()`] in a thread that already holds a corresponding [`MutexGuard`] results in an infinite loop.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use cachec::mutex::Mutex;
    /// # use std::sync::Arc;
    /// # use std::thread;
    ///
    /// let mutex = Arc::new(Mutex::new(0));
    /// let mutex_c = mutex.clone();
    ///
    /// thread::spawn(move || {
    ///     *mutex.lock() = 42;
    /// }).join().expect("thread::spawn failed");
    /// assert_eq!(*mutex_c.lock(), 42);
    /// ```
    #[inline]
    pub fn lock<'a>(&'a self) -> MutexGuard<'a, T> {
        while self
            .lock
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Acquire)
            .is_err()
        {
            hint::spin_loop();
        }
        MutexGuard { mutex: self }
    }

    /// Consumes the mutex and returns its inner value.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use cachec::mutex::Mutex;
    ///
    /// let mutex = Mutex::new(0);
    /// assert_eq!(mutex.into_inner(), 0);
    /// ```
    #[inline(always)]
    pub fn into_inner(self) -> T {
        self.data.into_inner()
    }
}

/// An RAII guard to access data from a mutex.
///
/// See the [`Mutex`] documentation for more details.
#[must_use = "unused triggers the release of the Mutex"]
pub struct MutexGuard<'a, T> {
    mutex: &'a Mutex<T>,
}

impl<'a, T> Deref for MutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.mutex.data.get().as_ref_unchecked() }
    }
}

impl<'a, T> DerefMut for MutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.mutex.data.get().as_mut_unchecked() }
    }
}

impl<'a, T> Drop for MutexGuard<'a, T> {
    fn drop(&mut self) {
        self.mutex.lock.store(false, Ordering::Release)
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, thread};

    use super::*;

    #[test]
    pub fn acquire_and_modify() {
        let mutex = Mutex::new(0);
        let mut value = mutex.lock();
        *value = 0;
        drop(value);
        assert_eq!(mutex.into_inner(), 0);
    }

    #[test]
    pub fn acquire_concurrent() {
        let mutex = Mutex::new(0);
        let mutex_even = Arc::new(mutex);
        let mutex_odd = mutex_even.clone();
        let own_mutex = mutex_even.clone();

        let even_thread = thread::spawn(move || {
            for i in 1..1_000 {
                let mut value = mutex_even.lock();
                *value += i;
            }
        });

        let odd_thread = thread::spawn(move || {
            for i in 1..1_000 {
                let mut value = mutex_odd.lock();
                *value += i;
            }
        });

        even_thread.join().unwrap();
        odd_thread.join().unwrap();
        assert_eq!(Arc::into_inner(own_mutex).unwrap().into_inner(), 999_000);
    }
}
