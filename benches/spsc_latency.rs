use cachec::spsc::Channel;

use hdrhistogram::Histogram;
use core_affinity;

use std::hint::spin_loop;
use std::thread;
use std::arch::x86_64::{_mm_lfence, _rdtsc};

fn main() {
    let channel = Channel::<u64>::new(2048);
    let (mut producer, mut consumer) = channel.split();
    let core_ids = core_affinity::get_core_ids().unwrap();
    let mut histogram: Histogram<u64> = Histogram::new_with_bounds(1, 1 << 23, 1).unwrap();

    let core1 = core_ids[3];
    let core2 = core_ids[2];

    let consumer_thread = thread::spawn(move || {
        let _ = core_affinity::set_for_current(core1);

        for i in 0..1_000_000 {
            let mut sent_timestamp = None;
            while sent_timestamp.is_none() {
                sent_timestamp = consumer.try_read();
            }
            let recv_timestamp = unsafe { _mm_lfence(); _rdtsc() };
            let elapsed = recv_timestamp - sent_timestamp.unwrap();

            if i > 10_000 {
                histogram.record(elapsed).unwrap();
            }
        }

        histogram
    });

    let producer_thread = thread::spawn(move || {
        let _ = core_affinity::set_for_current(core2);

        for _ in 0..1_000_000 {
            let timestamp = unsafe { _mm_lfence(); _rdtsc() };
            while producer.try_write(timestamp).is_some() {}
            spin_loop();
        }
    });

    let histogram = consumer_thread.join().unwrap();
    producer_thread.join().unwrap();

    println!("P99: {}", histogram.value_at_quantile(0.99));
    println!("P90: {}", histogram.value_at_quantile(0.90));
}