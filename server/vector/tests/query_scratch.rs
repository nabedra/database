//! The memory a direction-code query is charged before it is prepared (`Codec::query_cost`, task
//! B 2019cc23) covers what preparing it holds at its peak, on a thread that never ran a rotation
//! before and so allocates its rotation scratch then. Its own binary: it counts the allocations of
//! one thread with a global allocator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use nabedra_vector::index::{Codec, DirectionCodec};

struct Counting;

thread_local! {
    /// Bytes this thread holds, and the most it held since the last reset.
    static HELD: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
}

fn track(grown: usize, shrunk: usize) {
    let _ = HELD.try_with(|h| {
        let (now, peak) = h.get();
        let now = now.saturating_add(grown).saturating_sub(shrunk);
        h.set((now, peak.max(now)));
    });
}

// SAFETY: every call goes to `System` with the caller's arguments; the counting touches only a
// thread-local cell that needs no allocation.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        track(layout.size(), 0);
        // SAFETY: the caller's contract for `alloc`.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        track(0, layout.size());
        // SAFETY: the caller's contract for `dealloc`.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        track(new_size, layout.size());
        // SAFETY: the caller's contract for `realloc`.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static COUNTING: Counting = Counting;

/// On a new thread: the bytes held at the peak of rotating a query and preparing its code, above
/// what the thread held before, and what `query_cost` charges for it.
fn peak_and_charge(dim: usize) -> (usize, u64) {
    let codec = DirectionCodec::new(dim, 7).unwrap();
    let dir: Vec<f32> = (0..dim).map(|i| if i == 0 { 1.0 } else { 0.0 }).collect();
    // Rotated here, so that the thread below rotates for the first time in the query.
    let centroid = codec.rotate(&dir);
    let prepare = move || {
        let charge = codec.query_cost(dim).1;
        let (before, _) = HELD.with(Cell::get);
        HELD.with(|h| h.set((before, before)));
        let query = codec.query(&codec.rotate(&dir), &centroid);
        let (_, peak) = HELD.with(Cell::get);
        drop(query);
        (peak - before, charge)
    };
    // A fresh thread, so that the rotation's scratch is not there yet.
    nabedra_rt::task::spawn_service(format!("cold-{dim}"), prepare).unwrap().join().unwrap()
}

/// Padded lengths that are powers of two rotate in place; the others (192, 768, 1536) deal their
/// values through a scratch of the padded length, which a cold thread allocates during the query.
#[test]
fn vs17_the_query_charge_covers_a_cold_thread_preparing_a_direction_query() {
    for dim in [1usize, 64, 100, 128, 192, 768, 1024, 1536, 3000] {
        let (peak, charge) = peak_and_charge(dim);
        println!("dim {dim}: peak {peak} bytes, charged {charge}");
        assert!(peak as u64 <= charge, "dim {dim}: preparing a query held {peak} bytes, charged {charge}");
    }
}

/// The measure itself: a thread that already rotated holds less at the peak than a cold one, by
/// the scratch of the padded length. Without this, a warm measure would pass any charge.
#[test]
fn vs17_a_cold_thread_holds_the_rotation_scratch_at_its_peak() {
    let (cold, _) = peak_and_charge(192);
    let codec = DirectionCodec::new(192, 7).unwrap();
    let dir: Vec<f32> = (0..192).map(|i| if i == 0 { 1.0 } else { 0.0 }).collect();
    let centroid = codec.rotate(&dir);
    let warm = move || {
        let _ = codec.rotate(&dir);
        let (before, _) = HELD.with(Cell::get);
        HELD.with(|h| h.set((before, before)));
        let query = codec.query(&codec.rotate(&dir), &centroid);
        let (_, peak) = HELD.with(Cell::get);
        drop(query);
        peak - before
    };
    let warm = nabedra_rt::task::spawn_service("warm", warm).unwrap().join().unwrap();
    assert!(cold >= warm + 192 * size_of::<f32>(), "cold {cold}, warm {warm}");
}
