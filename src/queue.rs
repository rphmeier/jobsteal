use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{AtomicUsize, fence, Ordering};

use super::MAX_JOBS;
use super::job::Job;

const MASK: usize = MAX_JOBS - 1;

/// A double-ended job queue.
/// This is currently locking, but will be made lock-free when performance demands it.
pub struct Queue {
    buf: UnsafeCell<[*mut Job; MAX_JOBS]>,
    top: AtomicUsize,
    bottom: AtomicUsize,
}

unsafe impl Send for Queue {}
unsafe impl Sync for Queue {}

macro_rules! memory_barrier {
    () => {        
        fence(Ordering::SeqCst);
    }
}

#[cfg(nightly)]
macro_rules! compiler_barrier {
    () => {
        ::std::intrinsics::atomic_singlethreadfence()
    }
}

// do a full memory barrier on stable/beta.
#[cfg(not(nightly))]
macro_rules! compiler_barrier {
    () => {
        fence(Ordering::SeqCst);
    }
}

impl Queue {
    pub fn new() -> Self {
        Queue {
            buf: UnsafeCell::new([ptr::null_mut(); MAX_JOBS]),
            top: AtomicUsize::new(0),
            bottom: AtomicUsize::new(0),
        }
    }

    // push a job onto the private end of the queue.
    // this must be performed from the thread which logically
    // owns this queue.
    pub unsafe fn push(&self, job: *mut Job) {
        assert!(!job.is_null(), "Attempted to push null job onto queue");
        let b = self.bottom.load(Ordering::Acquire);
        (*self.buf.get())[b & MASK] = job;

        compiler_barrier!();
        self.bottom.store(b + 1, Ordering::Release);
    }

    // steal a job from the public end of the queue.
    // loops until it wins a race or the queue is empty.
    pub fn steal(&self) -> Option<*mut Job> {
        loop {
            unsafe {
                let t = self.top.load(Ordering::Acquire);

                compiler_barrier!();
                let b = self.bottom.load(Ordering::Acquire);
                if t < b {
                    // non-empty queue.
                    // may be racing against other steal/pop calls.
                    let job = (*self.buf.get())[t & MASK];
                    if self.top.compare_and_swap(t, t + 1, Ordering::AcqRel) != t {
                        // lost the race.
                        continue;
                    } else {
                        // won the race.
                        return Some(job);
                    }
                } else {
                    // empty queue.
                    return None;
                }
            }
        }
    }

    // pop a job from the private end of the queue.
    // this must be done from the thread which logically
    // owns this queue.
    pub unsafe fn pop(&self) -> Option<*mut Job> {
        let b = self.bottom.load(Ordering::Acquire).wrapping_sub(1);
        // check that bottom is not 0 before storing new b.
        if b == ::std::usize::MAX {
            return None;
        }
        self.bottom.store(b, Ordering::Release);
        memory_barrier!();

        let t = self.top.load(Ordering::Acquire);
        if t <= b {
            // non-empty queue.
            let mut job = Some((*self.buf.get())[b & MASK]);
            if t != b {
                job
            } else {
                if self.top.compare_and_swap(t, t + 1, Ordering::SeqCst) != t {
                    job = None;
                }

                self.bottom.store(t + 1, Ordering::Release);
                job
            }
        } else {
            // empty queue.
            self.bottom.store(t, Ordering::Release);
            None
        }
    }

    // get the length of this queue.
    pub fn len(&self) -> usize {
        self.bottom.load(Ordering::Acquire) - self.top.load(Ordering::Acquire)
    }

    // reset the counters, so they don't wrap around.
    pub fn reset_counters(&self) {
        debug_assert_eq!(self.len(), 0);

        self.bottom.store(0, Ordering::Release);
        self.top.store(0, Ordering::Release);
    }
}
