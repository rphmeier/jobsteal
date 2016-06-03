//! Lock-free, automatically resizing queue.
//! Implementation based on http://www.di.ens.fr/~zappa/readings/ppopp13.pdf

use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicIsize, AtomicPtr, fence};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst};

use super::INITIAL_CAPACITY;
use super::job::Job;

/// Data storage for the job queue.
struct Array<T> {
    size: usize,
    ptr: *mut T,
    prev: Option<Box<Array<T>>>, // linked list of previous arrays.
}

impl<T> Array<T> {
    // create a new array. requires that the given size is a power-of-two.
    unsafe fn new(size: usize) -> Self {
        debug_assert!(size.is_power_of_two());

        let mut v = Vec::<T>::with_capacity(size);
        let ptr = v.as_mut_ptr();
        mem::forget(v);
        Array {
            size: size,
            ptr: ptr,
            prev: None,
        }
    }

    unsafe fn put(&self, off: isize, t: T) {
        let mask = self.mask();
        ptr::write(self.ptr.offset(off & mask), t);
    }

    unsafe fn get(&self, off: isize) -> T {
        let mask = self.mask();
        ptr::read(self.ptr.offset(off & mask))
    }

    // consume this array and allocate a new one with double the size.
    // we keep the old arrays around until the top-level one is dropped,
    // just in case threads load old versions of the array pointer.
    // because of this, the memory required to store N items converges to 2*sizeof(N)
    // as N -> inf.
    unsafe fn grow(self: Box<Self>, b: isize, t: isize) -> Box<Self> {
        // allocate a new array with double the size.
        let mut bigger = Box::new(Array::new(self.size * 2));

        // copy over all the elements in use of the previous array.
        let mut i = t;
        while i != b {
            bigger.put(i, self.get(i));
            i = i.wrapping_add(1);
        }

        // store the current array in the linked list of live arrays.
        bigger.prev = Some(self);
        bigger
    }

    #[inline]
    fn mask(&self) -> isize {
        self.size as isize - 1
    }
}

impl<T> Drop for Array<T> {
    fn drop(&mut self) {
        unsafe {
            drop(Vec::from_raw_parts(self.ptr, 0, self.size));
        }
    }
}

/// The result of a steal operation.
pub enum Stolen {
    /// Aborted stealing for some spurious reason.
    /// Retrying may yield success.
    Abort,
    /// Empty queue.
    Empty,
    /// Returned upon success.
    Success(*mut Job),
}

/// The result of a pop operation.
pub enum Popped {
    /// Aborted popping because we lost a race condition.
    Abort,
    /// Empty queue.
    Empty,
    /// Successful pop.
    Success(*mut Job),
}

/// A double-ended job queue.
pub struct Queue {
    buf: AtomicPtr<Array<*mut Job>>,
    top: AtomicIsize,
    bottom: AtomicIsize,
}

unsafe impl Send for Queue {}
unsafe impl Sync for Queue {}

impl Queue {
    pub fn new() -> Self {
        let buf = unsafe { Box::into_raw(Box::new(Array::new(INITIAL_CAPACITY))) };
        Queue {
            buf: AtomicPtr::new(buf),
            top: AtomicIsize::new(0),
            bottom: AtomicIsize::new(0),
        }
    }

    // push a job onto the private end of the queue.
    // this must be performed from the thread which logically
    // owns this queue.
    pub unsafe fn push(&self, job: *mut Job) {
        assert!(!job.is_null(), "Attempted to push null job onto queue");

        let b = self.bottom.load(Relaxed);
        let t = self.top.load(Acquire);
        let mut a = self.buf.load(Relaxed);

        // need to grow?
        if b.wrapping_sub(t) >= (*a).size as isize {
            a = Box::into_raw(Box::from_raw(a).grow(b, t));
            self.buf.store(a, Release);
        }

        (*a).put(b, job);
        fence(Release);
        self.bottom.store(b.wrapping_add(1), Relaxed);
    }

    // steal a job from the public end of the queue.
    pub fn steal(&self) -> Stolen {
        let t = self.top.load(Acquire);
        fence(SeqCst); // top must be loaded before bottom.
        let b = self.bottom.load(Acquire);

        let len = b.wrapping_sub(t);
        if len > 0 {
            // non-empty queue
            let a = self.buf.load(Acquire);
            let x = unsafe { (*a).get(t) };

            // may be racing against other steals + a pop.
            // see if we won.
            if self.top.compare_and_swap(t, t.wrapping_add(1), SeqCst) == t {
                // yep, we won.
                Stolen::Success(x)
            } else {
                // lost the race.
                mem::forget(x);
                Stolen::Abort
            }
        } else {
            // empty queue.
            Stolen::Empty
        }
    }

    // pop a job from the private end of the queue.
    // this must be done from the thread which logically
    // owns this queue.
    pub unsafe fn pop(&self) -> Popped {
        {
            // try to perform an early return.
            let t = self.top.load(Relaxed);
            let b = self.bottom.load(Relaxed);
            let len = b.wrapping_sub(t);
            if len <= 0 {
                return Popped::Empty;
            }
        }

        let b = self.bottom.load(Relaxed).wrapping_sub(1);
        let a = self.buf.load(Relaxed);
        self.bottom.store(b, Relaxed);
        fence(SeqCst); // the store to bottom must occur before the load of top.
        let t = self.top.load(Relaxed);

        let len = b.wrapping_sub(t);
        if len >= 0 {
            // non-empty.
            let x = (*a).get(b);
            if len == 0 {
                // last element in the queue.
                // see if we're racing against anything.
                if self.top.compare_and_swap(t, t.wrapping_add(1), SeqCst) != t {
                    // lost. forget we ever read a value.
                    mem::forget(x);
                    // set the queue to a cononically empty state before
                    // we return.
                    self.bottom.store(b.wrapping_add(1), Relaxed);
                    return Popped::Abort;
                }

                // set the queue to a canonically empty state of t == b.
                self.bottom.store(b.wrapping_add(1), Relaxed);
            }
            Popped::Success(x)
        } else {
            // empty queue.
            self.bottom.store(b.wrapping_add(1), Relaxed);
            Popped::Empty
        }
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        let b = self.bottom.load(SeqCst);
        let t = self.top.load(SeqCst);

        let a = unsafe { Box::from_raw(self.buf.load(SeqCst)) };

        // to handle the corner case where b has wrapped but t hasn't yet.
        // no point really in dropping *mut Jobs, but this is future proofing for
        // if the type of the queue items is changed.
        let mut i = t;
        while i != b {
            unsafe { drop(a.get(i)) }
            i = i.wrapping_add(1);
        }
    }
}