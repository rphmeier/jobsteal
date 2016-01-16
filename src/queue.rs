use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicIsize, AtomicPtr, fence};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst};

use super::MAX_JOBS;
use super::job::Job;

/// Implementation based on http://www.di.ens.fr/~zappa/readings/ppopp13.pdf

/// Data storage for the job queue.
struct Array<T> {
    size: usize,
    ptr: *mut T,
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
            ptr: ptr
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
pub enum Stolen<T> {
    /// Aborted stealing for some spurious reason.
    /// Retrying may yield success.
    Abort,
    /// Empty queue.
    Empty,
    /// Returned upon success.
    Success(T),
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
        let buf = unsafe { Box::into_raw(Box::new(Array::new(MAX_JOBS))) };
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
        let a = self.buf.load(Relaxed);
        
        if b.wrapping_sub(t) == (*a).size as isize {
            // buffer is full. should resize here.
            panic!("Allocated too many jobs since last reset");
        }
        
        (*a).put(b, job);
        fence(Release);
        self.bottom.store(b.wrapping_add(1), Relaxed);
    }

    // steal a job from the public end of the queue.
    pub fn steal(&self) -> Stolen<*mut Job> {
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
    pub unsafe fn pop(&self) -> Option<*mut Job> {
        if self.len() == 0 { return None }
        
        let b = self.bottom.load(Relaxed).wrapping_sub(1);
        let a = self.buf.load(Relaxed);
        self.bottom.store(b, Relaxed);
        fence(SeqCst); // the store to bottom must occur before the load of top.
        let t = self.top.load(Relaxed);
        
        let len = b.wrapping_sub(t);
        if len >= 0 {
            // non-empty.
            let mut x = Some((*a).get(b));
            if len == 0 {
                // last element in the queue.
                // see if we're racing against anything.
                if self.top.compare_and_swap(t, t.wrapping_add(1), SeqCst) != t {
                    // lost. forget we ever read a value.
                    mem::forget(x.take());
                }
                
                // set the queue to a canonically empty state of t == b.
                self.bottom.store(b.wrapping_add(1), Relaxed);
            }
            x
        } else {
            // empty queue.
            None
        }
    }

    // get the length of this queue.
    #[inline]
    pub fn len(&self) -> usize {
        let t = self.top.load(Relaxed);
        let b = self.bottom.load(Relaxed);
        ::std::cmp::max(0, b.wrapping_sub(t)) as usize
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        let b = self.bottom.load(Relaxed);
        let t = self.top.load(Relaxed);
        
        let a = unsafe { Box::from_raw(self.buf.load(Relaxed)) };
        let len = b.wrapping_sub(t);
        
        // to handle the corner case where b has wrapped but t hasn't yet.
        // no point really in dropping *mut Jobs, but this is future proofing for
        // if the type of the queue items is changed.
        for i in (0..len).map(|off| t.wrapping_add(off)) {
            unsafe { 
                drop(a.get(i));
            }
        }
    }
}