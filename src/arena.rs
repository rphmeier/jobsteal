use std::cell::UnsafeCell;
use std::mem;

use super::INITIAL_CAPACITY;
use super::job::Job;

// a chain of vectors where jobs will be allocated in contiguous memory.
struct BufChain {
    buf: Vec<Job>,
    prev: Option<Box<BufChain>>,
}

impl BufChain {
    fn new(size: usize) -> Self {
        BufChain {
            buf: Vec::with_capacity(size),
            prev: None,
        }
    }

    // try to allocate this job on the top-level vector.
    // returns an error if out of space.
    fn alloc(&mut self, job: Job) -> Result<*mut Job, Job> {
        let len = self.buf.len();
        if len == self.buf.capacity() {
            Err(job)
        } else {
            self.buf.push(job);
            unsafe { Ok(self.buf.get_unchecked_mut(len) as *mut Job) }
        }
    }

    // create a new buffer with double the size of this one, while preserving
    // this one in the linked list.
    fn grow(self) -> Self {
        use std::cmp::max;

        let new_size = max(self.buf.len() * 2, 4);

        BufChain {
            buf: Vec::with_capacity(new_size),
            prev: Some(Box::new(self)),
        }
    }
}

// A pool allocator for jobs.
pub struct Arena {
    buf: UnsafeCell<BufChain>,
}

impl Arena {
    pub fn new() -> Self {
        Arena {
            buf: UnsafeCell::new(BufChain::new(INITIAL_CAPACITY)),
        }
    }

    // push a job onto the end of the pool and get a pointer to it.
    // this may only be called from the thread which logically owns this arenas.
    pub unsafe fn alloc(&self, job: Job) -> *mut Job {
        let mut buf = self.buf.get();
        match (*buf).alloc(job) {
            Ok(job_ptr) => job_ptr,
            Err(job) => {
                // grow the buffer, replacing it with a temporary while we grow it.
                let new_link = mem::replace(&mut *buf, BufChain::new(0)).grow();
                *buf = new_link;

                // we just grew the buffer, so we are guaranteed to have capacity.
                (*buf).alloc(job).ok().unwrap()
            }
        }
    }

    // gets the current top of the buffer.
    // this can only be called from the thread which logically owns this arena.
    pub unsafe fn top(&self) -> usize {
        (*self.buf.get()).buf.len()
    }

    // sets the top of the buffer.
    pub unsafe fn set_top(&self, top: usize) {
        (*self.buf.get()).buf.set_len(top);
    }
}

unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}
