use std::cell::RefCell;
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
        
        let new_size = max(self.buf.len(), 4);
        BufChain {
            buf: Vec::with_capacity(new_size),
            prev: Some(Box::new(self)),
        }
    }
    
    // "shrink" the chain by dropping all old vectors,
    // and setting the length of the top-level vector to
    // zero.
    // This can only be done when there are no pointers
    // to elements in the pool.
    unsafe fn shrink(&mut self) {
        self.prev.take();
        self.buf.set_len(0);
    }
}

// A pool allocator for jobs.
pub struct Pool {
    buf: RefCell<BufChain>,
}

impl Pool {
    pub fn new() -> Self {
        Pool {
            buf: RefCell::new(BufChain::new(INITIAL_CAPACITY)),
        }
    }

    // push a job onto the end of the pool and get a pointer to it.
    // this may only be called from the thread which logically owns this pool.
    pub unsafe fn alloc(&self, job: Job) -> *mut Job {
        let mut buf = self.buf.borrow_mut();
        match buf.alloc(job) {
            Ok(job_ptr) => job_ptr,
            Err(job) => {
                // grow the buffer, replacing it with a temporary while we grow it.
                let new_link = mem::replace(&mut *buf, BufChain::new(0)).grow();
                *buf = new_link;
                
                // we just grew the buffer, so we are guaranteed to have capacity.
                buf.alloc(job).ok().unwrap()
            }
        }
    }

    // cull any cached memory on the assumption that there are no existing
    // pointers to data in this pool.
    // this function must only be called when it can be proven that
    // all jobs allocated in this pool have already been run.
    pub unsafe fn cull_memory(&self) {
        self.buf.borrow_mut().shrink();
    }
}

unsafe impl Send for Pool {}
unsafe impl Sync for Pool {}
