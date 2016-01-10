use std::cell::Cell;
use std::mem;
use std::ptr;

use super::MAX_JOBS;
use super::job::Job;

// A pool allocator for jobs.
pub struct Pool {
	buf: Box<[Job]>,
	cur: Cell<usize>,
	last_reset: Cell<usize>,
}

impl Pool {
	pub fn new() -> Self {        
        Pool {
            buf: (0..MAX_JOBS).map(|_| unsafe { mem::uninitialized() }).collect::<Vec<_>>().into_boxed_slice(),
            cur: Cell::new(0),
            last_reset: Cell::new(0),
        }
	}
	
	// push a job onto the end of the pool and get a pointer to it.
    // this may only be called from the thread which logically owns this pool.
	pub unsafe fn alloc(&self, job: Job) -> *mut Job {		
			let cur = self.cur.get();
			let idx = cur & (MAX_JOBS - 1);
			self.cur.set(cur + 1);			
			assert!(cur < self.last_reset.get() + MAX_JOBS, 
			"Allocated too many jobs since last reset.");
			
			let slot: *mut Job = &self.buf[idx] as *const Job as *mut _;
			ptr::write(slot, job);
			slot
	}
	
	pub fn reset(&self) {
		self.last_reset.set(self.cur.get());
	}
}

unsafe impl Send for Pool {}
unsafe impl Sync for Pool {}