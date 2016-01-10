use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::Worker;

const JOB_SIZE: usize = 256;
const ARR_SIZE: usize = (JOB_SIZE / 8) - 1;

// virtual wrapper for arbitrary function.
// the virtualization here is an acceptable overhead.
trait Code {
	unsafe fn call(&mut self, args: &Worker);	
}

// a closure and child jobs counter.
struct CodeImpl<F> {
	func: F,
    counter: *const AtomicUsize,
}

impl<F> Code for CodeImpl<F> where F: Send + FnOnce(&Worker) {
	#[inline]
	unsafe fn call(&mut self, args: &Worker) {
        let f = mem::replace(&mut self.func, mem::uninitialized());
		f(args);
        if !self.counter.is_null() {
            (*self.counter).fetch_sub(1, Ordering::Release);
        }
	}
}

// stub implementation of TraitObject
#[repr(C)]
struct TraitObject {
    data: *mut (),
    vtable: *mut (),
}

// A job is some raw binary data at least the size of a cache line.
// On construction, we pack inside it:
//   - a function pointer and its closed environment.
//   - a counter which tracks the amount of child jobs.
//   - a vtable pointer to reconstruct a trait object at the call site.
// All of the following together must be no larger than JOB_SIZE.
pub struct Job {
	raw_data: [u64; ARR_SIZE], 
	vtable: *mut (),
}

unsafe impl Send for Job {}

impl Job {
	// create a new job with a destination to write the result to after it is called.
	// This pointer should be to a boxed memory location.
	pub fn new<F>(counter: *const AtomicUsize, f: F) -> Job 
    where F: Send + FnOnce(&Worker) {
		assert!(mem::align_of::<CodeImpl<F>>() <= mem::align_of::<[u64; ARR_SIZE]>(), 
			"Function alignment requirement is too high.");
			
		assert!(mem::size_of::<CodeImpl<F>>() <= ARR_SIZE * 8, 
			"Function environment too large.");
		
		let mut raw_data = [0u64; ARR_SIZE];
		let code = CodeImpl {
			func: f,
            counter: counter,
		};
		
		unsafe {
			// write the closure's environment to the array.
			let code_ptr: *mut CodeImpl<F> = &mut raw_data[0] as *mut _ as *mut _;
			ptr::write(code_ptr, code);
			
			// create a custom trait object which stores a relative offset,
			// and store the vtable from it. we restore the data pointer at
			// the call site.
			let fat_ptr: *mut Code = code_ptr;
			
			// don't store this trait object, just the vtable.
			// we know where the data is, and the data pointer here
			// might become obsolete if we move the Job around.
			let t_obj: TraitObject = mem::transmute(fat_ptr);
			
			Job {
				raw_data: raw_data,
				vtable: t_obj.vtable,
			}
		}
	}
	
	// Any unsafety that could occur when using a `Job` occurs here:
	//   - captured data escaping its lifetime
	//   - calling an invalid `Job` structure
    //   - calling a job with the wrong worker.
	// users of this method (me!) must be careful to avoid such cases. 
	pub unsafe fn call(&mut self, args: &Worker) {
		let t_obj = TraitObject {
			data: &self.raw_data[0] as *const _ as *mut (),
			vtable: self.vtable,
		};
		
		let code_ptr: *mut Code = mem::transmute(t_obj);
		(*code_ptr).call(args);
	}
}