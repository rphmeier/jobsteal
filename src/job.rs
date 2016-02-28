use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::worker::Worker;

const JOB_SIZE: usize = 256;

// number of bytes that the vtable takes.
#[cfg(target_pointer_width = "32")]
const VTABLE_WIDTH: usize = 4;
#[cfg(target_pointer_width = "64")]
const VTABLE_WIDTH: usize = 8;

// the number of padding bytes necessary to bring vtable_width up to a u64.
const PADDING_WIDTH: usize = 8 - VTABLE_WIDTH;

// number of u64s for the job data array.
// this is: JOB_SIZE - VTABLE_WIDTH - 8 bytes for enum discriminant.
const ARR_SIZE: usize = ((JOB_SIZE - VTABLE_WIDTH) / 8) - 1;

// virtual wrapper for arbitrary function.
// the virtualization here is an acceptable overhead.
pub trait Thunk {
    unsafe fn call(&mut self, args: &Worker);
}

// a closure and child jobs counter.
pub struct ThunkImpl<F> {
    func: Option<F>,
    counter: *const AtomicUsize,
}

impl<F> Thunk for ThunkImpl<F>
    where F: Send + FnOnce(&Worker)
{
    #[inline]
    unsafe fn call(&mut self, args: &Worker) {
        let f = self.func.take().expect("Job function called twice");
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
// On construction, if the job function is small enough, we pack inside it:
//   - a function pointer and its closed environment.
//   - a counter which tracks the amount of child jobs.
//   - a vtable pointer to reconstruct a trait object at the call site.
// All of the following together must be no larger than JOB_SIZE.
// If the job function isn't small enough, we just allocate it implicitly.
pub enum Job {
    Inline(InlineJob),
    Heap(Box<Thunk>),
}

pub struct InlineJob {
    raw_data: [u64; ARR_SIZE],
    vtable: *mut (),
    _padding: [u8; PADDING_WIDTH],
}

unsafe impl Send for Job {}

impl Job {
    // create a new job with a destination to write the result to after it is called.
    // This pointer should be to a boxed memory location.
    pub fn new<'a, F>(counter: *const AtomicUsize, f: F) -> Job
        where F: 'a + Send + FnOnce(&Worker)
    {
        let code = ThunkImpl {
            func: Some(f),
            counter: counter,
        };

        // The job function is too large or requires too high an alignment.
        if mem::align_of::<ThunkImpl<F>>() > mem::align_of::<[u64; ARR_SIZE]>()
        || mem::size_of::<ThunkImpl<F>>() > ARR_SIZE * 8 {
            let static_code: Box<Thunk> = unsafe { mem::transmute(Box::new(code) as Box<Thunk + 'a>) };
            return Job::Heap(static_code);
        }

        let mut raw_data = [0u64; ARR_SIZE];
        unsafe {
            // write the closure's environment to the array.
            let code_ptr: *mut ThunkImpl<F> = &mut raw_data[0] as *mut _ as *mut _;
            ptr::write(code_ptr, code);

            // create a custom trait object which stores a relative offset,
            // and store the vtable from it. we restore the data pointer at
            // the call site.
            let fat_ptr: *mut Thunk = code_ptr;

            // don't store this trait object, just the vtable.
            // we know where the data is, and the data pointer here
            // might become obsolete if we move the Job around.
            let t_obj: TraitObject = mem::transmute(fat_ptr);

            Job::Inline(InlineJob {
                raw_data: raw_data,
                vtable: t_obj.vtable,
                _padding: [0; PADDING_WIDTH],
            })
        }
    }

    // Any unsafety that could occur when using a `Job` occurs here:
    //   - captured data escaping its lifetime
    //   - calling an invalid `Job` structure
    //   - calling a job with the wrong worker.
    // users of this method (me!) must be careful to avoid such cases.
    pub unsafe fn call(&mut self, args: &Worker) {
        match self {
            &mut Job::Inline(ref mut job) => {
                let t_obj = TraitObject {
                    data: &mut job.raw_data[0] as *mut _ as *mut (),
                    vtable: job.vtable,
                };

                let code_ptr: *mut Thunk = mem::transmute(t_obj);
                (*code_ptr).call(args);
            }

            &mut Job::Heap(ref mut thunk) => {
                thunk.call(args);
            }
        }
    }
}