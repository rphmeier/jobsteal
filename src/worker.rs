use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{Spawner, make_spawner};
use super::arena::Arena;
use super::job::Job;
use super::queue::{Queue, Stolen};
use super::rand::{Rng, XorShiftRng};

// Each worker lives on a specific thread and manages a job queue and allocator.
// When attempting to run jobs, it will first look in its own queue and then
// attempt to steal from its siblings.
pub struct Worker {
    queues: Arc<Vec<Queue>>,
    arenas: Arc<Vec<Arena>>,
    idx: usize, // the index of this worker's queue and pool in the Vec.
    rng: UnsafeCell<XorShiftRng>,
}

impl Worker {
    pub fn new(queues: Arc<Vec<Queue>>, arenas: Arc<Vec<Arena>>, idx: usize, rng: XorShiftRng)
    -> Self {
        Worker {
            queues: queues,
            arenas: arenas,
            idx: idx,
            rng: UnsafeCell::new(rng),
        }
    }
    // pop a job from the worker's queue, or steal one from another queue.
    unsafe fn pop_or_steal(&self) -> Option<*mut Job> {
        const ABORTS_BEFORE_BACKOFF: usize = 32;

        if let Some(job) = self.queues[self.idx].pop() {
            return Some(job);
        }

        let idx = (*self.rng.get()).gen::<usize>() % self.queues.len();

        if idx != self.idx {
            let mut aborts = 0;
            loop {
                aborts += 1;
                if aborts > ABORTS_BEFORE_BACKOFF {
                    return None;
                }

                match self.queues[idx].steal() {
                    Stolen::Success(job) => return Some(job),
                    Stolen::Empty => return None,
                    _ => {}
                }
            }
        } else {
            None
        }
    }

    // do a sweep through all the queues, saying whether all
    // were observed to be empty initially.
    fn clear_pass(&self) -> bool {
        let mut all_clear = true;
        for (idx, queue) in self.queues.iter().enumerate() {
            if idx != self.idx {
                loop {
                    match queue.steal() {
                        Stolen::Success(job) => {
                            all_clear = false;
                            unsafe { (*job).call(self) }
                        }
                        Stolen::Empty => break,
                        Stolen::Abort => {
                            all_clear = false;
                        }
                    }
                }
            } else {
                while let Some(job) = unsafe { queue.pop() } {
                    all_clear = false;
                    unsafe { (*job).call(self) }
                }
            }
        }

        all_clear
    }

    pub fn clear(&self) {
        while !self.clear_pass() {}
    }

    // run the next job.
    pub unsafe fn run_next(&self) {
        if let Some(job) = self.pop_or_steal() {
            (*job).call(self)
        }
    }

    // This must be called on the thread the worker is assigned to.
    pub unsafe fn submit_internal<F>(&self, counter: *const AtomicUsize, f: F)
        where F: Send + FnOnce(&Worker)
    {
        let job = Job::new(counter, f);
        let job_ptr = self.arenas[self.idx].alloc(job);
        self.queues[self.idx].push(job_ptr);
    }

    // construct a new spawning scope.
    // this waits for all jobs submitted internally to complete.
    pub fn scope<'pool, 'new, F, R>(&'pool self, f: F) -> R
        where F: 'new + FnOnce(&Spawner<'pool, 'new>) -> R,
              R: 'new,

    {
        let counter = AtomicUsize::new(0);
        let s = make_spawner(self, &counter);

        // store the top of the job arena before the scope is run.
        let top = self.arenas[self.idx].top();

        let res = f(&s);

        struct PanicGuard<'a>(&'a AtomicUsize);
        impl<'a> Drop for PanicGuard<'a> {
            fn drop(&mut self) {
                while self.0.load(Ordering::Acquire) > 0 {
                    ::std::thread::yield_now()
                }
            }
        }

        // if this thread panics while running a job, we need
        // to wait for other threads to finish the scoped jobs
        // before unwinding. this could result in a deadlock in cases
        // where many functions are panicking and no other threads exist
        // to finish jobs.
        let guard = PanicGuard(&counter);

        loop {
            let status = counter.load(Ordering::Acquire);
            if status == 0 { break }
            unsafe { self.run_next() }
        }

        ::std::mem::forget(guard);

        // once all jobs have completed from the scope, we can reset the arena
        // to its previous state. This only holds true if:
        //   - the arena is only used from this thread. this is already supposed to be true.
        //   - spawners cannot be cloned. If we could clone spawners, using one within another's
        //     scope could lead to jobs being overwritten.
        unsafe { self.arenas[self.idx].set_top(top); }

        res
    }

    // gets a slice of all the workers' queues.
    pub fn queues(&self) -> &[Queue] {
        &self.queues
    }

    // gets a slice of all the workers' arenas.
    pub fn arenas(&self) -> &[Arena] {
        &self.arenas
    }
}