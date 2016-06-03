use std::cell::{Cell, UnsafeCell};
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{Spawner, make_spawner};
use super::arena::Arena;
use super::job::Job;
use super::queue::{Queue, Popped, Stolen};
use super::rand::{Rng, XorShiftRng};

// we use the 32nd bit from the right as an exit flag.
const EXIT_FLAG: usize = 1 << 31;

// data shared between workers.
pub struct SharedWorkerData {
    // all the workers' queues.
    queues: Vec<Queue>,
    // all the workers' arenas.
    arenas: Vec<Arena>,

    with_work: Mutex<usize>,
    wait_condvar: Condvar,
}

impl SharedWorkerData {
    pub fn new(queues: Vec<Queue>, arenas: Vec<Arena>) -> Self {
        SharedWorkerData {
            queues: queues,
            arenas: arenas,
            with_work: Mutex::new(0),
            wait_condvar: Condvar::new(),
        }
    }

    // notify all workers that it is time to exit, so no
    // more waiting around.
    pub fn notify_shutdown(&self) {
        *self.with_work.lock().unwrap() |= EXIT_FLAG;
    }

    // let other workers know that some work exists.
    // this is for workers which were previously out of work
    fn worker_has_work(&self) {
        let mut with_work = self.with_work.lock().unwrap();
        *with_work += 1;

        self.wait_condvar.notify_all();
    }

    // notify workers that there is some new work.
    // this is for workers which already had even more work.
    fn more_work_available(&self) {
        self.wait_condvar.notify_all();
    }

    // let other threads know that one less thread has work.
    fn out_of_work(&self) {
        let mut with_work = self.with_work.lock().unwrap();
        // we need to be careful not to clobber the flag.
        if *with_work | EXIT_FLAG == 0 {
             *with_work -= 1;
        }
    }

    // wait until work is available or it's time to exit.
    // returns false if the exit flag has been set.
    fn wait(&self, idx: usize) -> bool {
        assert!(idx != 0);

        let mut guard = self.with_work.lock().unwrap();
        let mut with_work = *guard;

        while with_work == 0 {
            guard = self.wait_condvar.wait(guard).unwrap();
            with_work = *guard;
        }

        drop(guard);

        if with_work & EXIT_FLAG != 0 {
            false
        } else {
            true
        }
    }
}

// Each worker lives on a specific thread and manages a job queue and allocator.
// When attempting to run jobs, it will first look in its own queue and then
// attempt to steal from its siblings.
pub struct Worker {
    shared_data: Arc<SharedWorkerData>,
    idx: usize, // the index of this worker's queue and pool in the Vec.
    rng: UnsafeCell<XorShiftRng>,
    // whether this specific worker has any work.
    has_work: Cell<bool>,

    // whether it's time to exit.
    exit_time: Cell<bool>,
}

impl Worker {
    pub fn new(shared_data: Arc<SharedWorkerData>, idx: usize, rng: XorShiftRng)
    -> Self {
        Worker {
            shared_data: shared_data,
            idx: idx,
            rng: UnsafeCell::new(rng),
            has_work: Cell::new(false),
            exit_time: Cell::new(false),
        }
    }

    // try to steal a job from another worker's queue.
    unsafe fn steal(&self) -> Option<*mut Job> {
        const ABORTS_BEFORE_BACKOFF: usize = 32;

        let idx = (*self.rng.get()).gen::<usize>() % self.queues().len();

        if idx != self.idx {
            let mut aborts = 0;
            loop {
                aborts += 1;
                if aborts > ABORTS_BEFORE_BACKOFF {
                    return None;
                }

                match self.queues()[idx].steal() {
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
        for (idx, queue) in self.queues().iter().enumerate() {
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
                loop {
                    match unsafe { queue.pop() } {
                        Popped::Success(job) => {
                            all_clear = false;
                            unsafe { (*job).call(self) }
                        }
                        Popped::Empty => break,
                        Popped::Abort => {
                            all_clear = false;
                        }
                    }
                }
            }
        }

        all_clear
    }

    pub fn clear(&self) {
        while !self.clear_pass() {}
    }

    // run the next job.
    // if we are busy-waiting for a scope to end, don't
    // wait for work to become available -- all workers running
    // out of work is a success condition.
    pub unsafe fn run_next(&self, should_wait: bool) {
        // if we might have work, try and pop it off.
        if self.has_work.get() {
            match self.queues()[self.idx].pop() {
                Popped::Success(job) => {
                    (*job).call(self)
                }

                Popped::Empty if should_wait => {
                    // we're out of work.
                    // let everyone know.
                    self.has_work.set(false);
                    self.shared_data.out_of_work();
                }

                // if it aborted, we can't say we're empty.
                _ => {}
            }
        }

        // wait for work
        if should_wait && !self.exit_time.get() {
            if !self.shared_data.wait(self.idx) {
                self.exit_time.set(true);
                // time to shut down.
                return;
            }
        }

        if let Some(job) = self.steal() {
            (*job).call(self);
        }
    }

    // This must be called on the thread the worker is assigned to.
    pub unsafe fn submit_internal<F>(&self, counter: *const AtomicUsize, f: F)
        where F: Send + FnOnce(&Worker)
    {
        if !self.has_work.get() {
            self.has_work.set(true);
            // let other threads know we have work.
            self.shared_data.worker_has_work();
        } else {
            self.shared_data.more_work_available();
        }

        let job = Job::new(counter, f);
        let job_ptr = self.arenas()[self.idx].alloc(job);
        self.queues()[self.idx].push(job_ptr);
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
        let top = unsafe { self.arenas()[self.idx].top() };

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
            unsafe { self.run_next(false) }
        }

        ::std::mem::forget(guard);

        // once all jobs have completed from the scope, we can reset the arena
        // to its previous state. This only holds true if:
        //   - the arena is only used from this thread. this is already supposed to be true.
        //   - spawners cannot be cloned. If we could clone spawners, using one within another's
        //     scope could lead to jobs being overwritten.
        unsafe { self.arenas()[self.idx].set_top(top); }

        res
    }

    // get the shared data between workers.
    pub fn shared_data(&self) -> &SharedWorkerData {
        &self.shared_data
    }

    // Whether this worker should initiate shutdown procedure.
    pub fn should_shutdown(&self) -> bool {
        self.exit_time.get()
    }

    // gets a slice of all the workers' queues.
    #[inline]
    pub fn queues(&self) -> &[Queue] {
        &self.shared_data.queues
    }

    // gets a slice of all the workers' arenas.
    #[inline]
    pub fn arenas(&self) -> &[Arena] {
        &self.shared_data.arenas
    }
}