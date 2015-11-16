use crossbeam::sync::TreiberStack;

use std::boxed::FnBox;
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;

use super::{Job, Worker};

/// A job's id in the pool.
#[derive(Clone)]
pub struct JobId {
    idx: usize,
    gen: usize,
}

struct PoolEntry {
    job: RefCell<Option<Job>>,
    gen: AtomicUsize,
}

/// A mostly lock-free pool allocator of jobs.
/// It only locks when doubling the size of the vector.
/// Eventually, I may implement a lock-free vector based off of
/// Stroustroup-et-al's paper on the subject, but for now, I've 
/// decided to "keep it simple, stupid".
pub struct JobPool {
    jobs: RwLock<Vec<PoolEntry>>,
    unused: TreiberStack<usize>,
}

impl JobPool {
    /// Initialize this pool with the given capacity.
    pub fn with_capacity(size: usize) -> Self {
        let mut v: Vec<PoolEntry> = Vec::with_capacity(size);
        let unused = TreiberStack::new();

        for i in (0..size).rev() {
            v.push(PoolEntry {
                job: RefCell::new(None),
                gen: AtomicUsize::new(0),
            });

            unused.push(i);
        }


        JobPool {
            jobs: RwLock::new(v),
            unused: unused,
        }
    }

    /// Submit a job to be allocated in the pool.
    pub fn submit(&self, job: Job) -> JobId {
        let next = if let Some(next) = self.unused.pop() {
            next
        } else {
            self.double();
            self.unused.pop().unwrap()
        };

        let jobs = match self.jobs.read() {
            Ok(j) => j,
            Err(p) => p.into_inner(),
        };

        let entry = &jobs[next];
        *entry.job.borrow_mut() = Some(job);

        JobId {
            idx: next,
            gen: entry.gen.load(Ordering::SeqCst),
        }
    }

    // double the vector
    fn double(&self) {
        let mut jobs = match self.jobs.write() {
            Ok(j) => j,
            Err(p) => p.into_inner(),
        };

        let old_len = jobs.len();
        let new_len = old_len * 2;

        jobs.reserve(new_len);

        for i in (old_len..new_len).rev() {
            jobs.push(PoolEntry {
                job: RefCell::new(None),
                gen: AtomicUsize::new(0),
            });

            self.unused.push(i);
        }
    }

    /// Whether the job referenced by this id has been executed.
    pub fn is_done(&self, id: &JobId) -> bool {
        let jobs = match self.jobs.read() {
            Ok(j) => j,
            Err(p) => p.into_inner(),
        };

        jobs[id.idx].gen.load(Ordering::SeqCst) == id.gen
    }

    /// Get the job given by id. This function should
    /// always yield Some if you use the id you are given.
    pub fn run(&self, id: JobId, worker: &Worker) {
        print!("Running job with id {}, gen {}: ", id.idx, id.gen);

        let jobs = match self.jobs.read() {
            Ok(j) => j,
            Err(p) => p.into_inner(),
        };

        // take the job out before pushing the index back onto
        // the unused stack.
        let entry = &jobs[id.idx];
        let maybe_job = entry.job.borrow_mut().take();

        // increment the generation only if this query was valid
        if maybe_job.is_some() {
            let runner = JobRunner {
                job: maybe_job,
                gen_ptr: &entry.gen as *const _,
            };

            runner.run_with(worker);
        }

        self.unused.push(id.idx);
    }
}

struct JobRunner {
    job: Option<Job>,
    gen_ptr: *const AtomicUsize,
}

impl JobRunner {
    fn run_with(mut self, worker: &Worker) {
        self.job.take().unwrap().call_box((worker,))
    }
}

impl Drop for JobRunner {
    fn drop(&mut self) {
        // This is done in the destructor so it is executing even if the
        // job executing panics.

        if !self.gen_ptr.is_null() {
            unsafe { (*self.gen_ptr).fetch_add(1, Ordering::SeqCst); }
        }
    }
}

unsafe impl Send for JobPool {}
unsafe impl Sync for JobPool {}