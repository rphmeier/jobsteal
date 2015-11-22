#![feature(catch_panic, fnbox)]

//! Work-stealing job-queue implementation.
//! Inspired by the Molecular Matters blog posts.

extern crate crossbeam;
extern crate rand;

use crossbeam::mem::CachePadded;
use crossbeam::sync::TreiberStack;

use std::any::Any;
use std::boxed::FnBox;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Sender, Receiver, TryRecvError};
use std::thread;

use ::rand::thread_rng;
use ::rand::distributions::{IndependentSample, Range};

/// Messages that can be sent to workers.
enum WorkerMessage {
    Stop,
}

type JobFn = Box<FnBox(&Worker) + 'static + Send>;

/// A job's id in the pool.
struct JobId {
    idx: usize,
}

struct Job {
    func: JobFn,
    sender: Sender<JobExitStatus>,
    scope_counter: *const AtomicUsize,
}

unsafe impl Send for Job {}

enum JobExitStatus {
    Success,
    Panic(Box<Any>),
}

/// A mostly lock-free pool allocator of jobs.
/// It only locks when doubling the size of the vector.
/// Eventually, I may implement a lock-free vector based off of
/// Stroustroup-et-al's paper on the subject, but for now, I've 
/// decided to "keep it simple, stupid".
/// However, this should be mostly wait free unless *many* jobs 
/// are being submitted.
struct JobPool {
    // we ensure with the lock-free stack of free indices that
    // an index is only used by one thread at a time.
    // mutable, disjoint borrows are therefore acceptable
    // as long as they occur within the umbrella of a read
    // lock and will not be interfered with by doubling of the
    // vector.
    jobs: RwLock<Vec<RefCell<Option<CachePadded<Job>>>>>,
    unused: TreiberStack<usize>,
}

impl JobPool {
    /// Initialize this pool with the given capacity.
    fn with_capacity(size: usize) -> Self {
        let mut v = Vec::with_capacity(size);
        let unused = TreiberStack::new();

        for i in (0..size).rev() {
            v.push(RefCell::new(None));

            unused.push(i);
        }

        JobPool {
            jobs: RwLock::new(v),
            unused: unused,
        }
    }

    /// Submit a job to be stored in the pool.
    fn submit(&self, job: JobFn) -> (JobId, Receiver<JobExitStatus>) {
        self.submit_with_counter(job, ::std::ptr::null())
    }

    /// Submit a job to be stored in the pool with a pointer to a scope counter.
    fn submit_with_counter(&self, job: JobFn, counter: *const AtomicUsize) -> (JobId, Receiver<JobExitStatus>) {
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

        let (tx, rx) = channel();

        let entry = &jobs[next];

        *entry.borrow_mut() = Some(CachePadded::new(Job {
            func: job,
            sender: tx,
            scope_counter: counter,
        }));

        (JobId { idx: next, }, rx)
    }

    // double the vector size
    fn double(&self) {
        let mut jobs = match self.jobs.write() {
            Ok(j) => j,
            Err(p) => p.into_inner(),
        };

        let old_len = jobs.len();
        let new_len = old_len * 2;

        jobs.reserve(new_len);

        for i in (old_len..new_len).rev() {
            jobs.push(RefCell::new(None));

            self.unused.push(i);
        }
    }

    /// Get the job given by id, passing the worker
    /// as a parameter.
    fn get_job(&self, id: JobId) -> Job {
        use std::ptr;

        let jobs = match self.jobs.read() {
            Ok(j) => j,
            Err(p) => p.into_inner(),
        };

        // take the job out before pushing the index back onto
        // the unused stack.
        let entry = jobs[id.idx].borrow_mut().take().unwrap();


        // CachePadded doesn't run destructors, so this is ok.
        let job = unsafe { ptr::read(&*entry) };
        self.unused.push(id.idx);
        job
    }
}

unsafe impl Send for JobPool {}
unsafe impl Sync for JobPool {}

/// A thread-safe double-ended queue for jobs.
/// Right now, it's just a dumb locking wrapper of VecDeque.
/// It stores indices of jobs in the global job pool.
struct JobQueue {
    inner: Mutex<VecDeque<JobId>>,
}

impl JobQueue {
    fn new() -> Self {
        JobQueue {
            inner: Mutex::new(VecDeque::new()),
        }
    }

    // pop a job from the private end of the queue.
    // only the owner of this queue should call this.
    fn pop(&self) -> Option<JobId> {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };

        guard.pop_back()
    }

    // steal a job from the public end of the queue.
    fn steal(&self) -> Option<JobId> {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };

        guard.pop_front()
    }

    // push another job to the queue.
    fn push(&self, job: JobId) {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };

        guard.push_back(job);
    }
}

unsafe impl Send for JobQueue {}
unsafe impl Sync for JobQueue {}

/// A worker executes jobs.
pub struct Worker {
    pool: Arc<JobPool>,
    queue: Arc<JobQueue>,
    siblings: Box<[Arc<JobQueue>]>,
}

impl Worker {
    /// Submit a job to be run.
    pub fn submit<F: FnOnce(&Worker) + 'static + Send>(&self, f: F) -> JoinHandle {
        let job = Box::new(f);
        let (id, notifier) = self.pool.submit(job);
        self.queue.push(id);

        JoinHandle {
            notifier: notifier,
            worker: Some(self),
        }
    }

    /// Create a scope to run jobs that access local stack data.
    /// This will block until child jobs are finished.
    pub fn scope<'a, 'b, F: FnOnce(&Scope<'a, 'b>)>(&'a self, f: F) {
        let s = Scope {
            counter: AtomicUsize::new(0),
            pool: &*self.pool, 
            queue: &*self.queue,
            worker: Some(self),
            _marker: PhantomData,
        };

        f(&s);

        // the move incurred by dropping s actually
        // invalidates all the pointers to its counter.
        // so this should preferably be done out of the 
        // destructor.
        s.wait_all();
    }

    fn run_next_job(&self) {
        let id = if let Some(id) = self.queue.pop() {
            Some(id)
        } else {
            let mut rng = thread_rng();

            let idx = Range::new(0, self.siblings.len()).ind_sample(&mut rng);
            let other_queue: &Arc<JobQueue> = &self.siblings[idx];

            // make sure we dn't steal from our own queue.
            if &*self.queue as *const JobQueue != &**other_queue as *const JobQueue {
                other_queue.steal()
            } else {
                None
            }
        };

        if let Some(id) = id {
            self.run_job(id);
        }
    }

    fn run_job(&self, id: JobId) {
        use std::mem;
        let job = self.pool.get_job(id);
        let Job { func, sender, scope_counter } = job;

        // is there a better way to do this than to box again?
        let unbounded_fn: Box<FnBox() + Send> = Box::new(move || func.call_box((self,)));
        let bounded_fn: Box<FnBox() + 'static + Send> = unsafe { mem::transmute(unbounded_fn) };

        let exit_status = match thread::catch_panic(|| bounded_fn()) {
            Ok(_) => { JobExitStatus::Success }
            Err(e) => { JobExitStatus::Panic(e) }
        };

        // decrement the scope counter and send the exit status
        if !scope_counter.is_null() {
            let counter: &AtomicUsize = unsafe { (&*scope_counter) };
            counter.fetch_sub(1, Ordering::SeqCst);
        }

        drop(sender.send(exit_status)); // message doesn't _have_ to arrive
    }

    fn run_to_completion(self) {
        while let Some(job) = self.queue.pop() {
            self.run_job(job);
        }
    }

}

/// A scope for submitting jobs.
pub struct Scope<'a, 'b> {
    counter: AtomicUsize,
    pool: &'a JobPool,
    queue: &'a JobQueue,
    worker: Option<&'a Worker>,
    _marker: PhantomData<RefCell<&'b ()>>,
}

impl<'a, 'b> Scope<'a, 'b> {
    pub fn submit<F: FnOnce(&Worker) + 'b + Send>(&'a self, f: F) -> JoinHandle<'a> {
        use std::mem;

        let unbounded_fn: Box<FnBox(&Worker) + 'b + Send> = Box::new(f);
        let bounded_fn: Box<FnBox(&Worker) + 'static + Send> = unsafe { mem::transmute(unbounded_fn) };

        // increment the counter before pushing the job to the
        // queue in case it gets grabbed really quickly
        let (id, notifier) = self.pool.submit_with_counter(bounded_fn, &self.counter as *const AtomicUsize);
        self.counter.fetch_add(1, Ordering::SeqCst);
        self.queue.push(id);
        JoinHandle {
            notifier: notifier,
            worker: None,
        }
    }

    fn wait_all(&self) {
        // busy wait until they're all done.
        while self.counter.load(Ordering::SeqCst) > 0 {
            if let &Some(ref worker) = &self.worker {
                worker.run_next_job();
            }
        }
    }
}

/// A JoinHandle can be used to manually wait for 
/// a job to be completed.
pub struct JoinHandle<'a> {
    notifier: Receiver<JobExitStatus>,
    worker: Option<&'a Worker>,
}

impl<'a> JoinHandle<'a> {
    pub fn wait(self) -> Result<(), Box<Any>> {
        loop {
            match self.notifier.try_recv() {
                Ok(exit_status) => { 
                    return match exit_status {
                        JobExitStatus::Success => Ok(()),
                        JobExitStatus::Panic(err) => Err(err),
                    }
                }

                // sender _never_ hangs up before the job is done.
                Err(TryRecvError::Disconnected) => unreachable!(),

                // nothing yet, just do some more work.
                _ => {
                    if let &Some(ref worker) = &self.worker {
                        worker.run_next_job()
                    }
                }
            }
        }
    }
}

pub struct JobSystem {
    handles: Vec<Sender<WorkerMessage>>,
    pool: Arc<JobPool>,
    queue: Arc<JobQueue>,
}

impl JobSystem {
    pub fn submit<F: FnOnce(&Worker) + 'static + Send>(&self, f: F) -> JoinHandle {
        let job = Box::new(f);
        let (id, notifier) = self.pool.submit(job);
        self.queue.push(id);

        JoinHandle {
            notifier: notifier,
            worker: None,
        }
    }
}

impl Drop for JobSystem {
    fn drop(&mut self) {
        for handle in &self.handles {
            match handle.send(WorkerMessage::Stop) {
                _ => {}
            }
        }
    }
}

/// Create a new `JobSystem` with `n_threads` worker threads.
/// This will fail if `n_threads` is less than 1.
pub fn make_pool(n_threads: usize) -> Option<JobSystem> {
    if n_threads < 1 {
        return None;
    }

    let job_pool = Arc::new(JobPool::with_capacity(4096));

    // make a queue for every thread, including this one.
    let queues: Vec<_> = (0..n_threads+1)
        .map(|_| Arc::new(JobQueue::new())).collect();

    let queues = queues.into_boxed_slice();
    let mut handles = Vec::new();

    // Spawn a worker thread for every queue but the last one, 
    // since that belongs to the thread this is spawned on.
    for queue in queues.iter().cloned().take(n_threads) {
        let (tx, rx) = channel();
        let siblings = queues.clone();
        let pool = job_pool.clone();

        thread::spawn(move || {
            let worker = Worker {
                pool: pool,
                queue: queue,
                siblings: siblings,
            };

            worker_main(worker, rx);
        });
        handles.push(tx);
    }

    Some(JobSystem {
        handles: handles,
        pool: job_pool,
        queue: queues.last().unwrap().clone(),
    })
}

// the main thread for each worker
fn worker_main(worker: Worker, rx: Receiver<WorkerMessage>) {
    loop {
        // check for message
        match rx.try_recv() {
            // time to stop!
            Ok(WorkerMessage::Stop) => {
                worker.run_to_completion();
                break;
            }

            // somehow the JobSystem got dropped without disconnecting.
            Err(TryRecvError::Disconnected) => {
                worker.run_to_completion();
                break;
            }

            _ => {}
        }
        worker.run_next_job();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    #[should_panic]
    fn no_zero_thread_pool() {
        make_pool(0).unwrap();
    }

    #[test]
    fn basic_spawning() {
        // four worker threads, including this one.
        let pool = make_pool(4).unwrap();

        let mut handles = Vec::new();
        for _ in 0..100 {
            // empty job
            let handle = pool.submit(|_| {});
            handles.push(handle);
        }

        for handle in handles {
            // make sure every job runs and
            // no job panics.
            handle.wait().unwrap();
        }
    }

    #[test]
    fn child_jobs() {
        let pool = make_pool(4).unwrap();

        pool.submit(|worker| {
            for _ in 0..100 {
                // empty job
                worker.submit(|_| {});
            };
        }).wait().unwrap();
    }

    #[test]
    fn panic_in_the_jobqueue() {
        let pool = make_pool(4).unwrap();

        let mut handles = Vec::new();
        for i in 0..100 {
            // 4 threads, 20 panics.
            let handle = pool.submit(move |_| match i % 5 {
                0 => panic!(i),
                _ => {}
            });

            handles.push(handle);
        }

        for handle in handles {
            match handle.wait() {
                Ok(_) => {},
                Err(b) => {
                    let panic_val = b.downcast_ref::<i32>().unwrap();
                    assert_eq!(panic_val % 5, 0);
                }
            }
        }

    }

    #[test]
    fn scoping() {
        let pool = make_pool(4).unwrap();
        pool.submit(|worker| {
            let mut v = vec![0; 256];

            worker.scope(|scope| {
                for chunk in v.chunks_mut(32) {
                    scope.submit(move |_| {
                        for i in chunk { *i += 1 }
                    });
                }
            });

            for i in v {
                assert_eq!(i, 1);
            }
        }).wait().unwrap();
    }
}