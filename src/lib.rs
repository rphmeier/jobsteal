#![feature(fnbox)]

//! Work-stealing job-queue implementation.
//! Inspired by the Molecular Matters blog posts.

use std::borrow::Borrow;
use std::boxed::FnBox;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::mem;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Sender, Receiver, TryRecvError};

use ::rand::thread_rng;
use ::rand::distributions::{IndependentSample, Range};

extern crate rand;

/// Messages that can be sent to workers.
enum WorkerMessage {
    Stop,
}

/// A job is a unit of work to be done.
struct Job {
    func: Option<Box<FnBox(&Worker) + 'static + Send>>,
    finished: AtomicBool,
}

impl Job {
    // make a new, unrooted job.
    fn new<F: FnOnce(&Worker) + 'static + Send>(f: F) -> Job {
        Job {
            func: Some(Box::new(f)),
            finished: AtomicBool::new(false),
        }
    }

    // run this job.
    fn run(&mut self, worker: &Worker) {
        let f: Box<FnBox(&Worker) + 'static + Send> = match self.func.take() {
            Some(f) => f,
            // jobs should never be run more than once.
            None => unreachable!(),
        };

        FnBox::call_box(f, (worker,));

        self.finished.store(true, Ordering::SeqCst);
    }

    fn is_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }
}

/// A thread-safe double-ended queue for jobs.
/// Right now, it's just a dumb locking wrapper of VecDeque.
struct JobQueue {
    inner: Mutex<VecDeque<*mut Job>>,
}

impl JobQueue {
    fn new() -> Self {
        JobQueue {
            inner: Mutex::new(VecDeque::new()),
        }
    }

    // pop a job from the private end of the queue.
    // only the owner of this queue should call this.
    fn pop(&self) -> Option<*mut Job> {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };

        guard.pop_back()
    }

    // steal a job from the public end of the queu.
    fn steal(&self) -> Option<*mut Job> {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };

        guard.pop_front()
    }

    // push another job to the queue.
    fn push(&self, job: *mut Job) {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };

        guard.push_back(job);
    }
}

unsafe impl Send for JobQueue {}
unsafe impl Sync for JobQueue {}

/// A worker thread. This logically owns a queue of jobs to run, and
/// a list of other worker threads' queues to steal from.
pub struct Worker {
    queue: Arc<JobQueue>,
    siblings: Box<[Arc<JobQueue>]>,
}

impl Worker {
    /// Submit a job to be run by this worker.
    /// You must use this handle to join 
    pub fn submit<F: FnOnce(&Worker) + 'static + Send>(&self, f: F) -> JoinHandle {
        let job = Box::new(Job::new(f));
        self.give_job(&*job as *const _ as *mut Job);

        JoinHandle {
            job: job,
            worker: self,
        }
    }

    /// Create an execution scope where you can run jobs with a non-static
    /// lifetime.
    pub fn scope<'a, 'b, F>(&'a self, f: F) 
    where F: FnOnce(&Scope<'a, 'b>) {
        let scope = Scope {
            worker: self,
            children: RefCell::new(Vec::new()),
            _marker: PhantomData,
        };

        f(&scope);

        drop(scope);
    }

    fn give_job(&self, job: *mut Job) {
        self.queue.push(job);
    }

    // Get a job from this queue or another.
    fn get_job(&self) -> Option<*mut Job> {
        if let Some(job) = self.queue.pop() {
            Some(job)
        } else {
            let mut rng = thread_rng();
            // ranges are [low, high)
            let dist_range = Range::new(0, self.siblings.len());

            let other_queue: &Arc<JobQueue> = &self.siblings[dist_range.ind_sample(&mut rng)];
            if &**other_queue as *const JobQueue != &*self.queue as *const JobQueue {
                other_queue.steal()
            } else {
                None
            }
        }
    }

    fn run_job(&self) {
        if let Some(job) = self.get_job() {
            unsafe { (*job).run(self) }
        }
    }
}

/// A handle to a specific job.
/// Users can either call `wait()` to explicitly wait for the job 
/// to finish, or the job will be waited for at the end of scope.
/// This handle will not cause any memory unsafety when leaked.
#[must_use]
pub struct JoinHandle<'a> {
    job: Box<Job>,
    worker: &'a Worker,
}

impl<'a> JoinHandle<'a> {
    /// Wait for this job to be finished.
    pub fn wait(self) {
        drop(self);
    }

    fn wait_(&self) {
        // work while we wait.
        while !self.job.is_finished() {
            self.worker.run_job();
        }
    }


    // TODO: find a way to release that doesn't involve
    // leaking memory or dropping the box.
    // I suspect it'll come when I change boxing jobs to
    // allocating them from a thread-local pool.
}

impl<'a> Drop for JoinHandle<'a> {
    fn drop(&mut self) {
        self.wait_();
    }
}

/// An execution scope for jobs.
pub struct Scope<'a, 'b> {
    worker: &'a Worker,
    children: RefCell<Vec<Box<Job>>>,
    _marker: PhantomData<Cell<&'b ()>>,
}

impl<'a, 'b> Scope<'a, 'b>{
    /// Submit a job to be completed within this scope.
    pub fn submit<F>(&'a self, f: F) -> ChildHandle<'a> 
    where F: FnOnce(&Worker) + 'b + Send {
        let bounded_fn: Box<FnBox(&Worker) + 'b + Send> = Box::new(f);
        let unbounded_fn: Box<FnBox(&Worker) + 'static + Send> = unsafe { mem::transmute(bounded_fn) };

        let job = Box::new(Job {
            func: Some(unbounded_fn),
            finished: AtomicBool::new(false),
        });

        let job_ptr = &*job as *const _ as *mut Job;
        self.children.borrow_mut().push(job);

        self.worker.give_job(job_ptr);

        // this handle is safe to leak and drop:
        // the scope object owns the jobs' memory and 
        // also enforces that the jobs don't outlive it.
        ChildHandle {
            job: job_ptr,
            worker: &self.worker,
        }
    }

    fn wait_all(&self) {
        // wait until all the children are done.
        for child in self.children.borrow().iter() {
            while !child.is_finished() {
                self.worker.run_job();
            }
        }
    }
}

impl<'a, 'b> Drop for Scope<'a, 'b> {
    fn drop(&mut self) {
        self.wait_all();
    }
}

/// A handle to a job spawned within a scope. Users can manually
/// `wait()` until it's completed. They can also `release()`
/// the guard to defer its completion to an arbitrary time
/// within its lifetime.
#[must_use]
pub struct ChildHandle<'a> {
    job: *mut Job,
    worker: &'a Worker,
}

impl<'a> ChildHandle<'a> {
    /// Wait for this job to be finished.
    pub fn wait(self) {
        if self.job.is_null() { return; }

        // work while we wait.
        while unsafe { !(*self.job).is_finished() } {
            self.worker.run_job();
        }
    }

    /// Releases this job. This allows it to be finished
    /// at any point within the job's lifetime.
    pub fn release(self) {
        drop(self)
    }
}
 
pub struct JobSystem {
    handles: Vec<Sender<WorkerMessage>>,
    worker: Worker,
}

impl JobSystem {
    /// Gets a reference to the worker for this thread.
    pub fn get_worker(&self) -> &Worker {
        &self.worker
    }
}

// No unsafety is induced by not stopping the handles.
// However, the threads will spin forever, which probably
// isn't ideal.
impl Drop for JobSystem {
    fn drop(&mut self) {
        for handle in &mut self.handles {
            // at this point, either the worker receives
            // the message or it's already shut down.
            let _ = handle.send(WorkerMessage::Stop);
        }
    }
}

/// Create a new `JobSystem` with `n_threads` workers, including the
/// current thread. This will fail if `n_threads` is 0.
pub fn make_pool(n_threads: usize) -> Option<JobSystem> {
    use std::thread;

    if n_threads == 0 {
        return None;
    }

    // make a queue for every thread, including this one.
    let queues: Vec<_> = (0..n_threads)
        .map(|_| Arc::new(JobQueue::new())).collect();

    let queues = queues.into_boxed_slice();

    let mut handles = Vec::new();

    // Spawn a worker thread for every queue but the last one, 
    // since that belongs to the thread this is spawned on.
    for queue in queues.iter().cloned().take(queues.len() - 1) {
        let (tx, rx) = channel();
        let siblings = queues.clone();

        thread::spawn(move || {
            let worker = Worker {
                queue: queue,
                siblings: siblings,
            };
            worker_main(worker, rx);
        });
        handles.push(tx);
    }

    Some(JobSystem {
        handles: handles,
        worker: Worker {
            queue: queues.last().unwrap().clone(),
            siblings: queues,
        },
    })
}

fn worker_main(worker: Worker, rx: Receiver<WorkerMessage>) {
    loop {
        // check for message
        match rx.try_recv() {
            Ok(WorkerMessage::Stop) => {
                break;
            }

            Err(TryRecvError::Disconnected) => {
                break;
            }

            _ => {}
        }

        worker.run_job();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn basic_spawning() {
        // four worker threads, including this one.
        let pool = make_pool(4).unwrap();
        let this_worker = pool.get_worker();

        let mut my_jobs = Vec::new();

        for _ in 0..100 {
            let handle = this_worker.submit(|_| println!("Hello!"));
            my_jobs.push(handle);
        }
    }

    #[test]
    fn child_jobs() {
        let pool = make_pool(4).unwrap();
        let this_worker = pool.get_worker();

        this_worker.submit(|worker| {
            let mut jobs = Vec::new();
            for _ in 0..100 {
                let handle = worker.submit(|_| println!("Hello!"));
                jobs.push(handle);
            };
        }).wait();
    }

    #[test]
    fn scoping() {
        let pool = make_pool(4).unwrap();
        let this_worker = pool.get_worker();
        this_worker.submit(|worker| {
            let mut v = vec![0; 256];

            worker.scope(|scope| {
                for chunk in v.chunks_mut(32) {
                    scope.submit(move |_| {
                        for i in chunk { *i += 1 }
                    }).release();
                }
            });

            for i in v {
                assert_eq!(i, 1);
            }

        }).wait();
    }
}