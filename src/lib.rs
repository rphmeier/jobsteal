#![feature(fnbox)]

//! Work-stealing job-queue implementation.
//! Inspired by the Molecular Matters blog posts.

extern crate crossbeam;
extern crate rand;

mod job_pool;

use std::borrow::Borrow;
use std::boxed::FnBox;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Sender, Receiver, TryRecvError};
use std::thread;

use job_pool::{JobId, JobPool};

use ::rand::thread_rng;
use ::rand::distributions::{IndependentSample, Range};

/// Messages that can be sent to workers.
enum WorkerMessage {
    Stop,
}

type Job = Box<FnBox(&Worker) + 'static + Send>;

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

    // steal a job from the public end of the queu.
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
    fn submit<F: FnOnce(&Worker) + 'static + Send>(&self, f: F) -> JoinHandle {
        let job = Box::new(f);
        let id = self.pool.submit(job);
        self.queue.push(id.clone());

        JoinHandle {
            id: id,
            worker: self,
        }
    }

    fn next_job(&self) -> Option<JobId> {
        if let Some(id) = self.queue.pop() {
            Some(id)
        } else {
            let mut rng = thread_rng();
            let idx = Range::new(0, self.siblings.len()).ind_sample(&mut rng);
            let other_queue: &Arc<JobQueue> = &self.siblings[idx];

            // make sure we don't steal from our own queue.
            if &*self.queue as *const JobQueue != &**other_queue as *const JobQueue {
                other_queue.steal()
            } else {
                None
            }
        }
    }

    fn run_job(&self) {
        if let Some(id) = self.next_job() {
            self.pool.run(id, self);
        }
    }
}

/// A JoinHandle can be used to manually wait for 
/// a job to be completed.
pub struct JoinHandle<'a> {
    id: JobId,
    worker: &'a Worker,
}

impl<'a> JoinHandle<'a> {
    fn wait(self) {
        while self.worker.pool.is_done(&self.id) {
            self.worker.run_job();
        }
    }
}

pub struct JobSystem {
    handles: Vec<Sender<WorkerMessage>>,
    worker: Worker,
}

impl JobSystem {
    fn get_worker(&self) -> &Worker {
        &self.worker
    }
}

impl Drop for JobSystem {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.send(WorkerMessage::Stop);
        }
    }
}

/// Create a new `JobSystem` with `n_threads` workers, including the
/// current thread. This will fail if `n_threads` is 0.
pub fn make_pool(n_threads: usize) -> Option<JobSystem> {
    if n_threads == 0 {
        return None;
    }

    let job_pool = Arc::new(JobPool::with_capacity(4096));

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
        worker: Worker {
            pool: job_pool,
            queue: queues.last().unwrap().clone(),
            siblings: queues,
        },
    })
}

// the main thread for each worker
fn worker_main(worker: Worker, rx: Receiver<WorkerMessage>) {
    loop {
        // check for message
        match rx.try_recv() {
            // time to stop!
            Ok(WorkerMessage::Stop) => {
                break;
            }

            // somehow the JobSystem got dropped without disconnecting.
            Err(TryRecvError::Disconnected) => {
                break;
            }

            _ => {}
        }

        // run a job
        worker.run_job();
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

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

/*    #[test]
    fn scoping() {
        let pool = make_pool(4).unwrap();
        let this_worker = pool.get_worker();
        this_worker.submit(|worker| {
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
        }).wait();
    }*/
}