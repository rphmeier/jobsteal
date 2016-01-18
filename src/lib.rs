//! A work-stealing fork-join queue used to perform processors' work asynchronously.
//! This is intended to be short-lived. Long running asynchronous tasks should use another method.
//! For infinite loops, the longest-running of tasks, behavior will be as expected.
#![cfg_attr(nightly, feature(core_intrinsics))]

extern crate rand;

mod job;
mod queue;
mod pool;

use rand::{Rng, SeedableRng, thread_rng, XorShiftRng};

use self::job::Job;
use self::pool::Pool;
use self::queue::{Queue, Stolen};

use std::cell::UnsafeCell;
use std::io::Error;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

const INITIAL_CAPACITY: usize = 256;

// messages to workers.
enum ToWorker {
    Start, // start
    Clear, // run all jobs to completion, then reset the pool.
    Shutdown, // all workers are clear and no jobs are running. safe to tear down pools.
}
// messages to the leader from workers.
enum ToLeader {
    Cleared, // done clearing. will wait for start or shutdown
    Panicked,
}

struct WorkerHandle {
    tx: Sender<ToWorker>,
    rx: Receiver<ToLeader>,
    thread: Option<thread::JoinHandle<()>>,
}

// Finite state machine for controlling workers.
#[derive(Clone, Copy, PartialEq)]
enum State {
    Running, // running jobs normally. waiting for state change.
    Paused, // paused, waiting for start or shutdown message.
}

struct Worker {
    queues: Arc<Vec<Queue>>,
    pools: Arc<Vec<Pool>>,
    idx: usize, // the index of this worker's queue and pool in the Vec.
    rng: UnsafeCell<XorShiftRng>,
}

impl Worker {
    // pop a job from the worker's queue, or steal one from the queue with the most work.
    unsafe fn pop_or_steal(&self) -> Option<*mut Job> { 
        const ABORTS_BEFORE_BACKOFF: usize = 64;     
             
        if let Some(job) = self.queues[self.idx].pop() {
            return Some(job);
        }
        
        // since pop failed, that means that the queue is guaranteed to be empty --
        // does this mean we can cull the cached arrays from the queue?

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

    fn clear(&self) {
        while !self.clear_pass() {}
    }

    // run the next job.
    unsafe fn run_next(&self) {
        if let Some(job) = self.pop_or_steal() {
            (*job).call(self)
        }
    }

    // This must be called on the thread the worker is assigned to.
    unsafe fn submit_internal<F>(&self, counter: *const AtomicUsize, f: F)
        where F: Send + FnOnce(&Worker)
    {
        let job = Job::new(counter, f);
        let job_ptr = self.pools[self.idx].alloc(job);
        self.queues[self.idx].push(job_ptr);
    }

    // construct a new spawning scope.
    // this waits for all jobs submitted internally
    // to complete.
    fn scope<'pool, 'new, F>(&'pool self, f: F)
        where F: FnOnce(&Spawner<'pool, 'new>)
    {
        let counter = AtomicUsize::new(0);
        let s = make_spawner(self, &counter);
        f(&s);
        
        loop {
            let status = counter.load(Ordering::Acquire);
            if status == 0 { break }
            unsafe { self.run_next() }
        }
    }
}

/// A job spawner associated with a specific scope.
/// Jobs spawned using this may spawn new jobs with the same lifetime.
pub struct Spawner<'pool, 'scope> {
    worker: &'pool Worker,
    counter: *const AtomicUsize,
    // invariant lifetime.
    _marker: PhantomData<*mut &'scope mut ()>,
}

impl<'pool, 'scope> Spawner<'pool, 'scope> {
    /// Execute a function which necessarily outlives the scope which
    /// this resides in.
    pub fn submit<F>(&self, f: F)
        where F: 'scope + Send + FnOnce(&Spawner<'pool, 'scope>)
    {
        use std::mem;

        // for sending the counter pointer across thread boundaries.
        struct SendCounter(*const AtomicUsize);
        unsafe impl Send for SendCounter {}

        unsafe {
            // increment the counter first just in case, somehow, this job
            // gets grabbed before we have a chance to increment it,
            // and we wait for all jobs to complete.
            let counter: SendCounter = SendCounter(self.counter);
            if !self.counter.is_null() {
                (*self.counter).fetch_add(1, Ordering::AcqRel);   
            }
            self.worker.submit_internal(self.counter, move |worker| {
                let SendCounter(count_ptr) = counter;
                // make a new spawner associated with the same scope,
                // but with the correct worker for the thread -- so if
                // this job spawns any children, we won't break any
                // invariants by accessing other workers' queues/pools
                // in unexpected ways.
                let spawner = make_spawner(worker, count_ptr);
                f(&mem::transmute(spawner))
            });
        }
    }

    /// Construct a new spawning scope smaller than the one this spawner resides in.
    pub fn scope<'new, F>(&self, f: F)
        where 'scope: 'new,
              F: FnOnce(&Spawner<'pool, 'new>)
    {
        self.worker.scope(f);
    }
    
    /// Execute two closures, possibly asynchronously, and return their results.
    /// This will block until they are both complete.
    #[allow(non_camel_case_types)]
    pub fn join<A, B, R_A, R_B>(&self, oper_a: A, oper_b: B) -> (R_A, R_B)
    where A: Send + FnOnce(&Spawner) -> R_A,
          B: Send + FnOnce(&Spawner) -> R_B,
          R_A: Send,
          R_B: Send 
    {
        let mut a = None;
        let mut b = None;
        self.scope(|spawner| {
            spawner.submit(|s| a = Some(oper_a(s)));
            spawner.submit(|s| b = Some(oper_b(s)));
        });
        (a.unwrap(), b.unwrap())
    }
}
fn make_spawner<'a, 'b>(worker: &'a Worker, counter: *const AtomicUsize) -> Spawner<'a, 'b> {
    Spawner {
        worker: worker,
        counter: counter,
        _marker: PhantomData,
    }
}

fn worker_main(tx: Sender<ToLeader>, rx: Receiver<ToWorker>, worker: Worker) {
    struct PanicGuard(Sender<ToLeader>);
    impl Drop for PanicGuard {
        fn drop(&mut self) {
            if thread::panicking() {
                let _ = self.0.send(ToLeader::Panicked);
            }
        }
    }
    // if the worker for this thread panics,
    let _guard = PanicGuard(tx.clone());
    
    match rx.recv() {
        Ok(ToWorker::Start) => {},
        _ => unreachable!(),
    };

    let mut state = State::Running;
    loop {
        match state {
            State::Running => {
                match rx.try_recv() {
                    Ok(ToWorker::Clear) => {
                        worker.clear();
                        tx.send(ToLeader::Cleared).expect("Pool hung up on worker");
                        state = State::Paused;
                    }
                    Ok(_) => unreachable!(),
                    Err(TryRecvError::Disconnected) => panic!("Pool hung up on worker"),
                    _ => {}
                }

                unsafe { worker.run_next() }
            }

            State::Paused => {
                if let Ok(msg) = rx.recv() {
                    match msg {
                        ToWorker::Start => {
                            state = State::Running;
                            continue;
                        }

                        ToWorker::Shutdown => {
                            break;
                        }

                        _ => unreachable!(),
                    }
                } else {
                    panic!("Pool hung up on worker");
                }
            }
        }
    }
}

/// The work pool manages worker threads in a 
/// work-stealing fork-join thread pool.
// The fields here are put in Vecs for cache contiguity.
pub struct WorkPool {
    workers: Vec<WorkerHandle>,
    local_worker: Worker,
    state: State,
}

impl WorkPool {
    /// Creates a work pool with `n` worker threads.
    pub fn new(n: usize) -> Result<Self, Error> {
        // one extra queue and pool for the job system.
        let queues = Arc::new((0..n + 1).map(|_| Queue::new()).collect::<Vec<_>>());
        let pools = Arc::new((0..n + 1).map(|_| Pool::new()).collect::<Vec<_>>());
        let mut workers = Vec::with_capacity(n);

        let mut rng = thread_rng();
        for i in 0..n {
            let (work_send, work_recv) = channel();
            let (lead_send, lead_recv) = channel();

            let worker = Worker {
                queues: queues.clone(),
                pools: pools.clone(),
                idx: i + 1,
                rng: UnsafeCell::new(XorShiftRng::from_seed(rng.gen::<[u32; 4]>())),
            };

            let builder = thread::Builder::new().name(format!("worker_{}", i));
            let handle = try!(builder.spawn(move || worker_main(lead_send, work_recv, worker)));

            workers.push(WorkerHandle {
                tx: work_send,
                rx: lead_recv,
                thread: Some(handle),
            });
        }

        let mut pool = WorkPool {
            workers: workers,
            local_worker: Worker {
                queues: queues,
                pools: pools,
                idx: 0,
                rng: UnsafeCell::new(XorShiftRng::from_seed(rng.gen::<[u32; 4]>())),
            },
            state: State::Paused,
        };
        
        pool.spin_up();
        
        Ok(pool)
    }

    /// Finish all current jobs which are queued, and synchronize the workers until
    /// it's time to start again. 
    ///
    /// If any of the threads have panicked, that panic
    /// will be propagated to this thread. Until the next job is submitted,
    /// workers will block, allowing other threads to have higher priority.
    pub fn synchronize(&mut self) {
        self.clear_all();
    }

    /// Create a new spawning scope for submitting jobs.
    /// Any jobs submitted in this scope will be completed
    /// by the end of this function call.
    pub fn scope<'pool, 'new, F>(&'pool mut self, f: F)
        where F: FnOnce(&Spawner<'pool, 'new>)
    {
        self.spin_up();
        self.local_worker.scope(f);
    }

    /// Execute a job which strictly owns its contents.
    /// The execution of this function may be deferred to beyond
    /// this function call.
    /// In the general case, it is safe to submit jobs which only
    /// strictly outlive the pool. However, there is always the
    /// chance that the pool could be leaked, violating the invariant
    /// that the job is completed before the borrow of the data is.
    pub fn submit<'a, F>(&'a mut self, f: F)
        where F: 'static + Send + FnOnce(&Spawner<'a, 'static>)
    {
        self.spawner().submit(f);
    }

    /// Spin up all the workers, in case they were in a paused
    /// state.
    pub fn spin_up(&mut self) {
        if self.state != State::Running {
            for worker in &self.workers {
                worker.tx.send(ToWorker::Start).expect("Worker hung up on pool");
            }

            self.state = State::Running;
        }
    }
    
    /// Get a reference to the local spawner.
    /// This can only be used to spawn static jobs.
    pub fn spawner<'a>(&'a mut self) -> Spawner<'a, 'static> {
        use std::ptr;
        
        self.spin_up();
        make_spawner(&self.local_worker, ptr::null_mut())
    }

    // clear all the workers and sets the state to paused.
    fn clear_all(&mut self) {
        if self.state == State::Paused {
            return;
        }
        // send every worker a clear message
        for worker in &self.workers {
            worker.tx.send(ToWorker::Clear).ok().expect("Worker hung up on pool.");
        }

        // do a clear run on the local worker as well.
        self.local_worker.clear();

        let mut panicked = false;
        // wait for confirmation from each worker.
        // the queues are not guaranteed to be empty until the last one responds!
        for worker in &self.workers {
            match worker.rx.recv() {
                Ok(ToLeader::Panicked) => {
                    panicked = true;
                }

                Err(_) => panic!("Worker hung up on job system."),
                _ => {}
            }
        }

        // cull all cached memory in the pools and queues.
        // at this point, we have received notice from all workers
        // that they have stopped (either willingly or by panic),
        // and that all jobs have been run.
        for pool in self.local_worker.pools.iter() {
            unsafe { pool.cull_memory(); }
        }
        
        for queue in self.local_worker.queues.iter() {
            unsafe { queue.cull_memory(); }
        }

        // cleared and panicked workers are waiting for a shutdown message.
        if panicked {
            panic!("Propagating worker thread panic")
        }


        self.state = State::Paused;
    }
}

impl Drop for WorkPool {
    fn drop(&mut self) {
        // finish all work.
        self.clear_all();

        // now tell every worker they can shut down safely.
        for worker in &self.workers {
            worker.tx.send(ToWorker::Shutdown).ok().expect("Worker hung up on job system");
        }

        // join the threads (although they should already be at this point).
        for worker in &mut self.workers {
            worker.thread.take().map(|handle| handle.join());
        }
    }
}

/// Create a pool with `n` worker threads.
pub fn make_pool(n: usize) -> Result<WorkPool, Error> {
    WorkPool::new(n)
}

#[cfg(test)]
mod tests {
    use super::WorkPool;

    #[test]
    fn creation_destruction() {
        for i in 0..32 {
            let _ = WorkPool::new(i).unwrap();
        }
    }

    #[test]
    fn split_work() {
        let mut pool = WorkPool::new(4).unwrap();
        let mut v = vec![0; 1024];
        pool.scope(|spawner| {
            for (idx, v) in v.iter_mut().enumerate() {
                spawner.submit(move |_| {
                    *v += idx;
                });
            }
        });

        assert_eq!(v, (0..1024).collect::<Vec<_>>());
    }

    #[test]
    fn multilevel_scoping() {
        let mut pool = WorkPool::new(4).unwrap();
        pool.scope(|spawner| {
            let mut v = vec![0; 256];
            spawner.scope(|s| {
                for i in &mut v {
                    s.submit(move |_| *i += 1)
                }
            });
            // job is forcibly joined here.

            assert_eq!(v, (0..256).map(|_| 1).collect::<Vec<_>>())
        }); // any other jobs would be forcibly joined here.
    }

    #[test]
    fn multiple_synchronizations() {
        let mut pool = WorkPool::new(4).unwrap();
        for _ in 0..100 {
            pool.synchronize();
        }
    }
    
    #[test]
    fn join() {
        let mut pool = WorkPool::new(4).unwrap();
        let (mut a, mut b) = (0, 0);
        pool.spawner().join(|_| a += 1, |_| b += 1);
        assert_eq!(a, 1);
        assert_eq!(b, 1);
    }

    #[test]
    fn outlives_pool() {
        let mut v = vec![0; 256];
        let mut pool = WorkPool::new(4).unwrap();

        // can only execute 'static functions here -- otherwise,
        // some person might make the dumb mistake of calling `forget` the
        // pool before it has the chance to run all submitted jobs.
        pool.submit(move |_| {
            for i in &mut v {
                *i += 1
            }
        });
    }

    #[test]
    #[should_panic]
    fn job_panic() {
        let mut pool = WorkPool::new(4).unwrap();

        pool.submit(|_| panic!("Eep!"));
    }
}
