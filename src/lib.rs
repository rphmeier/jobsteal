//! A work-stealing fork-join queue used to perform processors' work asynchronously.
//! This is intended to be short-lived. Long running asynchronous tasks should use another method.
//! For infinite loops, the longest-running of tasks, behavior will be as expected.
#![cfg_attr(nightly, feature(core_intrinsics))]

mod job;
mod queue;
mod pool;

use self::job::Job;
use self::pool::Pool;
use self::queue::Queue;

use std::io::Error;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

const MAX_JOBS: usize = 4096;

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
}

// TODO: implement !Sync for Worker. 

impl Worker {
    // pop a job from the worker's queue, or steal one from the queue with the most work.
    unsafe fn pop_or_steal(&self) -> Option<*mut Job> {
        if let Some(job) = self.queues[self.idx].pop() {
            return Some(job);
        }
        
        // if the queue is empty (pop failed), we can clear the pool for this queue.
        // this works only because 
        // A) we push pool-allocated jobs only onto the local queue,
        // B) pop gets priority when racing against "steal".
        self.pools[self.idx].reset();
        
        let (mut most_work, mut most_idx) = (0, None);
        for (i, queue) in self.queues.iter().enumerate() {
            let len = queue.len();
            if len > most_work {
                most_work = len;
                most_idx = Some(i);
            }
        }
        
        if let Some(idx) = most_idx {
            self.queues[idx].steal()
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
                while let Some(job) = queue.steal() {
                    all_clear = false;
                    unsafe { (*job).call(self) }
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
    
    // This must be called on the thread the worker is active on.
    unsafe fn submit_internal<F>(&self, counter: *const AtomicUsize, f: F) where F: Send + FnOnce(&Worker) {
        let job = Job::new(counter, f);
        let job_ptr = self.pools[self.idx].alloc(job);
        self.queues[self.idx].push(job_ptr);
    }
    
    // construct a new spawning scope.
    // this waits for all jobs submitted internally
    // to complete.
    fn scope<'pool, 'new, F>(&'pool self, f: F)
    where F: FnOnce(&Spawner<'pool, 'new>) {
        let counter = AtomicUsize::new(0);
        let s = make_spawner(self, &counter);
        f(&s);
        
        while counter.load(Ordering::Acquire) != 0 {
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

unsafe impl<'pool, 'scope> Sync for Spawner<'pool, 'scope> {}

impl<'pool, 'scope> Spawner<'pool, 'scope> {
    /// Execute a function which necessarily outlives the scope which
    /// this resides in.
    pub fn submit<F>(&self, f: F) 
    where F: 'scope + Send + FnOnce(&Spawner<'pool, 'scope>) {
        use std::mem;
        
        // for sending the counter pointer across thread boundaries.
        struct SendCounter(*const AtomicUsize);
        unsafe impl Send for SendCounter {}
        
        unsafe { 
            // increment the counter first just in case, somehow, this job
            // gets grabbed before we have a chance to increment it,
            // and we wait for all jobs to complete.
            let counter: SendCounter = SendCounter(self.counter);
            (*self.counter).fetch_add(1, Ordering::AcqRel);
            self.worker.submit_internal(self.counter, move |worker| {
                let SendCounter(count_ptr) = counter;
                // make a new spawner associated with the same scope,
                // but with the correct worker for the thread -- so if 
                // this job spawns any children, we won't break any 
                // invariants by accessing other workers' queues/pools
                // in unexpected ways.
                let spawner = make_spawner(worker, count_ptr);
                f(&mem::transmute(spawner) )
            });
        }
    }
    
    /// Construct a new spawning scope smaller than the one this spawner resides in.
    pub fn scope<'new, F>(&self, f: F)
    where 'scope: 'new, F: FnOnce(&Spawner<'pool, 'new>) {
        self.worker.scope(f);
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
    
    let mut state = State::Paused;
    loop {
        match state {
            State::Running => {
                match rx.recv().expect("Pool hung up on worker") {
                    ToWorker::Clear => {
                        worker.clear();
                        tx.send(ToLeader::Cleared).expect("Pool hung up on worker");
                        state = State::Paused;
                    }
                    
                    _ => unreachable!()
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
                        
                        _ => unreachable!()
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
        let queues = Arc::new((0..n+1).map(|_| Queue::new()).collect::<Vec<_>>());
        let pools = Arc::new((0..n+1).map(|_| Pool::new()).collect::<Vec<_>>());
        let mut workers = Vec::with_capacity(n);
        
        for i in 0..n {
            let (work_send, work_recv) = channel();
            let (lead_send, lead_recv) = channel();
            
            let worker = Worker {
                queues: queues.clone(),
                pools: pools.clone(),
                idx: i + 1,
            };
            
            let builder = thread::Builder::new().name(format!("worker_{}", i));
            let handle = try!(builder.spawn(|| worker_main(lead_send, work_recv, worker)));
            
            workers.push(WorkerHandle {
                tx: work_send,
                rx: lead_recv,
                thread: Some(handle),
            });
        }
        
        Ok(WorkPool {
            workers: workers,
            local_worker: Worker {
                queues: queues,
                pools: pools,
                idx: 0,
            },
            state: State::Paused,
        })
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
    where F: FnOnce(&Spawner<'pool, 'new>) {
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
    where F: 'static + Send + FnOnce(&Spawner<'a, 'static>) {
        use std::mem;
        use std::ptr;
        
        self.spin_up();
        // make a job without a counter.
        unsafe { 
                self.local_worker.submit_internal(ptr::null_mut(), move |worker| {
                let spawner = make_spawner(&worker, ptr::null_mut());
                f(&mem::transmute(spawner))          
            })
        }
    }
    
    // spin up all the workers, in case they were in a paused
    // state.
    fn spin_up(&mut self) {
        if self.state != State::Running { 
            for worker in &self.workers {
                worker.tx.send(ToWorker::Start).expect("Worker hung up on pool");
            }
            
            self.state = State::Running;
        }
    }
    
    // clear all the workers and sets the state to paused.
    fn clear_all(&mut self) {
        if self.state == State::Paused { return; }
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
                Ok(msg) => {
                    match msg {
                        ToLeader::Cleared => continue,
                        ToLeader::Panicked => {
                            panicked = true;
                        }
                    }
                }
                
                Err(_) => panic!("Worker hung up on job system.")
            }
        }
        
        // reset the counters in every queue so there's no chance of
        // overlap.
        for queue in self.local_worker.queues.iter() {
            // asserts that len == 0 in debug mode.
            queue.reset_counters();
        }
        
        // reset all the pools.
        for pool in self.local_worker.pools.iter() {
            pool.reset();    
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
    fn outlives_pool() {
        let mut v = vec![0; 256];
        let mut pool = WorkPool::new(4).unwrap();
        
        // can only execute 'static functions here -- otherwise,
        // some person might make the dumb mistake of calling `forget` the
        // pool before it has the chance to run all submitted jobs.
        pool.submit(move |_| for i in &mut v { *i += 1} );
    }
    
    #[test]
    #[should_panic]
    fn job_panic() {
        let mut pool = WorkPool::new(4).unwrap();
        
        pool.submit(|_| panic!("Eep!"));
    }
}