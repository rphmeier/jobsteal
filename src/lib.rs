//! A work-stealing fork-join queue used to perform processors' work asynchronously.
//! This is intended to be short-lived. Long running asynchronous tasks should use another method.
//! For infinite loops, the longest-running of tasks, behavior will be as expected.
extern crate rand;

mod arena;
mod job;
mod queue;

use rand::{Rng, SeedableRng, thread_rng, XorShiftRng};

use self::job::Job;
use self::arena::Arena;
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
    Shutdown, // all workers are clear and no jobs are running. safe to tear down arenas and queues.
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

// FIXME: This really shouldn't need to be public, but the compiler complains about it being used
// in a "public" function signature within the `job` module. I don't agree, but I don't have any other
// recourse than to complain about it in this comment :)
#[doc(hidden)]
pub struct Worker {
    queues: Arc<Vec<Queue>>,
    arenas: Arc<Vec<Arena>>,
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
        let job_ptr = self.arenas[self.idx].alloc(job);
        self.queues[self.idx].push(job_ptr);
    }

    // construct a new spawning scope.
    // this waits for all jobs submitted internally to complete.
    fn scope<'pool, 'new, F, R>(&'pool self, f: F) -> R
        where F: 'new + FnOnce(&Spawner<'pool, 'new>) -> R,
              R: 'new,

    {
        let counter = AtomicUsize::new(0);
        let s = make_spawner(self, &counter);
        let res = f(&s);

        loop {
            let status = counter.load(Ordering::Acquire);
            if status == 0 { break }
            unsafe { self.run_next() }
        }

        res
    }
}

/// A job spawner associated with a specific scope.
///
/// Jobs spawned using this must outlive the scope.
/// The `scope` function can be used to create a more focused spawner.
pub struct Spawner<'pool, 'scope> {
    worker: &'pool Worker,
    counter: *const AtomicUsize,
    // invariant lifetime.
    _marker: PhantomData<*mut &'scope mut ()>,
}

impl<'pool, 'scope> Spawner<'pool, 'scope> {
    /// Submit a job to be executed by the thread pool and given access to the thread pool.
    ///
    /// The job's contents must outlive the spawner's scope. If they don't,
    /// you can create a more focused scope by calling the `scope` function on the spawner,
    /// and then submitting the job.
    ///
    /// Jobs are passed a handle to the spawner, so work can be split recursively.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use jobsteal::make_pool;
    ///
    /// let mut pool = make_pool(2).unwrap();
    ///
    /// // get a handle to the pool's spawner.
    /// let spawner = pool.spawner();
    ///
    /// // execute a job which can spawn other jobs.
    /// spawner.recurse(|inner| {
    ///     for i in 0..10 {
    ///         inner.submit(move || println!("{}", i));
    ///     }
    /// })
    /// ```
    pub fn recurse<F>(&self, f: F)
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
                // invariants by accessing other workers' queues/arenas
                // in unexpected ways.
                let spawner = make_spawner(worker, count_ptr);
                f(mem::transmute(&spawner))
            });
        }
    }

    /// Submit a job to be executed by the thread pool.
    ///
    /// The job's contents must outlive the spawner's scope. If they don't,
    /// you can create a more focused scope by calling the `scope` function on the spawner,
    /// and then submitting the job.
    ///
    /// Jobs are passed a handle to the spawner, so work can be split recursively.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use jobsteal::make_pool;
    ///
    /// let mut pool = make_pool(2).unwrap();
    ///
    /// // get a handle to the pool's spawner.
    /// let spawner = pool.spawner();
    ///
    /// for i in 0..10 {
    ///     // this spawner can only be used to execute jobs which fully own their data.
    ///     spawner.submit(move || println!("Hello {}", i));
    /// }
    /// ```
    pub fn submit<F>(&self, f: F) where F: 'scope + Send + FnOnce() {
        if !self.counter.is_null() {
            unsafe { (*self.counter).fetch_add(1, Ordering::AcqRel) };
        }

        unsafe {
            self.worker.submit_internal(self.counter, move |_| {
                f();
            });
        }
    }

    /// Construct a new spawning scope smaller than the one this spawner resides in.
    ///
    /// # Examples
    ///
    /// Incrementing all the values in a vector.
    ///
    /// ```
    /// use jobsteal::make_pool;
    ///
    /// let mut pool = make_pool(2).unwrap();
    /// let spawner = pool.spawner();
    ///
    /// let mut v = (0..1024).collect::<Vec<_>>();
    /// spawner.scope(|scope| {
    ///     // within this scope, we can spawn jobs that access data outside of it.
    ///     for i in &mut v {
    ///         scope.submit(move || *i *= 2);
    ///     }
    /// });
    /// // all jobs submitted in the scope are completed before execution resumes here.
    /// ```
    pub fn scope<'new, F, R>(&'new self, f: F) -> R
        where 'scope: 'new,
        F: 'new + FnOnce(&Spawner<'pool, 'new>) -> R,
        R: 'new
    {
        self.worker.scope(f)
    }

    /// Execute two closures, possibly asynchronously, and return their results.
    /// This will block until they are both complete.
    ///
    /// # Examples
    ///
    /// Sorting a list in parallel.
    ///
    /// ```
    /// extern crate jobsteal;
    /// use jobsteal::Spawner;
    /// fn main() {
    ///     // a simple quicksort
    ///     fn quicksort<'a, 'b, T: Ord + Send>(data: &mut [T], spawner: &Spawner<'a, 'b>) {
    ///     	if data.len() <= 1 { return; }
    ///
    ///     	// partition the data.
    ///     	let pivot = partition(data);
    ///     	let (a, b) = data.split_at_mut(pivot);
    ///
    ///     	// recursively sort the two parts using the threadpool.
    ///     	spawner.join(
    ///     		|inner| quicksort(a, inner),
    ///     		|inner| quicksort(b, inner),
    ///     	);
    ///     }
    ///
    ///     // partition the array such that all elements on the left of the partition
    ///     // are less than those to the right.
    ///     fn partition<T: Ord>(data: &mut [T]) -> usize {
    ///         let pivot = data.len() - 1;
    ///         let mut i = 0;
    ///
    ///     	for j in 0..pivot {
    ///     		if data[j] <= data[pivot] {
    ///     			data.swap(i, j);
    ///     			i += 1;
    ///     		}
    ///     	}
    ///     	data.swap(i, pivot);
    ///     	i
    ///     }
    ///
    ///
    ///     let len = 1024;
    ///     let mut to_sort = (0..len).rev().collect::<Vec<_>>();
    ///
    ///     let mut pool = jobsteal::make_pool(4).unwrap();
    ///     quicksort(&mut to_sort, &pool.spawner());
    ///
    ///     for pair in to_sort.windows(2) {
    /// 	    assert!(pair[1] >= pair[0]);
    ///     }
    /// }
    /// ```
    #[allow(non_camel_case_types)]
    pub fn join<A, B, R_A, R_B>(&self, oper_a: A, oper_b: B) -> (R_A, R_B)
    where A: Send + for<'new> FnOnce(&Spawner<'pool, 'new>) -> R_A,
          B: Send + for<'new> FnOnce(&Spawner<'pool, 'new>) -> R_B,
          R_A: Send,
          R_B: Send,
    {
        let mut a_dest = None;
        let mut b_dest = None;

        self.scope(|scope| {
            a_dest = Some(oper_a(&scope));
            b_dest = Some(oper_b(&scope));
        });

        (a_dest.unwrap(), b_dest.unwrap())
    }
}

/// It is only safe to clone spawners with a 'static scope.
/// Otherwise, we could write something like this:
///
/// ```ignore
/// let mut pool = make_pool(4).unwrap();
/// let leaked = pool.scope(|scope| scope.clone());
/// // oops, we just leaked a scoped spawner!
/// ```
impl<'a> Clone for Spawner<'a, 'static> {
    fn clone(&self) -> Self {
        make_spawner(self.worker, self.counter)
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

/// The work pool manages worker threads in a work-stealing fork-join thread pool.
///
/// You can submit jobs to the pool directly using `Pool::submit`. This has similar
/// semantics to the standard library `thread::spawn`, in that the work provided must
/// fully own its data.
///
/// `Pool` also provides a facility for spawning scoped jobs via the "scope" function.
pub struct Pool {
    workers: Vec<WorkerHandle>,
    local_worker: Worker,
    state: State,
}

impl Pool {
    /// Creates a pool with `n` worker threads.
    ///
    /// You can create a pool with 0 worker threads,
    /// but jobs won't be run until the pool is given an opportunity to join them.
    /// This will occur at scope boundaries, the pool's destructor, or in calls to
    /// `synchronize`.
    fn new(n: usize) -> Result<Self, Error> {
        // one extra queue and pool for the job system.
        let queues = Arc::new((0..n + 1).map(|_| Queue::new()).collect::<Vec<_>>());
        let arenas = Arc::new((0..n + 1).map(|_| Arena::new()).collect::<Vec<_>>());
        let mut workers = Vec::with_capacity(n);

        let mut rng = thread_rng();
        for i in 0..n {
            let (work_send, work_recv) = channel();
            let (lead_send, lead_recv) = channel();

            let worker = Worker {
                queues: queues.clone(),
                arenas: arenas.clone(),
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

        let mut pool = Pool {
            workers: workers,
            local_worker: Worker {
                queues: queues,
                arenas: arenas,
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
    ///
    /// This is shorthand for `my_pool.spawner().scope(|scope| ...)`.
    ///
    /// Any jobs submitted in this scope will be completed by the end of this function call.
    /// See Spawner::scope for a more detailed description.
    pub fn scope<'pool, 'new, F, R>(&'pool mut self, f: F) -> R
        where F: 'new + FnOnce(&Spawner<'pool, 'new>) -> R,
              R: 'new,
    {
        self.spin_up();

        self.local_worker.scope(f)
    }

    /// Execute a job which strictly owns its contents and is to be given access to the thread pool.
    ///
    /// The execution of this function may be deferred beyond this function call,
    /// to as far as either the next call to `synchronize` or the pool's destructor.
    ///
    /// This is shorthand for `my_pool.spawner().recurse(my_job);`
    ///
    /// # Safety
    ///
    /// In the general case, it is safe to submit jobs which only strictly outlive the pool.
    /// However, there is always the chance that the pool could be leaked, violating the
    /// invariant that the job is completed while its data is still alive.
    pub fn recurse<'a, F>(&'a mut self, f: F)
        where F: 'static + Send + FnOnce(&Spawner<'a, 'static>)
    {
        self.spawner().recurse(f);
    }

    /// Execute a job which strictly owns its contents.
    ///
    /// The execution of this function may be deferred beyond this function call,
    /// to as far as either the next call to `synchronize` or the pool's destructor.
    ///
    /// This is shorthand for `my_pool.spawner().submit(my_job);`
    ///
    /// # Safety
    ///
    /// In the general case, it is safe to submit jobs which only strictly outlive the pool.
    /// However, there is always the chance that the pool could be leaked, violating the
    /// invariant that the job is completed while its data is still alive.
    pub fn submit<'a, F>(&'a mut self, f: F)
        where F: 'static + Send + FnOnce()
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

    /// Get the pool's spawner.
    ///
    /// This will initally only accept jobs which outive 'static,
    /// but the scope can be focused further.
    /// See the `Spawner` documentation for more information.
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

        // cull all cached memory in the arenas and queues.
        // at this point, we have received notice from all workers
        // that they have stopped (either willingly or by panic),
        // and that all jobs have been run.
        for arena in self.local_worker.arenas.iter() {
            unsafe { arena.cull_memory(); }
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

impl Drop for Pool {
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
///
/// You can create a pool with 0 worker threads,
/// but jobs won't be run until the pool is given an opportunity to join them.
/// This will occur at scope boundaries, the pool's destructor, or in manual calls to
/// `synchronize`.
pub fn make_pool(n: usize) -> Result<Pool, Error> {
    Pool::new(n)
}

#[cfg(test)]
mod tests {
    use super::Pool;

    fn pool_harness<F>(f: F) where F: Fn(&mut Pool) {
        for i in 0..32 {
            let mut pool = Pool::new(i).unwrap();
            f(&mut pool);
        }
    }

    #[test]
    fn creation_destruction() {
        pool_harness(|_| {}); // just do nothing
    }

    #[test]
    fn split_work() {
        pool_harness(|pool| {
            let mut v = vec![0; 1024];
            pool.scope(|spawner| {
                for (idx, v) in v.iter_mut().enumerate() {
                    spawner.submit(move || {
                        *v += idx;
                    });
                }
            });

            assert_eq!(v, (0..1024).collect::<Vec<_>>());
        });
    }

    #[test]
    fn multilevel_scoping() {
        pool_harness(|pool| {
            pool.scope(|spawner| {
                let mut v = vec![0; 256];
                spawner.scope(|s| {
                    for i in &mut v {
                        s.submit(move || *i += 1)
                    }
                });
                // job is forcibly joined here.

                assert_eq!(v, vec![1; 256]);
            }); // any other jobs would be forcibly joined here.
        });
    }

    #[test]
    fn multiple_synchronizations() {
        pool_harness(|pool| {
            for _ in 0..100 {
                pool.synchronize();
            }
        });
    }

    #[test]
    fn join() {
        pool_harness(|pool| {
            let (a, b) = (1, 2);
            let (r_a, r_b) = pool.spawner().join(|_| a, |_| b);
            assert_eq!(a, r_a);
            assert_eq!(b, r_b);
        });
    }

    #[test]
    fn join_scoping() {
        pool_harness(|pool| {
            let mut v = vec![0; 256];
            {
                let (a, b) = v.split_at_mut(128);
                pool.spawner().join(
                    |_| for i in a { *i += 1},
                    |_| for i in b { *i += 1},
                );
            }

            assert_eq!(v, vec![1; 256])
        });
    }

    #[test]
    fn outlives_pool() {
        let v = vec![0; 256];
        pool_harness(|pool| {
            let mut v = v.clone();
            // can only execute 'static functions here -- otherwise,
            // some person might make the dumb mistake of calling `forget` the
            // pool before it has the chance to run all submitted jobs.
            pool.submit(move || {
                for i in &mut v {
                    *i += 1
                }
            });
        });
    }

    #[test]
    fn scope_return() {
        pool_harness(|pool| {
            let x = pool.scope(|_| 0);
            assert_eq!(x, 0);
        });
    }

    #[test]
    #[should_panic]
    fn job_panic() {
        let mut pool = Pool::new(1).unwrap();
        pool.submit(|| panic!("Eep!"));
    }

    #[test]
    fn clone_static_spawner() {
        pool_harness(|pool| {
           let _ = pool.spawner().clone();
        });
    }
}
