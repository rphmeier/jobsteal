//! A work-stealing fork-join thread pool used to perform work asynchronously.
//! Tasks submitted are intended to be short-lived. Long running asynchronous tasks should use another method.
//! This crate is fairly experimental and should be used with caution.

extern crate crossbeam;
extern crate rand;

mod arena;
mod job;
mod worker;

pub mod iter;

pub use iter::{IntoSpliterator, Split, Spliterator};

use crossbeam::sync::chase_lev::deque;

use rand::{Rng, SeedableRng, thread_rng, XorShiftRng};

use self::arena::Arena;
use self::worker::{SharedWorkerData, Worker};

use std::io::Error;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

const INITIAL_CAPACITY: usize = 256;

struct WorkerHandle {
    thread: Option<thread::JoinHandle<()>>,
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
    ///     fn quicksort<T: Ord + Send>(data: &mut [T], spawner: &Spawner) {
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
    ///     let mut pool = jobsteal::make_pool(0).unwrap();
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
            // run oper_a potentially asynchronously.
            scope.recurse(|scope| a_dest = Some(oper_a(&scope)));
            // run oper_b synchronously.
            b_dest = Some(oper_b(&scope));
            // wait for oper_a to finish, running it locally if possible.
        });

        (a_dest.unwrap(), b_dest.unwrap())
    }
}

fn make_spawner<'a, 'b>(worker: &'a Worker, counter: *const AtomicUsize) -> Spawner<'a, 'b> {
    Spawner {
        worker: worker,
        counter: counter,
        _marker: PhantomData,
    }
}

fn worker_main(worker: Worker) {
    while !worker.should_shutdown() {
        unsafe { worker.run_next(true) };
    }

    worker.clear();
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
        let (mut poppers, stealers) = {
            let mut poppers = Vec::with_capacity(n + 1);
            let mut stealers = Vec::with_capacity(n + 1);

            for _ in 0..n+1 {
                let (p, s) = deque();

                poppers.push(p);
                stealers.push(s);
            }

            (poppers.into_iter(), stealers)
        };

        let mut rng = thread_rng();

        let arenas = (0..n + 1).map(|_| Arena::new()).collect::<Vec<_>>();
        let shared_data = Arc::new(SharedWorkerData::new(stealers, arenas));
        let mut workers = Vec::with_capacity(n);

        let local_worker = Worker::new(
            shared_data.clone(),
            0,
            XorShiftRng::from_seed(rng.gen::<[u32; 4]>()),
            poppers.next().unwrap(),
        );

        for (i, pop) in (0..n).zip(poppers) {
            let worker = Worker::new(
                shared_data.clone(),
                i + 1,
                XorShiftRng::from_seed(rng.gen::<[u32; 4]>()),
                pop,
            );

            let builder = thread::Builder::new().name(format!("worker_{}", i));
            let handle = try!(builder.spawn(move || worker_main(worker)));

            workers.push(WorkerHandle {
                thread: Some(handle),
            });
        }

        let pool = Pool {
            workers: workers,
            local_worker: local_worker,
        };

        Ok(pool)
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

    /// Get the pool's spawner.
    ///
    /// This will initally only accept jobs which outive 'static,
    /// but the scope can be focused further.
    /// See the `Spawner` documentation for more information.
    pub fn spawner<'a>(&'a mut self) -> Spawner<'a, 'static> {
        use std::ptr;

        make_spawner(&self.local_worker, ptr::null_mut())
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.local_worker.shared_data().notify_shutdown();
        self.local_worker.clear();

        // join the threads to wait for all of them to shutdown
        // properly. propagate panics here.
        for worker in &mut self.workers {
            let handle = worker.thread.take().unwrap();

            if handle.join().is_err() {
                panic!("Propagating worker thread panic");
            }
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
fn pool_harness<F>(f: F) where F: Fn(&mut Pool) {
    for i in 0..32 {
        let mut pool = Pool::new(i).unwrap();
        f(&mut pool);
    }
}

#[cfg(test)]
mod tests {
    use super::{pool_harness, Pool};

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
    fn submit_large_jobs() {
        pool_harness(|pool| {
           let mut v = vec![0; 1024];
           pool.scope(|scope| {
               let v = &mut v;
               let a = [0; 4096];

               scope.submit(move || {
                   // move the array in to artificially increase the environment size.
                   let _ = a;
                   for i in v.iter_mut() { *i += 1 }
               });
           });

           // check that the job is actually being executed.
           assert_eq!(v, vec![1; 1024]);
        });
    }
}
