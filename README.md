# Jobsteal [![jobsteal](https://travis-ci.org/rphmeier/jobsteal.svg?branch=master)](https://travis-ci.org/rphmeier/jobsteal)

A work-stealing fork-join threadpool written in Rust.
This features low-level APIs to directly submit tasks to the pool,
along with a high-level parallel iteration API called `Spliterator`,
which is very similar to Rust's Iterators.

## [See the Documentation](https://rphmeier.github.io/jobsteal/)

## Examples
This is a basic example to show a simple way to use the job pool.

```rust
use jobsteal::make_pool;
fn main() {
    // Build a pool with 4 threads, including this one.
    let mut pool = make_pool(4).unwrap();

    // spawn 100 jobs
    for i in 0..100 {
        // You can only submit jobs with a static lifetime this way.
        pool.submit(move || println!("Job {}", i));
    }
}
```

---
Here's a more useful example where we split up a vector into chunks and submit a job for every part. This makes use of the scoping feature.
```rust
use jobsteal::make_pool;

fn main(){
    // Build a pool
    let mut pool = make_pool(4).unwrap();

    let mut v = vec![0; 256];

    // Create a scoped spawner.
    pool.scope(|scope| {
        for chunk in v.chunks_mut(32) {

            // Jobs spawned by the scope are only allowed to access
            // data which strictly outlives the call to "scope".
            scope.submit(move || {
                for i in chunk { *i += 1 }
            });
        }
    });
    // all jobs within the scope are forced to complete before the scope function returns.

    for i in v {
        assert_eq!(i, 1);
    }
}
```

The spawner passed to the "scope" closure can be used to create more scopes -- as nested as you'd like.
Each job function can have a spawner passed to it as well by spawning jobs with `Spawner::recurse`, so you can very easily split tasks recursively.
Recursive work-splitting typically leads to much better work distribution between worker threads.

However, it's much easier to submit jobs in order like we did in the above example. Jobsteal's `Spliterator` lets us feel like we're doing that,
while actually splitting the work optimally between threads!
Here's how:
```rust
use jobsteal::{make_pool, BorrowSpliteratorMut, Spliterator};

fn main() {
    let mut pool = make_pool(4).unwrap();

    let mut v = vec![0; 256];

    // iterate over the vector in parallel, incrementing each item by one.
    // the `for_each` function takes a spawner so it can dispatch jobs onto
    // the thread pool.
    v.split_iter_mut().for_each(&pool.spawner(), |i| *i += 1);

    for i in v {
        assert_eq!(i, 1);
    }
}
```

## Unwind Safety
A panic in one worker is intended to propagate to the main thread. However, the code hasn't been vetted for safety, so please try to avoid panicking in your jobs.
There should probably be an UnwindSafe bound on job functions. This would require nightly, and UnwindSafe is also really cumbersome.
