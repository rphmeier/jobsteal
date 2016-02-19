# Jobsteal [![jobsteal](https://travis-ci.org/rphmeier/jobsteal.svg?branch=master)](https://travis-ci.org/rphmeier/jobsteal)

A work-stealing fork-join threadpool written in Rust.

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

        for i in v {
            assert_eq!(i, 1);
        }
    });
    // all jobs within the scope are forced to complete before the scope function returns.

}
```

The spawner passed to the "scope" closure can be used to create more scopes -- as nested as you'd like.
Each job function can have a spawner passed to it as well by spawning jobs with `Spawner::recurse`, so you can very easily split tasks recursively.
Recursive work-splitting typically leads to much better work distribution between worker threads.

## Recover Safety
A panic in one worker is intended to propagate to the main thread eventually. However, the code hasn't been vetted for safety, so please try to avoid panicking in your jobs.
There should probably be a RecoverSafe bound on job functions. This would require nightly, and RecoverSafe is also really cumbersome.
