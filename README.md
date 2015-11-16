# Jobsteal

A work-stealing fork-join threadpool written in Rust.

## Examples
This is a basic example to show a simple way to use the job pool.

```rust
use jobsteal::make_pool;
fn main() {
    // Build a pool with 4 threads, including this one.
    // This call will fail if you supply this function with '0'.
    let pool = make_pool(4).unwrap();

    // spawn 100 jobs
    for i in 0..100 {
        // You can only submit jobs with a static lifetime this way.
        // It returns a RAII guard for force-waiting.
        // If you just let it drop, you can do other things while you wait.
        let handle = pool.submit(move |_| println!("Job {}", i));
    }
}
```

---
Here's a more useful example where we split up a vector into chunks and submit a job for every part. This makes use of the scoping feature. Scoped threads return a different kind of handle.
```rust
use jobsteal::make_pool;

fn main(){ 
    // Build a pool
    let pool = make_pool(4).unwrap();

    let mut v = vec![0; 256];

    // Create a scope object
    pool.scope(|scope| {
        for chunk in v.chunks_mut(32) {

            // Jobs spawned by the scope are only allowed to access
            // data outside the scope of this closure.
            let handle = scope.submit(move |_| {
                for i in chunk { *i += 1 }
            });

            // Scoped handles are released on drop or manually.
            // You can also wait for work to finish manually by
            // calling wait().
            handle.release();
        }

        for i in v {
            assert_eq!(i, 1);
        }
    });

}
```

---
## Safety
All handles should be (relatively) safe to leak. However, the code hasn't been vetted for safety. I would strongly recommend against leaking handles intentionally. Additionally, the destructor for Scope isn't fully transactional yet and cannot necessarily resume while unwinding. Panic safety isn't perfect, so please avoid panics in your code for the time being.