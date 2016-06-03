//! Whether all or any of the elements in a `Spliterator` fulfill a predicate.
use std::sync::atomic::{AtomicBool, Ordering};

use super::{Callback, Consumer, IntoSpliterator, Split, Spliterator};
use Spawner;

struct AnyCallback<'a, P: 'a> {
    global: &'a AtomicBool,
    pred: &'a P
}

impl<'a, P: 'a> Clone for AnyCallback<'a, P> {
    fn clone(&self) -> Self {
        AnyCallback {
            global: self.global,
            pred: self.pred,
        }
    }
}

impl<'a, P: 'a, T> Callback<T> for AnyCallback<'a, P> where P: Fn(T) -> bool {
    type Out = bool;

    fn call<I: Iterator<Item=T>>(self, iter: I) -> bool {
        // we use a global tracker for a fast exit path.
        if !self.global.load(Ordering::SeqCst) {
            for i in iter {
                // short-circuit as soon as we find a good item.
                if (self.pred)(i) {
                    self.global.store(true, Ordering::SeqCst);
                    return true;
                }
            }
        }
        return false;
    }
}

/// Consumes the iterator, returning true if all elements fulfill the predicate
/// and false otherwise.
pub fn all<I: IntoSpliterator, P: Sync>(iter: I, spawner: &Spawner, pred: P) -> bool
where P: Fn(I::Item) -> bool {
    // all just asks if there are any that don't fulfill the predicate.
    // if the answer to that question is true, then not all of them do.
    !any(iter, spawner, move |item| !pred(item))
}

/// Consumes the iterator, returning true if any element fulfills the predicate
/// and false otherwise.
pub fn any<I: IntoSpliterator, P: Sync>(iter: I, spawner: &Spawner, pred: P) -> bool
where P: Fn(I::Item) -> bool {
    let global = AtomicBool::new(false);
    let cb = AnyCallback {
        global: &global,
        pred: &pred,
    };

    let (base, consumer) = iter.into_split_iter().destructure();

    any_helper::<I::SplitIter, P>(base, &consumer, spawner, cb)
}

fn any_helper<I: Spliterator, P>(base: I::Base, consumer: &I::Consumer, spawner: &Spawner, cb: AnyCallback<P>) -> bool
where P: Sync + Fn(I::Item) -> bool {
    if let Some(idx) = base.should_split(1.0) {
        let (b1, b2) = base.split(idx);

        let new_cb = cb.clone();

        let (r1, r2) = spawner.join(
            move |inner| any_helper::<I, P>(b1, consumer, inner, cb),
            move |inner| any_helper::<I, P>(b2, consumer, inner, new_cb),
        );

        r1 || r2
    } else {
        consumer.consume(base, cb)
    }
}

#[cfg(test)]
mod tests {
    use ::{BorrowSpliterator, Spliterator, pool_harness};

    #[test]
    fn any_even() {
        let v = (0..5000).map(|x| x * 2).collect::<Vec<_>>();
        pool_harness(|pool| {

            let x = v.split_iter().cloned()
                .any(&pool.spawner(), |x| x % 2 == 0);
            assert!(x);
        })
    }

    #[test]
    fn all_even() {
        let v = (0..5000).map(|x| x * 2 + 1).collect::<Vec<_>>();

        pool_harness(|pool| {
            let x = v.split_iter()
                .cloned()
                .all(&pool.spawner(), |x| x % 2 == 0);

            assert!(!x);
        })
    }
}