//! Collect Spliterators into collections.

use std::iter::FromIterator;
use std::marker::PhantomData;

use super::{Callback, Consumer, IntoSpliterator, Split, Spliterator};
use Spawner;

/// Items that can be combined with another of the same type.
pub trait Combine {
    /// Combine `self` with `other`.
    fn combine(self, other: Self) -> Self;
}

/// Collect the elements of this iterator into a collection.
///
/// This is more easily used through `Spliterator::collect()`.
///
/// Note that this works by repeatedly combining the outputs of
/// FromIterator, so it may lead to more allocations than anticipated.
pub fn collect<I: IntoSpliterator, T>(iter: I, spawner: &Spawner) -> T
where I::Item: Send, T: FromIterator<I::Item> + Combine + Send {
    let (base, consumer) = iter.into_split_iter().destructure();

    collect_helper::<I::SplitIter, T>(base, &consumer, spawner)
}

fn collect_helper<I: Spliterator, T>(base: I::Base, consumer: &I::Consumer, spawner: &Spawner) -> T
where I::Item: Send, T: FromIterator<I::Item> + Combine + Send {
    if let Some(idx) = base.should_split(1.0) {
        let (b1, b2) = base.split(idx);
        let (t1, t2) =
            spawner.join(
                move |inner| collect_helper::<I, T>(b1, consumer, inner),
                move |inner| collect_helper::<I, T>(b2, consumer, inner),
            );

        t1.combine(t2)
    } else {
        consumer.consume(base, CollectCallback(PhantomData))
    }
}

struct CollectCallback<T>(PhantomData<T>);

impl<I, T: FromIterator<I>> Callback<I> for CollectCallback<T> {
    type Out = T;
    fn call<Iter: Iterator<Item=I>>(self, iter: Iter) -> T {
        T::from_iter(iter)
    }
}

impl<T> Combine for Vec<T> {
    fn combine(mut self, mut other: Self) -> Self {
        self.append(&mut other);
        self
    }
}

#[cfg(test)]
mod tests {
    use ::{IntoSpliterator, Spliterator, pool_harness};

    #[test]
    fn collect_basics() {
        pool_harness(|pool| {
            let v = (0..5000).collect::<Vec<_>>();
            let doubled = v.into_split_iter().map(|x| x * 2).collect::<Vec<_>>(&pool.spawner());

            let st_doubled: Vec<_> = (0..5000).map(|x| x * 2).collect();

            assert_eq!(doubled, st_doubled);
        })
    }
}