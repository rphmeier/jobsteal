//! Fold or reduce the items of a `SplitIterator` into one.
//!
//! This is usually done through addition or multiplication.

use super::{Callback, Consumer, IntoSplitIterator, Split, SplitIterator};
use ::Spawner;

/// Something which can "fold" two items together.
pub trait Folder<T> {
    fn fold(&self, a: T, b: T) -> T;
}

impl<T, F> Folder<T> for F where F: Fn(T, T) -> T {
    fn fold(&self, a: T, b: T) -> T {
        (self)(a, b)
    }
}

pub fn fold<I: IntoSplitIterator, F: Sync>(iter: I, spawner: &Spawner, initial: I::Item, folder: F) -> I::Item
where F: Folder<I::Item>, I::Item: Send {
    let (base, consumer) = iter.into_split_iter().destructure();
    match fold_helper::<I::SplitIter, F>(base, &consumer, spawner, &folder) {
        Some(item) => folder.fold(initial, item),
        None => initial,
    }
}

fn fold_helper<I, F>(base: I::Base, consumer: &I::Consumer, spawner: &Spawner, folder: &F) -> Option<I::Item>
where I: SplitIterator, F: Sync + Folder<I::Item>, I::Item: Send {
    if let Some(idx) = base.should_split(1.0) {
        let (b1, b2) = base.split(idx);

        let (f1, f2) =
            spawner.join(
                move |inner| fold_helper::<I, F>(b1, consumer, inner, folder),
                move |inner| fold_helper::<I, F>(b2, consumer, inner, folder),
            );

        match (f1, f2) {
            (Some(a), Some(b)) => Some(folder.fold(a, b)),
            (Some(x), _) | (_, Some(x)) => Some(x),
            _ => None,
        }
    } else {
        return consumer.consume(base, FoldCallback(folder));

        struct FoldCallback<F>(F);

        impl<'a, T, F: 'a + Sync> Callback<T> for FoldCallback<&'a F>
        where F: Folder<T>  {
            type Out = Option<T>;

            fn call<I: Iterator<Item=T>>(self, mut iter: I) -> Option<T> {
                if let Some(item) = iter.next() {
                    let mut item = item;
                    for i in iter {
                        item = self.0.fold(item, i)
                    }
                    Some(item)
                } else {
                    None
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use ::{IntoSplitIterator, SplitIterator, pool_harness};

    #[test]
    fn fold_adding() {
        let v: Vec<_> = (0..10000).collect();
        pool_harness(|pool| {
            let i = v.into_split_iter().cloned().fold(&pool.spawner(), 0, |a, b| a + b);
            assert_eq!(i, 5000*9999); // n*(n-1)/2
        })
    }
}