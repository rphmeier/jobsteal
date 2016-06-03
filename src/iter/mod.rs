//! Jobsteal's Parallel iterators.
//!
//! The `Spliterator` trait handles parallel iteration,
//! and the `Split` trait deals with items which can be the
//! base of a Spliterator.
//!
//! Using `Spliterators`, we can write code that feels single-threaded
//! and simple that actually gets dispatched optimally onto the thread pool.
//!
//! Here's an example where we take a list of 16000 numbers, filter it by
//! whether they're even, and then perform an action for each one...in parallel!
//!
//! ```rust
//! use jobsteal::{make_pool, IntoSpliterator, Spliterator};
//!
//! let mut pool = make_pool(4).unwrap();
//!
//! let v = (0..16000).collect::<Vec<_>>();
//! v.into_split_iter()
//!     .filter(|&x| x % 2 == 0) // only even numbers allowed!
//!     .for_each(&pool.spawner(), |x| drop(x)); // do an action for each item!
//! ```
//!
//! These `Spliterator`s can be used almost exactly like Rust's normal iterators!
//! The only difference is that when they're being "consumed" by something like
//! `for_each`, `collect`, `fold`, or `any`, they take one of jobsteal's spawners
//! as an argument. This allows them to distribute the work across the threads of
//! the pool the spawner is a handle for.

use std::iter::FromIterator;

use super::Spawner;

pub mod all_any;
pub mod collect;
pub mod fold;

mod cost_mul;
mod enumerate;
mod filter;
mod flat_map;
mod map;
mod vec;
mod zip;

pub use self::collect::Combine;

/// A parallel iterator which works by splitting the underlying data
/// and sharing it between threads.
///
/// Functions which consume the iterator will take a `&Spawner` as an argument,
/// so that they can distribute their work in parallel.
///
/// These can be used almost exactly the same as Rust's regular `Iterators`,
/// with adapters like `enumerate`, `map`, `filter`, and more along with
/// consumers like `for_each` and `collect`.
///
/// This trait may seem complicated to implement, but fear not!
/// It is automatically implemented for data which implements `Split`,
/// which is a lot easier to implement.
///
/// `Spliterator` only needs to be manually implemented by those who are
/// trying to make their own adapters. Those individuals may want to look
/// at the source of iterator adapters like `Map` and `Filter`. The good news
/// is that all the `Spliterator` adapers are implemented using only public
/// and safe jobsteal code, so anybody can create their own. The bad news is
/// that it does get fairly complex.
pub trait Spliterator: Sized {
    /// The item this iterator produces.
    type Item;

    /// The splittable base data which this consists of.
    type Base: Split;

    /// A consumer which can act as an ad-hoc iterator adapter
    /// chain while being shared across threads.
    type Consumer: Consumer<Self::Base, Item=Self::Item>;

    /// Destructure this iterator into a splittable base and
    /// a shareable consumer of that base.
    fn destructure(self) -> (Self::Base, Self::Consumer);

    /// Clone the items of this iterator to get owned copies.
    fn cloned<'a, T: 'a + Clone>(self) -> Cloned<Self> where Self: Spliterator<Item=&'a T> {
        Cloned {
            parent: self,
        }
    }

    /// Enumerate items by their index.
    fn enumerate(self) -> Enumerate<Self> {
        Enumerate {
            parent: self,
            off: 0,
        }
    }

    /// Filter items by some predicate.
    fn filter<F: Sync>(self, pred: F) -> Filter<Self, F> where F: Fn(&Self::Item) -> bool {
        Filter {
            parent: self,
            pred: pred,
        }
    }

    /// Produce an iterator for each element, and then yield the elements of those iterators.
    fn flat_map<U, F: Sync>(self, flat_map: F) -> FlatMap<Self, F>
    where U: IntoIterator, F: Fn(Self::Item) -> U {
        FlatMap {
            parent: self,
            flat_map: flat_map
        }
    }

    /// Map the items of this iterator to another type using the supplied function.
    fn map<F: Sync, U>(self, map: F) -> Map<Self, F> where F: Fn(Self::Item) -> U {
        Map {
            parent: self,
            map: map,
        }
    }

    /// Zip this iterator with another, combining their items
    /// in a tuple.
    fn zip<B: IntoSpliterator>(self, other: B) -> Zip<Self, B::SplitIter> {
        Zip {
            a: self,
            b: other.into_split_iter(),
        }
    }

    /// Whether any of the elements fulfill the supplied predicate.
    fn any<P: Sync>(self, spawner: &Spawner, pred: P) -> bool
    where P: Fn(Self::Item) -> bool {
        all_any::any(self, spawner, pred)
    }

    /// Whether all of the elements fulfill the supplied predicate.
    fn all<P: Sync>(self, spawner: &Spawner, pred: P) -> bool
    where P: Fn(Self::Item) -> bool {
        all_any::all(self, spawner, pred)
    }

    /// Consume this iterator, performing an action for each item.
    fn for_each<F>(self, spawner: &Spawner, f: F) where F: Sync + Fn(Self::Item) {
        let (base, consumer) = self.destructure();
        for_each_helper::<Self, F>(base, &consumer, spawner, &f);
    }

    /// Collect the items of this iterator into a combinable collection.
    ///
    /// Note that this works by repeatedly combining the results of `from_iter`,
    /// so this will probably lead to more allocations than a single-threaded
    /// iterator `collect`.
    fn collect<T: Send>(self, spawner: &Spawner) -> T
    where Self::Item: Send, T: FromIterator<Self::Item> + Combine + Send {
        collect::collect(self, spawner)
    }

    /// Fold the items of the iterator together with the given operation and
    /// initial state.
    ///
    /// Note: For this to work in parallel, the implementation
    /// assumes that the operation is commutative.
    ///
    /// There will not be any safety issues caused by non-commutative operations,
    /// but there may be some incorrectness.
    fn fold<F: Sync>(self, spawner: &Spawner, initial: Self::Item, folder: F) -> Self::Item
    where F: fold::Folder<Self::Item>, Self::Item: Send {
        fold::fold(self, spawner, initial, folder)
    }


    /// A lower bound and optional upper bound on the size of this iterator.
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }

    /// Uses a cost multiplier for this iterator.
    ///
    /// This takes the absolute value of the multiplier supplied.
    ///
    /// This will affect how often the data backing this iterator is split
    /// in two.
    ///
    /// `with_cost_mul(1.0)` will have no effect.
    ///
    /// `with_cost_mul(2.0)` will cause twice as much splitting, while
    /// `with_cost_mul(0.5)` will cause half as much splitting.
    ///
    /// `with_cost_mul(0.0)` will cause the data to never split, and
    /// `with_cost_mul(f32::INFINITY)` will cause the data to split whenever
    /// possible.
    fn with_cost_mul(self, mul: f32) -> CostMul<Self> {
        let mul = if mul < 0.0 { -mul } else { mul };
        CostMul {
            parent: self,
            mul: mul,
        }
    }
}

fn for_each_helper<T, F>(base: T::Base, consumer: &T::Consumer, spawner: &Spawner, f: &F)
where T: Spliterator, F: Sync + Fn(T::Item) {
    if let Some(idx) = base.should_split(1.0) {
        let (b1, b2) = base.split(idx);
        spawner.join(
            move |inner| for_each_helper::<T, F>(b1, consumer, inner, f),
            move |inner| for_each_helper::<T, F>(b2, consumer, inner, f),
        );
    } else {
        consumer.consume(base, f);
    }
}

/// An iterator for which the exact number of elements is known.
///
/// If this is implemented for an iterator,`size_hint` for that iterator
/// must return a pair of the exact size.
pub trait ExactSizeSpliterator: Spliterator {
    /// Get the number of elements in this iterator.
    fn size(&self) -> usize;
}

#[derive(Clone)]
pub struct Cloned<T> {
    parent: T,
}

/// Enumerate iterator adapter
#[derive(Clone)]
pub struct Enumerate<T> {
    parent: T,
    off: usize,
}

/// Filter ilterator adapter.
///
/// This filters each element by a given predicate.
#[derive(Clone)]
pub struct Filter<T, F> {
    parent: T,
    pred: F,
}

/// Flat Mapping iterator adapter.
///
/// This produces an iterator for each item, and then yields the items of
/// those iterators.
#[derive(Clone)]
pub struct FlatMap<T, F> {
    parent: T,
    flat_map: F,
}

/// Map iterator adapter.
///
/// This transforms each element into a new object.
#[derive(Clone)]
pub struct Map<T, F> {
    parent: T,
    map: F,
}

/// Zip iterator adapter.
///
/// This combines two other iterators into one.
#[derive(Clone)]
pub struct Zip<A, B> {
    a: A,
    b: B,
}

/// A cost multiplier.
/// See the docs of `Split::with_cost_mul` for more.
#[derive(Clone)]
pub struct CostMul<T> {
    parent: T,
    mul: f32,
}

/// Data which can be split in two at an index.
pub trait Split: Send + IntoIterator {
    /// Whether this should split.
    ///
    /// This is given a multiplier, which tells you to treat the data
    /// as "mul" times as large as it is. The reason for this is that
    /// each iterator adapter makes producing an individual item more expensive.
    /// As the cost of producing each item goes up, we want to split the data
    /// into more parallelizable jobs so we can get better work distribution
    /// across threads.
    ///
    /// For example, should_split(1.0) for slices returns Some only
    /// when then length of the slice is over 4096. This means
    /// that `should_split(2.0)` will return Some when the length
    /// of the slice is over 2048.
    fn should_split(&self, mul: f32) -> Option<usize>;

    /// Split the data at the specified index.
    /// Note that this may not always be the same as the index
    /// you return from should_split.
    fn split(self, usize) -> (Self, Self);

    /// A hint for the size of this data, containing
    /// a known lower bound (potentially zero) and an optional upper bound.
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

impl<T: Split> Spliterator for T {
    type Item = T::Item;
    type Base = Self;
    type Consumer = ();

    fn destructure(self) -> (Self, ()) {
        (self, ())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        Split::size_hint(self)
    }
}

/// Things that can be turned into a `Spliterator`.
pub trait IntoSpliterator {
    /// The item the split iterator will produce.
    type Item;

    /// The iterator this will turn into.
    type SplitIter: Spliterator<Item=Self::Item>;

    fn into_split_iter(self) -> Self::SplitIter;
}

impl<T: Spliterator> IntoSpliterator for T {
    type Item = <Self as Spliterator>::Item;
    type SplitIter = Self;

    fn into_split_iter(self) -> Self::SplitIter {
        self
    }
}

/// Things that can have a `Spliterator` borrowed from them.
pub trait BorrowSpliterator<'a> {
    type Item: 'a;
    type SplitIter: 'a + Spliterator<Item=Self::Item>;

    fn split_iter(&'a self) -> Self::SplitIter;
}

/// Things that can have a `Spliterator` borrowed mutably from them.
pub trait BorrowSpliteratorMut<'a> {
    type Item: 'a;
    type SplitIter: 'a + Spliterator<Item=Self::Item>;

    fn split_iter_mut(&'a mut self) -> Self::SplitIter;
}

impl<'a, T: 'a + Sync> IntoSpliterator for &'a [T] {
    type Item = &'a T;
    type SplitIter = SliceSplit<'a, T>;

    fn into_split_iter(self) -> Self::SplitIter {
        SliceSplit(self)
    }
}

impl<'a, T: 'a + Sync + Send> IntoSpliterator for &'a mut [T] {
    type Item = &'a mut T;
    type SplitIter = SliceSplitMut<'a, T>;

    fn into_split_iter(self) -> Self::SplitIter {
        SliceSplitMut(self)
    }
}

impl<'a, T: 'a + Sync> IntoSpliterator for &'a Vec<T> {
    type Item = &'a T;
    type SplitIter = SliceSplit<'a, T>;

    fn into_split_iter(self) -> Self::SplitIter {
        SliceSplit(&self)
    }
}

impl<'a, T: 'a + Sync + Send> IntoSpliterator for &'a mut Vec<T> {
    type Item = &'a mut T;
    type SplitIter = SliceSplitMut<'a, T>;

    fn into_split_iter(self) -> Self::SplitIter {
        SliceSplitMut(self)
    }
}

impl<'a, T: 'a + Sync> BorrowSpliterator<'a> for Vec<T> {
    type Item = &'a T;
    type SplitIter = SliceSplit<'a, T>;

    fn split_iter(&'a self) -> Self::SplitIter {
        SliceSplit(&*self)
    }
}

impl<'a, T: 'a + Sync + Send> BorrowSpliteratorMut<'a> for Vec<T> {
    type Item = &'a mut T;
    type SplitIter = SliceSplitMut<'a, T>;

    fn split_iter_mut(&'a mut self) -> Self::SplitIter {
        SliceSplitMut(&mut *self)
    }
}

/// Used to mask data so that implementations don't conflict.
///
/// Specifically, this is used in the implementation of `Spliterator`
/// for `Zip`, by hiding the base. This is because `Spliterator` requires
/// that the base be `Split`. However, for `Split` types, there is a blanket
/// impl for `Spliterator` for convenience, so if we didn't mask the type,
/// there would be conflicting implementations of `Spliterator` for `Zip`.
/// This will be obsoleted when specialization becomes stable.
///
/// Numerous other iterator adapters also use this, but will all be cleared
/// by specialization.
pub struct Hide<T>(T);

/// A split iterator over an immutable slice.
pub struct SliceSplit<'a, T: 'a>(&'a [T]);

/// A split iterator over a mutable slice.
pub struct SliceSplitMut<'a, T: 'a>(&'a mut [T]);

impl<'a, T: 'a> Clone for SliceSplit<'a, T> {
    fn clone(&self) -> Self {
        SliceSplit(self.0)
    }
}

impl<'a, T: 'a> IntoIterator for SliceSplit<'a, T> {
    type Item = <&'a [T] as IntoIterator>::Item;
    type IntoIter = <&'a [T] as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter { self.0.into_iter() }
}

impl<'a, T: 'a> IntoIterator for SliceSplitMut<'a, T> {
    type Item = <&'a mut [T] as IntoIterator>::Item;
    type IntoIter = <&'a mut [T] as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter { self.0.into_iter() }
}

impl<'a, T: 'a + Sync> Split for SliceSplit<'a, T> {
    fn should_split(&self, mul: f32) -> Option<usize> {
        let len = self.0.len();
        if len > 1 && (len as f32 *mul) > 4096.0 { Some(len / 2) }
        else { None }
    }

    fn split(self, idx: usize) -> (Self, Self) {
        let (a, b) = self.0.split_at(idx);
        (
            SliceSplit(a),
            SliceSplit(b),
        )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.0.len();

        (len, Some(len))
    }
}

impl<'a, T: 'a + Sync + Send> Split for SliceSplitMut<'a, T> {
    fn should_split(&self, mul: f32) -> Option<usize> {
        let len = self.0.len();
        if len > 1 && (len as f32 * mul) > 4096.0 { Some(len / 2) }
        else { None }
    }

    fn split(self, idx: usize) -> (Self, Self) {
        let (a, b) = self.0.split_at_mut(idx);
        (
            SliceSplitMut(a),
            SliceSplitMut(b),
        )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.0.len();

        (len, Some(len))
    }
}

impl<'a, T: 'a + Sync> ExactSizeSpliterator for SliceSplit<'a, T> {
    fn size(&self) -> usize {
        self.0.len()
    }
}

impl<'a, T: 'a + Sync + Send> ExactSizeSpliterator for SliceSplitMut<'a, T> {
    fn size(&self) -> usize {
        self.0.len()
    }
}

/// A callback which takes an iterator of the given item type,
/// processes it, and produces a result.
///
/// These are passed to `Consumer`s to lazily consume iterators.
pub trait Callback<T>: Sized {
    type Out;

    fn call<I: Iterator<Item=T>>(self, I) -> Self::Out;
}

impl<F, T> Callback<T> for F where F: FnMut(T) {
    type Out = ();

    fn call<I: Iterator<Item=T>>(mut self, iter: I) {
        for i in iter {
            (self)(i)
        }
    }
}

/// A consumer takes an `IntoIterator`, which is usually the
/// `Base` of a `Spliterator`, produces the desired iterator,
/// and passes it to the callback given.
pub trait Consumer<In: IntoIterator>: Sync {
    type Item;

    /// Consume the iterator, typically by passing it on to the
    /// parent consumer along with a callback which will receive
    /// a producer of items to transform.
    fn consume<C: Callback<Self::Item>>(&self, i: In, cb: C) -> C::Out;
}

impl<In: IntoIterator> Consumer<In> for () {
    type Item = In::Item;

    fn consume<C: Callback<Self::Item>>(&self, i: In, cb: C) -> C::Out {
        cb.call(i.into_iter())
    }
}

#[cfg(test)]
mod tests {
    use ::{pool_harness, IntoSpliterator, Spliterator};

    #[test]
    fn doubling() {
        let v: Vec<_> = (0..5000).collect();

        pool_harness(|pool| {
            let mut v1 = v.clone();
            (&mut v1).into_split_iter().for_each(&pool.spawner(), |x| *x *= 2);

            assert_eq!(v1, (0..5000).map(|x| x * 2).collect::<Vec<_>>());
        });
    }
}