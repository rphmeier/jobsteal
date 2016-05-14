use super::Spawner;

/// A parallel iterator which works by splitting the underlying data
/// and sharing it between threads.
pub trait SplitIterator: Sized {
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

    /// Zip this iterator with another, combining their items
    /// in a tuple.
    fn zip<B: SplitIterator>(self, other: B) -> Zip<Self, B> {
        Zip {
            a: self,
            b: other,
        }
    }

    /// Map the items of this iterator to another type using the supplied function.
    fn map<F: Sync, U>(self, map: F) -> Map<Self, F> where F: Fn(Self::Item) -> U {
        Map {
            parent: self,
            map: map,
        }
    }
}

/// Data which can be split in two at an index.
pub trait Split: Send + IntoIterator {
    /// Whether this should split, returning an index
    /// which would be best.
    fn should_split(&self) -> Option<usize>;

    /// Split the data at the specified index.
    /// Note that this may not always be the same as the index
    /// you return from should_split.
    fn split(self, usize) -> (Self, Self);
}

/// Things that can be turned into a `SplitIterator`.
pub trait IntoSplitIterator {
    /// The item the split iterator will produce.
    type Item;

    /// The iterator this will turn into.
    type SplitIter: SplitIterator<Item=Self::Item>;

    fn into_split_iter(self) -> Self::SplitIter;
}

impl<T: SplitIterator> IntoSplitIterator for T {
    type Item = <Self as SplitIterator>::Item;
    type SplitIter = Self;

    fn into_split_iter(self) -> Self::SplitIter {
        self
    }
}

impl<'a, T: 'a + Sync> IntoSplitIterator for &'a [T] {
    type Item = &'a T;
    type SplitIter = SliceSplit<'a, T>;

    fn into_split_iter(self) -> Self::SplitIter {
        SliceSplit(self)
    }
}

impl<'a, T: 'a + Sync + Send> IntoSplitIterator for &'a mut [T] {
    type Item = &'a mut T;
    type SplitIter = SliceSplitMut<'a, T>;

    fn into_split_iter(self) -> Self::SplitIter {
        SliceSplitMut(self)
    }
}

impl<'a, T: 'a + Sync> IntoSplitIterator for &'a Vec<T> {
    type Item = &'a T;
    type SplitIter = SliceSplit<'a, T>;

    fn into_split_iter(self) -> Self::SplitIter {
        SliceSplit(&self)
    }
}

impl<'a, T: 'a + Sync + Send> IntoSplitIterator for &'a mut Vec<T> {
    type Item = &'a mut T;
    type SplitIter = SliceSplitMut<'a, T>;

    fn into_split_iter(self) -> Self::SplitIter {
        SliceSplitMut(self)
    }
}

/// Used to mask data so that implementations don't conflict.
///
/// Specifically, this is used in the implementation of `SplitIterator`
/// for `Zip`, by hiding the base. This is because `SplitIterator` requires
/// that the base be `Split`. However, for `Split` types, there is a blanket
/// impl for `SplitIterator` for convenience, so if we didn't mask the type,
/// there would be conflicting implementations of `SplitIterator` for `Zip`.
/// This will be obsoleted when specialization becomes stable.
pub struct Hide<T>(T);

/// A split iterator over an immutable slice.
pub struct SliceSplit<'a, T: 'a>(&'a [T]);

/// A split iterator over a mutable slice.
pub struct SliceSplitMut<'a, T: 'a>(&'a mut [T]);

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
    fn should_split(&self) -> Option<usize> {
        let len = self.0.len();
        if len > 4096 { Some(len / 2) }
        else { None }
    }

    fn split(self, idx: usize) -> (Self, Self) {
        let (a, b) = self.0.split_at(idx);
        (
            SliceSplit(a),
            SliceSplit(b),
        )
    }
}

impl<'a, T: 'a + Sync + Send> Split for SliceSplitMut<'a, T> {
    fn should_split(&self) -> Option<usize> {
        let len = self.0.len();
        if len > 4096 { Some(len / 2) }
        else { None }
    }

    fn split(self, idx: usize) -> (Self, Self) {
        let (a, b) = self.0.split_at_mut(idx);
        (
            SliceSplitMut(a),
            SliceSplitMut(b),
        )
    }
}

pub trait Callback<T>: Sized {
    fn call<I: Iterator<Item=T>>(self, I);
}

impl<F, T> Callback<T> for F where F: FnMut(T) {
    fn call<I: Iterator<Item=T>>(mut self, iter: I) {
        for i in iter {
            (self)(i)
        }
    }
}

pub trait Consumer<In: IntoIterator>: Sync {
    type Item;

    /// Consume the iterator, typically by passing it on to the
    /// parent consumer along with a callback which will receive
    /// a producer of items to transform.
    fn consume<C: Callback<Self::Item>>(&self, i: In, cb: C);
}

impl<In: IntoIterator> Consumer<In> for () {
    type Item = In::Item;

    fn consume<C: Callback<Self::Item>>(&self, i: In, cb: C) {
        cb.call(i.into_iter())
    }
}

impl<T: Split> SplitIterator for T {
    type Item = T::Item;
    type Base = Self;
    type Consumer = ();

    fn destructure(self) -> (Self, ()) {
        (self, ())
    }
}

/// Map iterator adapter.
///
/// This transforms each element into a new object.
pub struct Map<T, F> {
    parent: T,
    map: F,
}

struct MapCallback<C, F> {
    cb: C,
    map: F,
}

impl<Item, C: Callback<U>, F, U> Callback<Item> for MapCallback<C, F>
where F: Fn(Item) -> U {
    fn call<I: Iterator<Item=Item>>(self, iter: I) {
        self.cb.call(iter.map(self.map));
    }
}

impl<In: IntoIterator, T, F: Sync, U> Consumer<In> for Map<T, F>
where T: Consumer<In>, F: Fn(T::Item) -> U {
    type Item = U;

    fn consume<C: Callback<U>>(&self, i: In, cb: C) {
        let callback = MapCallback {
            cb: cb,
            map: &self.map,
        };

        self.parent.consume(i, callback);
    }
}

impl<T: SplitIterator, F: Sync, U> SplitIterator for Map<T, F>
where F: Fn(T::Item) -> U {
    type Item = U;
    type Base = T::Base;
    type Consumer = Map<T::Consumer, F>;

    fn destructure(self) -> (Self::Base, Self::Consumer) {
        let (b, c) = self.parent.destructure();

        (b, Map { parent: c, map: self.map })
    }
}

/// Zip iterator adapter.
///
/// This combines two other iterators into one.
pub struct Zip<A, B> {
    a: A,
    b: B,
}

// Okay, the callback situation gets a little hairy with Zip...
//
// The idea is that a Zip which is a consumer is a Zip combining two
// other consumers. The input is a Zip combining two IntoIterators and
// a callback which wants a zipped iterator of the two consumers' item types.
// The problem is that the callback design only allows for getting an iterator
// from one consumer at a time. So we `consume` the first consumer, passing a callback
// which stores the other consumer along with the iterator that the second consumer
// will eventually consume. On top of that, this callback will store the original
// callback, which is expecting a zipped iterator.
//
// When this callback is called, it will receive an iterator of the first item type along
// with self by-value. Now, remember that self is storing the second consumer,
// the second consumer's argument, and the final callback. So the next step is to
// create one more callback which store the iterator we just received along with the
// final callback. We have the second consumer consume its iterator, and pass this new
// callback to it. When that second callback is called, it will receive an iterator of
// the second item as an argument. At that point, we have an iterator of each item type and the
// final callback all in one place. So we just zip them together, shoot them into the final
// callback, and we're done!
//
// ...there is probably a better way.
struct ZipCallbackA<'a, ConsB: 'a, InB, C> {
    consumer_b: &'a ConsB,
    in_b: InB,
    cb: C
}

struct ZipCallbackB<IterA, C> {
    iter: IterA,
    cb: C,
}

impl<'a, ItemA, InB, ConsB: 'a, C> Callback<ItemA> for ZipCallbackA<'a, ConsB, InB, C>
where ConsB: Consumer<InB>, InB: IntoIterator,
      C: Callback<(ItemA, ConsB::Item)> {
    fn call<I: Iterator<Item=ItemA>>(self, iter: I) {
        // consume B's consumer with a new callback, which stores
        // the iterator we just received.
        let b_cb = ZipCallbackB {
            iter: iter,
            cb: self.cb,
        };

        self.consumer_b.consume(self.in_b, b_cb);
    }
}

impl<ItemA, IterA, C, ItemB> Callback<ItemB> for ZipCallbackB<IterA, C>
where IterA: Iterator<Item=ItemA>, C: Callback<(ItemA, ItemB)> {
    fn call<IterB: Iterator<Item=ItemB>>(self, iter_b: IterB) {
        // we are receiving an iterator for ItemB, and we are storing
        // an iterator for ItemB along with a callback expecting an
        // iterator of (ItemA, ItemB). Let's give it what it wants.
        self.cb.call(self.iter.zip(iter_b));
    }
}

impl<InA, A, InB, B> Consumer<Hide<Zip<InA, InB>>> for Zip<A, B>
where A: Consumer<InA>, B: Consumer<InB>,
      InA: IntoIterator, InB: IntoIterator {
    type Item = (A::Item, B::Item);

    fn consume<C: Callback<Self::Item>>(&self, i: Hide<Zip<InA, InB>>, cb: C) {
        let a_cb = ZipCallbackA {
            consumer_b: &self.b,
            in_b: i.0.b,
            cb: cb,
        };

        self.a.consume(i.0.a, a_cb);
    }
}

impl<A: SplitIterator, B: SplitIterator> SplitIterator for Zip<A, B> {
    type Item = (A::Item, B::Item);
    type Base = Hide<Zip<A::Base, B::Base>>;
    type Consumer = Zip<A::Consumer, B::Consumer>;

    fn destructure(self) -> (Self::Base, Self::Consumer) {
        let (a_b, a_c) = self.a.destructure();
        let (b_b, b_c) = self.b.destructure();

        (
            Hide(Zip { a: a_b, b: b_b }),
            Zip { a: a_c, b: b_c },
        )
    }
}

impl<A: IntoIterator, B: IntoIterator> IntoIterator for Hide<Zip<A, B>> {
    type Item = (A::Item, B::Item);
    type IntoIter = ::std::iter::Zip<A::IntoIter, B::IntoIter>;

    fn into_iter(self) -> Self::IntoIter {
        let z = self.0;
        z.a.into_iter().zip(z.b.into_iter())
    }
}

impl<A: Split, B: Split> Split for Hide<Zip<A, B>> {
    fn should_split(&self) -> Option<usize> {
        let z = &self.0;
        match (z.a.should_split(), z.a.should_split()) {
            (Some(a), Some(b)) => {
                Some(if a > b { b } else { a })
            }
            _ => None
        }
    }

    fn split(self, idx: usize) -> (Self, Self) {
        let z = self.0;
        let (a1, a2) = z.a.split(idx);
        let (b1, b2) = z.b.split(idx);

        (
            Hide(Zip { a: a1, b: b1 }),
            Hide(Zip { a: a2, b: b2 }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test1() {
        let v1 = vec![1, 2, 3];
        let v2 = vec![4, 5, 6];

        let s1 = SliceSplit(&v1);
        let s2 = SliceSplit(&v2);

        let diff = 1;
        let i = s1.map(|x| x + diff).zip(s2).map(|(a, b)| a + b);

        let (base, consumer) = i.destructure();
        consumer.consume(base, |x| println!("{:?}", x));
    }
}