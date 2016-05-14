use super::{Callback, Consumer, Hide, Split, SplitIterator, Zip};

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