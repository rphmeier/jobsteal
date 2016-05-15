use super::{Callback, Consumer, Map, SplitIterator, ExactSizeSplitIterator};

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

impl<T: ExactSizeSplitIterator, F: Sync> ExactSizeSplitIterator for Map<T, F>
where Map<T, F>: SplitIterator {
    fn size(&self) -> usize {
        self.parent.size()
    }
}