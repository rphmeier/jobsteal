use super::{Callback, Consumer, Map, Split, SplitIterator, ExactSizeSplitIterator};

const MAP_COST: f32 = 0.05;

pub struct MapBase<T>(T);

impl<T: IntoIterator> IntoIterator for MapBase<T> {
    type Item = T::Item;
    type IntoIter = T::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T: Split> Split for MapBase<T> {
    fn should_split(&self, mul: f32) -> Option<usize> {
        self.0.should_split(mul + MAP_COST)
    }

    fn split(self, idx: usize) -> (Self, Self) {
        let (a, b) = self.0.split(idx);
        (MapBase(a), MapBase(b))
    }
}

struct MapCallback<C, F> {
    cb: C,
    map: F,
}

impl<Item, C: Callback<U>, F, U> Callback<Item> for MapCallback<C, F>
where F: Fn(Item) -> U {
    type Out = C::Out;

    fn call<I: Iterator<Item=Item>>(self, iter: I) -> C::Out {
        self.cb.call(iter.map(self.map))
    }
}

impl<In: IntoIterator, T, F: Sync, U> Consumer<MapBase<In>> for Map<T, F>
where T: Consumer<In>, F: Fn(T::Item) -> U {
    type Item = U;

    fn consume<C: Callback<U>>(&self, i: MapBase<In>, cb: C) -> C::Out {
        let callback = MapCallback {
            cb: cb,
            map: &self.map,
        };

        self.parent.consume(i.0, callback)
    }
}

impl<T: SplitIterator, F: Sync, U> SplitIterator for Map<T, F>
where F: Fn(T::Item) -> U {
    type Item = U;
    type Base = MapBase<T::Base>;
    type Consumer = Map<T::Consumer, F>;

    fn destructure(self) -> (Self::Base, Self::Consumer) {
        let (b, c) = self.parent.destructure();

        (MapBase(b), Map { parent: c, map: self.map })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.parent.size_hint()
    }
}

impl<T: ExactSizeSplitIterator, F: Sync> ExactSizeSplitIterator for Map<T, F>
where Map<T, F>: SplitIterator {
    fn size(&self) -> usize {
        self.parent.size()
    }
}