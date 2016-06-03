use super::{Callback, Consumer, FlatMap, Split, Spliterator};

const FLAT_MAP_MUL: f32 = 1.5;

pub struct FlatMapBase<T>(T);

impl<T: IntoIterator> IntoIterator for FlatMapBase<T> {
    type Item = T::Item;
    type IntoIter = T::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T: Split> Split for FlatMapBase<T> {
    fn should_split(&self, mul: f32) -> Option<usize> {
        self.0.should_split(mul * FLAT_MAP_MUL)
    }

    fn split(self, idx: usize) -> (Self, Self) {
        let (a, b) = self.0.split(idx);

        (FlatMapBase(a), FlatMapBase(b))
    }
}

impl<T: Spliterator, F, U> Spliterator for FlatMap<T, F>
where F: Sync + Fn(T::Item) -> U, U: IntoIterator {
    type Item = U::Item;
    type Base = FlatMapBase<T::Base>;
    type Consumer = FlatMap<T::Consumer, F>;

    fn destructure(self) -> (Self::Base, Self::Consumer) {
        let (b, c) = self.parent.destructure();
        (FlatMapBase(b), FlatMap { parent: c, flat_map: self.flat_map })
    }

    // no size hint, since the yielded iterators could be all empty or infinite.
}

struct FlatMapCallback<C, F> {
    cb: C,
    flat_map: F,
}

impl<C, F, T, U> Callback<T> for FlatMapCallback<C, F>
where C: Callback<U::Item>, F: Sync + Fn(T) -> U, U: IntoIterator {
    type Out = C::Out;

    fn call<I: Iterator<Item=T>>(self, iter: I) -> Self::Out {
        self.cb.call(iter.flat_map(self.flat_map))
    }
}

impl<In: IntoIterator, T: Consumer<In>, F, I> Consumer<FlatMapBase<In>> for FlatMap<T, F>
where F: Sync + Fn(T::Item) -> I, I: IntoIterator {
    type Item = I::Item;

    fn consume<C: Callback<Self::Item>>(&self, i: FlatMapBase<In>, cb: C) -> C::Out {
        let cb = FlatMapCallback {
            cb: cb,
            flat_map: &self.flat_map,
        };

        self.parent.consume(i.0, cb)
    }
}

#[cfg(test)]
mod tests {
    use ::{IntoSpliterator, Spliterator, pool_harness};

    #[test]
    fn flat_map_basics() {
        let v = (0..5000).collect::<Vec<_>>();
        pool_harness(|pool| {
             let v2 = (&v).into_split_iter()
                .map(|&x| x)
                .flat_map(::std::iter::once)
                .collect::<Vec<_>>(&pool.spawner());

             assert_eq!(v, v2);
        });
    }
}