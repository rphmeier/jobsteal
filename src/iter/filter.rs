use super::{Callback, Consumer, Filter, Split, SplitIterator};

const FILTER_COST: f32 = 0.05;

pub struct FilterBase<T>(T);

impl<T: IntoIterator> IntoIterator for FilterBase<T> {
    type Item = T::Item;
    type IntoIter = T::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T: Split> Split for FilterBase<T> {
    fn should_split(&self, mul: f32) -> Option<usize> {
        self.0.should_split(mul + FILTER_COST)
    }

    fn split(self, idx: usize) -> (Self, Self) {
        let (a, b) = self.0.split(idx);

        (FilterBase(a), FilterBase(b))
    }
}

struct FilterCallback<C, F> {
    cb: C,
    pred: F,
}

impl<Item, C: Callback<Item>, F> Callback<Item> for FilterCallback<C, F>
where F: Fn(&Item) -> bool {
    type Out = C::Out;

    fn call<I: Iterator<Item=Item>>(self, iter: I) -> C::Out {
        self.cb.call(iter.filter(self.pred))
    }
}

impl<In: IntoIterator, T: Consumer<In>, F: Sync> Consumer<FilterBase<In>> for Filter<T, F>
where F: Fn(&T::Item) -> bool {
    type Item = T::Item;

    fn consume<C: Callback<Self::Item>>(&self, i: FilterBase<In>, cb: C) -> C::Out {
        let cb = FilterCallback {
            cb: cb,
            pred: &self.pred,
        };

        self.parent.consume(i.0, cb)
    }
}

impl<T: SplitIterator, F: Sync> SplitIterator for Filter<T, F>
where F: Fn(&T::Item) -> bool{
    type Item = T::Item;
    type Base = FilterBase<T::Base>;
    type Consumer = Filter<T::Consumer, F>;

    fn destructure(self) -> (Self::Base, Self::Consumer) {
        let (b, c) = self.parent.destructure();

        (FilterBase(b), Filter { parent: c, pred: self.pred })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (_, u) = self.parent.size_hint();

        (0, u)
    }
}

#[cfg(test)]
mod tests {
    use ::{SplitIterator, IntoSplitIterator, pool_harness};

    #[test]
    fn filter_basics() {
        pool_harness(|pool| {
            let v: Vec<_> = (0..1000).collect();
            v.into_split_iter().filter(|x| *x % 2 == 1).for_each(&pool.spawner(), |x| assert!(x % 2 == 1));
        });
    }
}