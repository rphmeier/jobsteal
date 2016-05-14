use super::{Callback, Consumer, Filter, SplitIterator};

struct FilterCallback<C, F> {
    cb: C,
    pred: F,
}

impl<Item, C: Callback<Item>, F> Callback<Item> for FilterCallback<C, F>
where F: Fn(&Item) -> bool {
    fn call<I: Iterator<Item=Item>>(self, iter: I) {
        self.cb.call(iter.filter(self.pred))
    }
}

impl<In: IntoIterator, T: Consumer<In>, F: Sync> Consumer<In> for Filter<T, F>
where F: Fn(&T::Item) -> bool {
    type Item = T::Item;

    fn consume<C: Callback<Self::Item>>(&self, i: In, cb: C) {
        let cb = FilterCallback {
            cb: cb,
            pred: &self.pred,
        };

        self.parent.consume(i, cb);
    }
}

impl<T: SplitIterator, F: Sync> SplitIterator for Filter<T, F>
where F: Fn(&T::Item) -> bool{
    type Item = T::Item;
    type Base = T::Base;
    type Consumer = Filter<T::Consumer, F>;

    fn destructure(self) -> (Self::Base, Self::Consumer) {
        let (b, c) = self.parent.destructure();

        (b, Filter { parent: c, pred: self.pred })
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