use super::{Callback, Consumer, CostMul, Hide, Split, Spliterator, ExactSizeSpliterator};

pub struct CostMulConsumer<T>(T);

impl<In: IntoIterator, T> Consumer<Hide<CostMul<In>>> for CostMulConsumer<T>
where T: Consumer<In> {
    type Item = T::Item;

    fn consume<C: Callback<Self::Item>>(&self, i: Hide<CostMul<In>>, cb: C) -> C::Out {
        self.0.consume(i.0.parent, cb)
    }
}

impl<T: IntoIterator> IntoIterator for Hide<CostMul<T>> {
    type Item = T::Item;
    type IntoIter = T::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.parent.into_iter()
    }
}

impl<T: Split> Split for Hide<CostMul<T>> {
    fn should_split(&self, mul: f32) -> Option<usize> {
        self.0.parent.should_split(mul * self.0.mul)
    }

    fn split(self, idx: usize) -> (Self, Self) {
        let (a, b) = self.0.parent.split(idx);

        (
            Hide(CostMul { parent: a, mul: self.0.mul }),
            Hide(CostMul { parent: b, mul: self.0.mul }),
        )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.parent.size_hint()
    }
}

impl<T: Spliterator> Spliterator for CostMul<T> {
    type Item = T::Item;
    type Base = Hide<CostMul<T::Base>>;
    type Consumer = CostMulConsumer<T::Consumer>;

    fn destructure(self) -> (Self::Base, Self::Consumer) {
        let (b, c) = self.parent.destructure();
        (Hide(CostMul { parent: b, mul: self.mul }), CostMulConsumer(c))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.parent.size_hint()
    }
}

impl<T: ExactSizeSpliterator>  ExactSizeSpliterator for CostMul<T> {
    fn size(&self) -> usize {
        self.parent.size()
    }
}