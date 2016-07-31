use super::{Callback, Consumer, FilterMap, Split, Spliterator};

const FILTER_MAP_COST: f32 = 0.05;

pub struct FilterMapBase<T>(T);

impl<T: IntoIterator> IntoIterator for FilterMapBase<T> {
	type Item = T::Item;
	type IntoIter = T::IntoIter;

	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl<T: Split> Split for FilterMapBase<T> {
	fn should_split(&self, mul: f32) -> Option<usize> {
		self.0.should_split(mul + FILTER_MAP_COST)
	}

	fn split(self, idx: usize) -> (Self, Self) {
		let (a, b) = self.0.split(idx);

		(FilterMapBase(a), FilterMapBase(b))
	}
}

struct CB<C, F> {
	cb: C,
	pred: F,
}

impl<In, Out, C: Callback<Out>, F> Callback<In> for CB<C, F>
where F: Fn(In) -> Option<Out> {
	type Out = C::Out;

	fn call<I: Iterator<Item=In>>(self, iter: I) -> C::Out {
		self.cb.call(iter.filter_map(self.pred))
	}
}

impl<In: IntoIterator, T, F: Sync, U> Consumer<FilterMapBase<In>> for FilterMap<T, F>
where T: Consumer<In>, F: Fn(T::Item) -> Option<U> {
	type Item = U;

	fn consume<C: Callback<U>>(&self, i: FilterMapBase<In>, cb: C) -> C::Out {
		let callback = CB {
			cb: cb,
			pred: &self.pred,
		};

		self.parent.consume(i.0, callback)
	}
}

impl<T: Spliterator, U, F: Sync> Spliterator for FilterMap<T, F>
where F: Fn(T::Item) -> Option<U> {
	type Item = U;
	type Base = FilterMapBase<T::Base>;
	type Consumer = FilterMap<T::Consumer, F>;

	fn destructure(self) -> (Self::Base, Self::Consumer) {
		let (b, c) = self.parent.destructure();

		(FilterMapBase(b), FilterMap { parent: c, pred: self.pred })
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		let (_, u) = self.parent.size_hint();

		(0, u)
	}
}

#[cfg(test)]
mod tests {
	use ::{Spliterator, IntoSpliterator, pool_harness};

	#[test]
	fn filter_map() {
		pool_harness(|pool| {
			fn foo(x: i32) -> Option<i32> {
				if x % 5 == 0 {
					None
				} else {
					Some(x * 2)
				}
			}

			let v: Vec<_> = (0..1000).collect();
			v.into_split_iter().filter_map(foo).for_each(&pool.spawner(), |x| {
				assert!(x % 5 != 0);
				assert!(x % 2 == 0);
			})
		})
	}
}