//! Vector-related Spliterator.

use super::{Split, ExactSizeSpliterator, IntoSpliterator};

use std::sync::Arc;
use std::ptr;

// a wrapper around a vector which doesn't leak memory, but does not drop the contents when
// it is dropped.
// this is because the iterator reads out of the vector, and we don't want to
// double-drop.
struct NoDropVec<T> {
	inner: Vec<T>,
}

impl<T> Drop for NoDropVec<T> {
	fn drop(&mut self) {
		unsafe { self.inner.set_len(0) }
	}
}

// required by Arc. VecSplit/Iter never actually uses the data in the vector
// except to move out of it, and it never aliases.
unsafe impl<T: Send> Sync for NoDropVec<T> {}

/// The iterator which `VecSplit<T>` will turn when done splitting.
pub struct Iter<T> {
	_items: Arc<NoDropVec<T>>,
	cur: *mut T,
	end: *mut T,
}

impl<T> Iterator for Iter<T> {
	type Item = T;

	fn next(&mut self) -> Option<T> {
		if self.cur == self.end {
			None
		} else {
			let item = unsafe { ptr::read(self.cur) };
			self.cur = unsafe { self.cur.offset(1) };
			Some(item)
		}
	}
}

// drop all the remaining items, just to be nice.
impl<T> Drop for Iter<T> {
	fn drop(&mut self) {
		while let Some(_) = self.next() {}
	}
}

/// A `Spliterator` for vectors.
pub struct VecSplit<T> {
	// keeps the data alive until it is consumed.
	items: Arc<NoDropVec<T>>,
	start: usize,
	len: usize,
}

impl<T> IntoIterator for VecSplit<T> {
	type IntoIter = Iter<T>;
	type Item = T;

	fn into_iter(self) -> Self::IntoIter {
		let cur_ptr = unsafe { self.items.inner.as_ptr().offset(self.start as isize) };
		let end_ptr = unsafe { cur_ptr.offset(self.len as isize) };

		Iter {
			_items: self.items,
			cur: cur_ptr as *mut T,
			end: end_ptr as *mut T,
		}
	}
}

impl<T: Send> Split for VecSplit<T> {
    fn should_split(&self, mul: f32) -> Option<usize> {
        if self.len > 1 && (self.len as f32 *mul) > 4096.0 { Some(self.len / 2) }
        else { None }
    }

    fn split(self, mut idx: usize) -> (Self, Self) {
        if idx > self.len { idx = self.len }

		(
			VecSplit { items: self.items.clone(), start: self.start, len: idx},
			VecSplit { items: self.items, start: self.start + idx, len: self.len - idx }
		)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

impl<T: Send> ExactSizeSpliterator for VecSplit<T> {
	fn size(&self) -> usize {
		self.len
	}
}

impl<T: Send> IntoSpliterator for Vec<T> {
	type Item = T;
	type SplitIter = VecSplit<T>;

	fn into_split_iter(self) -> VecSplit<T> {
		let len = self.len();

		VecSplit {
			items: Arc::new(NoDropVec { inner: self }),
			start: 0,
			len: len,
		}
	}
}

#[cfg(test)]
mod tests {
	use ::{IntoSpliterator, Spliterator, make_pool};

	#[test]
	fn it_works() {
		let mut pool = make_pool(4).unwrap();

		let v: Vec<_> = (0..10000).collect();
		let doubled: Vec<_> = v.into_split_iter().map(|x| x * 2).collect(&pool.spawner());

		assert_eq!(doubled, (0..10000).map(|x| x*2).collect::<Vec<_>>());
	}
}