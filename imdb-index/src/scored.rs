use std::cmp;
use std::collections::BinaryHeap;
use std::num::FpCategory;
use std::vec;

/// A collection of scored values, sorted in descending order by score.
#[derive(Clone, Debug, Default)]
pub struct SearchResults<T>(Vec<Scored<T>>);

impl<T> SearchResults<T> {
    /// Create an empty collection of scored values.
    pub fn new() -> SearchResults<T> {
        SearchResults(vec![])
    }

    /// Create a collection of search results from a min-heap of scored values.
    pub fn from_min_heap(
        queue: &mut BinaryHeap<cmp::Reverse<Scored<T>>>,
    ) -> SearchResults<T> {
        let mut results = vec![];
        while let Some(x) = queue.pop() {
            results.push(x.0);
        }
        results.reverse();
        SearchResults(results)
    }

    /// Add a new scored value to this collection.
    ///
    /// The score provided must be less than or equal to every other score in
    /// this collection, otherwise this method will panic.
    pub fn push(&mut self, scored: Scored<T>) {
        assert!(self.0.last().map_or(true, |smallest| &scored <= smallest));
        self.0.push(scored);
    }

    /// Normalizes the scores in this collection such that all scores are in
    /// the range `[0, 1]` where the top result always has score `1.0`.
    ///
    /// This operation is idempotent and does not change the ordering of
    /// results.
    pub fn normalize(&mut self) {
        if let Some(top_score) = self.0.get(0).map(|s| s.score()) {
            // The minimal score is 0, so if the top score is 0, then all
            // scores must be 0. No normalization needed. (And we avoid a
            // divide-by-zero below.)
            if top_score.classify() == FpCategory::Zero {
                return;
            }
            for result in &mut self.0 {
                let score = result.score();
                result.set_score(score / top_score);
            }
        }
    }

    /// Recomputes the scores in this collection using the given function.
    ///
    /// The results are then re-sorted according to the new scores.
    pub fn rescore<F: FnMut(&T) -> f64>(&mut self, mut rescore: F) {
        for result in &mut self.0 {
            let score = rescore(result.value());
            result.set_score(score);
        }
        self.0.sort_by(|s1, s2| s1.cmp(&s2).reverse());
    }

    /// Trim this collection so that it contains at most the first `size`
    /// results.
    pub fn trim(&mut self, size: usize) {
        if self.0.len() > size {
            self.0.drain(size..);
        }
    }

    /// Returns the number of results in this collection.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns true if and only if this collection is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Return a slice of search results in order.
    pub fn as_slice(&self) -> &[Scored<T>] {
        &self.0
    }

    /// Consume this collection and return the underlying sorted sequence of
    /// scored values.
    pub fn into_vec(self) -> Vec<Scored<T>> {
        self.0
    }
}

impl<T> IntoIterator for SearchResults<T> {
    type IntoIter = vec::IntoIter<Scored<T>>;
    type Item = Scored<T>;

    fn into_iter(self) -> vec::IntoIter<Scored<T>> {
        self.into_vec().into_iter()
    }
}

/// Any value associated with a score.
///
/// We define Eq and Ord on this type in a way that ignores `value` and only
/// uses the `score` to determine ordering. The public API of `Scored`
/// guarantees that scores are never `NaN`.
#[derive(Clone, Copy, Debug)]
pub struct Scored<T> {
    score: f64,
    value: T,
}

impl<T> Scored<T> {
    /// Create a new value `T` with a score of `1.0`.
    pub fn new(value: T) -> Scored<T> {
        Scored { score: 1.0, value: value }
    }

    /// Return the score for this item.
    ///
    /// In general, no restrictions are placed on the range of scores, however
    /// most search APIs that use it will return scores in the range `[0, 1]`.
    ///
    /// The score returned is guaranteed to never be `NaN`.
    pub fn score(&self) -> f64 {
        self.score
    }

    /// Set the score, replacing the existing value with the given value.
    ///
    /// This panics if the given score is `NaN`.
    pub fn set_score(&mut self, score: f64) {
        assert!(score.is_finite());
        self.score = score;
    }

    /// Consume this scored value and return a new scored value that drops the
    /// existing score and replaces it with the given score.
    ///
    /// This panics if the given score is `NaN`.
    pub fn with_score(mut self, score: f64) -> Scored<T> {
        self.set_score(score);
        self
    }

    /// Consume this scored value and map its value using the function given,
    /// returning a new scored value with the result of the map and an
    /// unchanged score.
    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> Scored<U> {
        Scored { score: self.score, value: f(self.value) }
    }

    /// Consume this scored value and map its score using the function given,
    /// return a new `Scored` with an unchanged value.
    ///
    /// This panics if score returned by `f` is `NaN`.
    pub fn map_score<F: FnOnce(f64) -> f64>(self, f: F) -> Scored<T> {
        let score = f(self.score);
        self.with_score(score)
    }

    /// Return a reference to the underlying value.
    pub fn value(&self) -> &T {
        &self.value
    }

    /// Consume this scored value, drop the score and return the underlying
    /// `T`.
    pub fn into_value(self) -> T {
        self.value
    }

    /// Consume this scored value and return the underlying pair of score and
    /// `T`.
    pub fn into_pair(self) -> (f64, T) {
        (self.score, self.value)
    }
}

impl<T: Default> Default for Scored<T> {
    fn default() -> Scored<T> {
        Scored::new(T::default())
    }
}

impl<T> Eq for Scored<T> {}

impl<T> PartialEq for Scored<T> {
    fn eq(&self, other: &Scored<T>) -> bool {
        let (s1, s2) = (self.score, other.score);
        s1 == s2
    }
}

impl<T> Ord for Scored<T> {
    fn cmp(&self, other: &Scored<T>) -> cmp::Ordering {
        self.score.partial_cmp(&other.score).unwrap()
    }
}

impl<T> PartialOrd for Scored<T> {
    fn partial_cmp(&self, other: &Scored<T>) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use std::f64::NAN;
    use super::Scored;

    #[test]
    #[should_panic]
    fn never_nan_1() {
        Scored::new(()).set_score(NAN);
    }

    #[test]
    #[should_panic]
    fn never_nan_2() {
        Scored::new(()).with_score(NAN);
    }

    #[test]
    #[should_panic]
    fn never_nan_3() {
        Scored::new(()).map_score(|_| NAN);
    }
}
