use std::collections::BTreeMap;
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use std::vec;

use imdb_index::{
    Index, IndexBuilder, MediaEntity, NameScorer, NgramType, Query, Searcher,
    Similarity,
};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

/// The default truth data used in an evaluation. It's small enough that we
/// embed it directly into the binary.
const TRUTH_DATA: &str = include_str!("../../data/eval/truth.toml");

lazy_static! {
    /// A structured representation of the default truth data.
    static ref TRUTH: Truth = toml::from_str(TRUTH_DATA).unwrap();
}

/// The truth data for our evaluation.
///
/// The truth data consists of a set of information needs that we call "tasks."
#[derive(Clone, Debug, Deserialize)]
struct Truth {
    #[serde(rename = "task")]
    tasks: Vec<Task>,
}

/// A task or "information need" defined by the truth data. Each task
/// corresponds to a query that we feed to the name index, and each task has a
/// single correct answer.
#[derive(Clone, Debug, Deserialize)]
struct Task {
    query: String,
    answer: String,
}

impl Truth {
    /// Load truth data from the given TOML file.
    fn from_path<P: AsRef<Path>>(path: P) -> anyhow::Result<Truth> {
        let path = path.as_ref();

        let mut contents = String::new();
        File::open(path)?.read_to_string(&mut contents)?;
        Ok(toml::from_str(&contents)?)
    }
}

/// A specification for running an evaluation. Fundamentally, a specification
/// describes the thing we want to evaluate, where the thing we want to
/// evaluate is a specific configuration of how we build *and* search an IMDb
/// index.
///
/// A specification describes both how the index should be built and how
/// queries should be generated. Specifications with equivalent index settings
/// may reuse the same on-disk index. For example, the ngram size and type are
/// index settings, but the similarity function, name scorer and result size
/// are all query time settings.
///
/// A specification cannot itself produce a complete query. Namely, a
/// specification requires an information need (called a "task") to construct
/// a query specific to that need. The results of that query are then compared
/// with that information need's answer to determine the score, which is,
/// invariably, a reflection of how well the configuration given by this
/// specification performs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Spec {
    result_size: usize,
    ngram_size: usize,
    ngram_type: NgramType,
    sim: Similarity,
    scorer: Option<NameScorer>,
}

impl Spec {
    /// Create a new spec using a default configuration.
    pub fn new() -> Spec {
        Spec {
            result_size: 30,
            ngram_size: 3,
            ngram_type: NgramType::default(),
            sim: Similarity::None,
            scorer: Some(NameScorer::OkapiBM25),
        }
    }

    /// Set the result size for this specification.
    ///
    /// This returns an error if the given size is less than `1`.
    pub fn with_result_size(
        mut self,
        result_size: usize,
    ) -> anyhow::Result<Spec> {
        if result_size < 1 {
            anyhow::bail!(
                "result size {} is invalid, must be greater than 0",
                result_size
            );
        }
        self.result_size = result_size;
        Ok(self)
    }

    /// Set the ngram size for this specification.
    ///
    /// This returns an error if the given size is less than `2`.
    pub fn with_ngram_size(
        mut self,
        ngram_size: usize,
    ) -> anyhow::Result<Spec> {
        if ngram_size < 2 {
            anyhow::bail!(
                "ngram size {} is invalid, must be greater than 1",
                ngram_size,
            );
        }
        self.ngram_size = ngram_size;
        Ok(self)
    }

    /// Set the ngram type for this specification.
    pub fn with_ngram_type(mut self, ngram_type: NgramType) -> Spec {
        self.ngram_type = ngram_type;
        self
    }

    /// Set the similarity ranker function for this specification.
    pub fn with_similarity(mut self, sim: Similarity) -> Spec {
        self.sim = sim;
        self
    }

    /// Set the name scorer for this specification.
    ///
    /// Note that if the given scorer is `None`, then an evaluation will likely
    /// be quite slow, since each information need will result in an exhaustive
    /// search of the corpus.
    pub fn with_scorer(mut self, scorer: Option<NameScorer>) -> Spec {
        self.scorer = scorer;
        self
    }

    /// Evaluate this specification against the built-in truth data.
    pub fn evaluate<P1: AsRef<Path>, P2: AsRef<Path>>(
        &self,
        data_dir: P1,
        eval_dir: P2,
    ) -> anyhow::Result<Evaluation> {
        let searcher = Searcher::new(self.index(data_dir, eval_dir)?);
        Ok(Evaluation {
            evaluator: Evaluator { spec: self, searcher },
            tasks: TRUTH.clone().tasks.into_iter(),
        })
    }

    /// Evaluate this specification against a set of truth data at the given
    /// file path.
    pub fn evaluate_with<P1: AsRef<Path>, P2: AsRef<Path>, P3: AsRef<Path>>(
        &self,
        data_dir: P1,
        eval_dir: P2,
        truth_path: P3,
    ) -> anyhow::Result<Evaluation> {
        let searcher = Searcher::new(self.index(data_dir, eval_dir)?);
        Ok(Evaluation {
            evaluator: Evaluator { spec: self, searcher },
            tasks: Truth::from_path(truth_path)?.tasks.into_iter(),
        })
    }

    /// Create a query derived from this specification and a particular
    /// information need or "task."
    fn query(&self, task: &Task) -> Query {
        Query::new()
            .name(&task.query)
            .name_scorer(self.scorer.clone())
            .similarity(self.sim.clone())
            .size(self.result_size)
    }

    /// Either open or create an index suitable for this specification.
    ///
    /// If no index exists in the expected sub-directory of `eval_dir`, then
    /// a new index is created.
    fn index<P1: AsRef<Path>, P2: AsRef<Path>>(
        &self,
        data_dir: P1,
        eval_dir: P2,
    ) -> anyhow::Result<Index> {
        let index_dir = self.index_dir(eval_dir.as_ref());
        Ok(if index_dir.exists() {
            Index::open(data_dir, index_dir)?
        } else {
            IndexBuilder::new()
                .ngram_size(self.ngram_size)
                .ngram_type(self.ngram_type)
                .create(data_dir, index_dir)?
        })
    }

    /// The sub-directory of `eval_dir` in which to store this specification's
    /// index.
    fn index_dir<P: AsRef<Path>>(&self, eval_dir: P) -> PathBuf {
        eval_dir.as_ref().join(self.index_name())
    }

    /// The expected name of the index for this evaluation specification.
    ///
    /// The name of the index is derived specifically from this specification's
    /// index-time settings, such as the ngram size. This permits multiple
    /// distinct specifications to reuse the same index.
    fn index_name(&self) -> String {
        format!("ngram-{}_ngram-type-{}", self.ngram_size, self.ngram_type)
    }
}

impl Default for Spec {
    fn default() -> Spec {
        Spec::new()
    }
}

impl fmt::Display for Spec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let scorer = match self.scorer {
            None => "none".to_string(),
            Some(ref scorer) => scorer.to_string(),
        };
        write!(
            f,
            "size-{}_ngram-{}_ngram-type-{}_sim-{}_scorer-{}",
            self.result_size,
            self.ngram_size,
            self.ngram_type,
            self.sim,
            scorer,
        )
    }
}

/// A summary of the results of evaluating every information need or "task" for
/// a single evaluation specification. The summary boils the quality of the
/// specification down to two figures: the mean reciprocal rank and the ratio
/// of tasks that produced an answer.
///
/// The mean reciprocal rank measures the average precision of the
/// specification. That is, it measures how well we answer the following
/// question: "If your search produced the correct answer, how highly was it
/// ranked?"
///
/// The ratio of tasks that produced an answer measures how well we answer the
/// following question: "Of the searches ran, how many of them produced the
/// correct result at any rank?"
///
/// Implicit in the evaluation is the notion of a bounded number of results.
/// That is, every specification dictates the maximum number of results
/// returned by a search. If the answer isn't in that result set, then we stop
/// there and declare that the answer wasn't found.
///
/// The reason for using two different scores is so that they counter balance
/// each other. Namely, a specification that does really well on a smaller
/// number of results might end up with a higher MRR than other specifications,
/// but will have a lower ratio of successful searches.
#[derive(Debug, Deserialize, Serialize)]
pub struct Summary {
    /// The specification name that this result is summarizing.
    pub name: String,
    /// Mean reciprocal rank.
    pub mrr: f64,
    /// The ratio of tasks that found an answer. The higher the better.
    pub found: f64,
}

impl Summary {
    /// Returns a group of summaries for all distinct specifications found
    /// in the back of results given.
    ///
    /// If no results are given, then no summaries are returned.
    pub fn from_task_results(results: &[TaskResult]) -> Vec<Summary> {
        let mut grouped: BTreeMap<&str, Vec<&TaskResult>> = BTreeMap::new();
        for result in results {
            grouped.entry(&result.name).or_insert(vec![]).push(result);
        }

        let mut summaries = vec![];
        for results in grouped.values() {
            summaries.push(Summary::from_same_task_results(results));
        }
        summaries
    }

    /// Returns a summary for a single group of task results. All the results
    /// given must have the same name, otherwise this panics. This also panics
    /// if the given results are empty.
    fn from_same_task_results(results: &[&TaskResult]) -> Summary {
        assert!(!results.is_empty());
        assert!(results.iter().all(|r| results[0].name == r.name));

        let mut precision_sum = 0.0;
        let mut found = 0u64;
        for r in results {
            precision_sum += r.rank.map_or(0.0, |rank| 1.0 / (rank as f64));
            if r.rank.is_some() {
                found += 1;
            }
        }
        Summary {
            name: results[0].name.clone(),
            mrr: precision_sum / (results.len() as f64),
            found: (found as f64) / (results.len() as f64),
        }
    }
}

/// The result of evaluating a single information need or "task."
#[derive(Debug, Deserialize, Serialize)]
pub struct TaskResult {
    /// The name of the evaluation's spec. This name includes all of the
    /// parameters that influence the evaluation, such as ngram size,
    /// similarity function, etc.
    pub name: String,
    /// The freeform text query, which represents a specific manifestation of
    /// this information need. Generally speaking, this corresponds to the
    /// query that an end user will type.
    pub query: String,
    /// The IMDb identifier corresponding to a singular answer expected by an
    /// end user.
    pub answer: String,
    /// If the answer appears in the search results, then this corresponds to
    /// the rank of that search result. The rank is determined by the answer's
    /// absolute position in the list of ranked search results.
    ///
    /// Ties in the ranked list are handled by assigning the maximum possible
    /// rank to each search result with the same score. For example, if we
    /// request 30 results and the answer is incidentally 10th in the list but
    /// every search result has the same score of 1.0, then the rank of our
    /// answer is 30. (Indeed, the rank of every search result is 30 in this
    /// example.)
    pub rank: Option<u64>,
    /// The time it took to execute this query, in seconds.
    pub duration_seconds: f64,
}

/// An evaluation is an iterator over all of the results of evaluating every
/// information need in the truth data.
#[derive(Debug)]
pub struct Evaluation<'s> {
    /// The evaluator, which turns an information need into a `TaskResult`.
    evaluator: Evaluator<'s>,
    /// All of the tasks to evaluate.
    tasks: vec::IntoIter<Task>,
}

impl<'s> Iterator for Evaluation<'s> {
    type Item = anyhow::Result<TaskResult>;

    fn next(&mut self) -> Option<anyhow::Result<TaskResult>> {
        self.tasks.next().map(|task| self.evaluator.run(&task))
    }
}

/// An evaluator is responsible for executing a single search for a single
/// information need. It records the evaluation of that search result in a
/// `TaskResult`.
#[derive(Debug)]
struct Evaluator<'s> {
    /// The evaluation specification.
    spec: &'s Spec,
    /// A handle to a searcher for an IMDb index.
    searcher: Searcher,
}

impl<'s> Evaluator<'s> {
    /// Run this evaluator on a single information need and return the
    /// evaluation.
    fn run(&mut self, task: &Task) -> anyhow::Result<TaskResult> {
        let start = Instant::now();
        let rank = self.rank(task)?;
        let duration = Instant::now().duration_since(start);
        Ok(TaskResult {
            name: self.spec.to_string(),
            query: task.query.clone(),
            answer: task.answer.clone(),
            rank,
            duration_seconds: fractional_seconds(&duration),
        })
    }

    /// Execute the search for the given information need and determine the
    /// rank of the expected answer for the given information need. If the
    /// expected answer didn't appear in the search results, then `None` is
    /// returned.
    ///
    /// The rank of the answer is determined in exactly the way you might
    /// expect: if the answer appears as the Nth result in a search, then its
    /// rank is N. There is one tricky part of this, and it is specifically in
    /// how we break ties. Stated succinctly, we always take the maximum
    /// possible rank of a result. For example, given the following results,
    /// where the first column is the score, the second column is the
    /// result name, and the third column is the *intuitive* rank:
    ///
    ///     1.0  a  1
    ///     1.0  b  1
    ///     1.0  c  1
    ///     0.9  d  4
    ///     0.8  e  5
    ///     0.8  f  5
    ///     0.7  g  7
    ///
    /// Namely, records that are tied all get assigned the same rank, and the
    /// next result with a lower score is assigned a rank equivalent to its
    /// absolute position in the result list.
    ///
    /// The problem with this ranking strategy is that it biases toward rankers
    /// that have a naive score. In particular, so long as a search returns the
    /// answer in the results, it could assign a score of `1.0` to every
    /// result and get a maximal RR (Reciprocal Rank) evaluation.
    ///
    /// Instead, we invert how results are ranked. The above example is instead
    /// ranked like so:
    ///
    ///     1.0  a  3
    ///     1.0  b  3
    ///     1.0  c  3
    ///     0.9  d  4
    ///     0.8  e  6
    ///     0.8  f  6
    ///     0.7  g  7
    ///
    /// In other words, we assign the maximal possible rank instead of the
    /// minimal possible rank.
    ///
    /// There are other strategies, but in general, we want to reward high
    /// precision rankers.
    fn rank(&mut self, task: &Task) -> anyhow::Result<Option<u64>> {
        let results = self.searcher.search(&self.spec.query(&task))?;

        let mut rank = results.len() as u64;
        let mut prev_score = None;
        let mut ranked: Vec<(u64, MediaEntity)> = vec![];
        for (i, scored) in results.into_iter().enumerate().rev() {
            let (score, entity) = scored.into_pair();
            if prev_score.map_or(true, |s| !approx_eq(s, score)) {
                rank = i as u64 + 1;
                prev_score = Some(score);
            }
            ranked.push((rank, entity));
        }
        ranked.reverse();

        for (rank, entity) in ranked {
            if entity.title().id == task.answer {
                return Ok(Some(rank));
            }
        }
        Ok(None)
    }
}

/// Compares two floating point numbers for equality approximately for some
/// epsilon.
fn approx_eq(x1: f64, x2: f64) -> bool {
    // We used a fixed error because it's good enough in practice.
    (x1 - x2).abs() <= 0.0000000001
}

/// Returns the number of seconds in this duration in fraction form.
/// The number to the left of the decimal point is the number of seconds,
/// and the number to the right is the number of milliseconds.
fn fractional_seconds(d: &Duration) -> f64 {
    let fractional = (d.subsec_nanos() as f64) / 1_000_000_000.0;
    d.as_secs() as f64 + fractional
}

#[cfg(test)]
mod tests {
    use imdb_index::{NameScorer, NgramType, Similarity};

    use super::Spec;

    #[test]
    fn spec_printer() {
        let spec = Spec {
            result_size: 30,
            ngram_size: 3,
            ngram_type: NgramType::Window,
            sim: Similarity::None,
            scorer: Some(NameScorer::OkapiBM25),
        };
        let expected =
            "size-30_ngram-3_ngram-type-window_sim-none_scorer-okapibm25";
        assert_eq!(spec.to_string(), expected);

        let spec = Spec {
            result_size: 1,
            ngram_size: 2,
            ngram_type: NgramType::Edge,
            sim: Similarity::Jaro,
            scorer: None,
        };
        let expected = "size-1_ngram-2_ngram-type-edge_sim-jaro_scorer-none";
        assert_eq!(spec.to_string(), expected);
    }
}
