use std::cmp;
use std::collections::binary_heap;
use std::collections::BinaryHeap;
use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use std::str::{self, FromStr};
use std::time::Instant;

use byteorder::{ByteOrder, LE};
use failure::ResultExt;
use fnv::FnvHashMap;
use fst;
use memmap::Mmap;
use serde_json;

use error::{Error, ErrorKind, Result};
use index::writer::CursorWriter;
use scored::{Scored, SearchResults};
use util::{fst_map_builder_file, fst_map_file, mmap_file, open_file, NiceDuration};

/// The name of the file containing the index configuration.
///
/// The index configuration is a JSON file with some meta data about this
/// index, such as its version, ngram size and aggregate statistics about the
/// corpus that has been indexed.
const CONFIG: &str = "names.config.json";

/// The name of the ngram term index.
///
/// The ngram term index maps ngrams (fixed size sequences of Unicode
/// codepoints) to file offsets. Each file offset points to the postings for
/// the corresponding term.
const NGRAM: &str = "names.ngram.fst";

/// The name of the postings list index.
///
/// The postings list contains an entry for every term in the ngram index.
/// Each entry corresponds to a list of document/frequency pairs. Namely, each
/// entry is a DocID and a frequency count indicating how many times the
/// corresponding term appeared in that document. Each entry in the list is
/// encoded as a single 32 little-endian integer. The high 4 bits represent
/// the frequency (which is capped at 15, a reasonable number for indexing
/// short name strings) while the low 28 bits represent the doc id. The
/// `MAX_DOC_ID` constant below ensures we make sure to never use a doc id
/// that won't fit this encoding scheme.
///
/// The last eight bytes in the postings index contains a 64-bit little-endian
/// encoded integer indicating the average length of all documents represented
/// by the ngram index. The length is recorded in units of terms, which
/// generally correspond to the total number of ngrams in a name.
const POSTINGS: &str = "names.postings.idx";

/// The name of the identifier map index.
///
/// This file maps `DocID`s to `NameID`s. It consists of a sequence of
/// 64-bit little-endian encoded integers, where the length of the sequence
/// corresponds to the total number of names in the index. Each entry in the
/// sequence encodes a `NameID`. In other words, the index to this sequence is
/// a `DocID` and the value at that index is a `NameID`.
///
/// The id map is used to map doc ids returned by the postings to name ids
/// which were provided by the caller. This also permits search to deduplicate
/// results. That is, we should never return multiple results for the same
/// NameID, even though we may have indexed multiple names for the same name
/// id.
const IDMAP: &str = "names.idmap.idx";

/// The name of the document length index.
///
/// This file consists of a sequence of 16-bit little-endian encoded
/// integers, where the length of the sequence corresponds to the total number
/// of names in the index. Each entry represents the length, in terms, of each
/// name.
///
/// The lengths are used during scoring to compute a normalization term. This
/// allows the scoring mechanism to take document length into account.
const NORMS: &str = "names.norms.idx";

/// The external identifier for every distinct record represented by this name
/// index. There are no restrictions on name ids, and multiple names may be
/// indexed that correspond to the same name id.
///
/// With respect to IMDb, there is a 1-to-1 correspondence between the records
/// in title.basics.tsv and the set of NameIDs, even though there may be
/// multiple names for each record.
///
/// For IMDb, this is represented by the byte offset of the corresponding
/// record in title.basics.tsv. This provides constant time lookup to full
/// record. Note, though, that this module knows nothing about such things.
/// To this module, name ids are opaque identifiers.
pub type NameID = u64;

/// An internal surrogate identifier for every distinct name in the index. Note
/// that multiple distinct doc ids can map to the same name id. For example, if
/// a name has multiple distinct forms, then they each get their own docid, but
/// each of the docids will map to the same name id.
///
/// The reason why we need DocID in addition to NameID is two fold:
///
/// 1. Firstly, we'd like each name variant to have its own term frequency
///    count. If every variant shared the same internal id, then names with
///    multiple variants would behave as if they were one long name with each
///    variant concatenated together. Our ranking scheme takes document length
///    into account, so we don't want this.
/// 2. Secondly, using an internal ID gives us control over the structure of
///    those ids. For example, we can declare them to be a sorted sequence of
///    increasing integers. This lets us traverse our postings more efficiently
///    during search.
type DocID = u32;

/// The maximum docid allowed.
///
/// When writing postings, we pack docids and their term frequency counts into
/// a single u32. We give 4 bits for frequency and 28 bits for docid. That
/// means we can permit up to 268,435,455 = (1<<28)-1 names, which is plenty
/// for all unique names in IMDb.
const MAX_DOC_ID: DocID = (1 << 28) - 1;

/// A query for searching the name index.
///
/// A query provides the name query and defines the maximum number of results
/// returned by searching the name index.
#[derive(Clone, Debug)]
pub struct NameQuery {
    name: String,
    size: usize,
    scorer: NameScorer,
    stop_word_ratio: f64,
}

impl NameQuery {
    /// Create a query that searches the given name.
    pub fn new(name: &str) -> NameQuery {
        NameQuery {
            name: name.to_string(),
            size: 30,
            scorer: NameScorer::default(),
            stop_word_ratio: 0.01,
        }
    }

    /// Set this query's result set size. At most `size` results will be
    /// returned when searching with this query.
    pub fn with_size(self, size: usize) -> NameQuery {
        NameQuery { size, ..self }
    }

    /// Set this query's scorer. By default, Okapi BM25 is used.
    pub fn with_scorer(self, scorer: NameScorer) -> NameQuery {
        NameQuery { scorer, ..self }
    }

    /// Set the ratio (in the range `0.0` to `1.0`, inclusive) at which a term
    /// is determined to be a stop word. Set to `0.0` to disable. By default
    /// this is set to a non-zero value.
    ///
    /// This ratio is used at query time to partition all of the ngrams in the
    /// query into two bins: one bin is for "low frequency" ngrams while the
    /// other is for "high frequency" ngrams. The partitioning is determined
    /// by this ratio. Namely, if an ngram occurs in fewer than `ratio`
    /// documents in the entire corpus, then it is considered a low frequency
    /// ngram.
    ///
    /// Once these two partitions are created, both are used to create two
    /// disjunction queries. The low frequency query drives search results,
    /// while the high frequency query is only used to boost scores when it
    /// matches a result yielded by the low frequency query. Otherwise, results
    /// from the high frequency query aren't considered.
    pub fn with_stop_word_ratio(self, ratio: f64) -> NameQuery {
        NameQuery {
            stop_word_ratio: ratio,
            ..self
        }
    }
}

/// A reader for the name index.
#[derive(Debug)]
pub struct IndexReader {
    /// The configuration of this index. This is how we determine index-time
    /// settings automatically, such as ngram size and type.
    config: Config,
    /// The ngram index, also known more generally as the "term index." It maps
    /// terms (which are ngrams for this index) to offsets into the postings
    /// file. The offset indicates the start of a list of document ids
    /// containing that term.
    ngram: fst::Map,
    /// The postings. This corresponds to a sequence of lists, where each list
    /// is a list of document ID/frequency pairs. Each list corresponds to the
    /// document ids containing a particular term. The beginning of each list
    /// is pointed to by an offset in the term index.
    postings: Mmap,
    /// A sequence of 64-bit little-endian encoded integers that provide a
    /// map from document ID to name ID. The document ID is an internal
    /// identifier assigned to each unique name indexed, while the name ID is
    /// an external identifier provided by users of this index.
    ///
    /// This map is used to return name IDs to callers. Namely, results are
    /// natively represented by document IDs, but they are mapped to name IDs
    /// during collection of results and subsequently deduped. In particular,
    /// multiple document IDs can map to the same name ID.
    ///
    /// The number of entries in this map is equivalent to the total number of
    /// names indexed.
    idmap: Mmap,
    /// A sequence of 16-bit little-endian encoded integers indicating the
    /// document length (in terms) of the correspond document ID.
    ///
    /// The number of entries in this map is equivalent to the total number of
    /// names indexed.
    norms: Mmap,
}

/// The configuration for this name index. It is JSON encoded to disk.
///
/// Note that we don't track the version here. Instead, it is tracked wholesale
/// as part of the parent index.
#[derive(Debug, Deserialize, Serialize)]
struct Config {
    ngram_type: NgramType,
    ngram_size: usize,
    avg_document_len: f64,
    num_documents: u64,
}

impl IndexReader {
    /// Open a name index in the given directory.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<IndexReader> {
        let dir = dir.as_ref();

        // All of the following open memory maps. We claim it is safe because
        // we don't mutate them and no other process (should) either.
        let ngram = unsafe { fst_map_file(dir.join(NGRAM))? };
        let postings = unsafe { mmap_file(dir.join(POSTINGS))? };
        let idmap = unsafe { mmap_file(dir.join(IDMAP))? };
        let norms = unsafe { mmap_file(dir.join(NORMS))? };

        let config_file = open_file(dir.join(CONFIG))?;
        let config: Config =
            serde_json::from_reader(config_file).map_err(|e| Error::config(e.to_string()))?;
        Ok(IndexReader {
            config: config,
            ngram: ngram,
            postings: postings,
            idmap: idmap,
            norms: norms,
        })
    }

    /// Execute a search.
    pub fn search(&self, query: &NameQuery) -> SearchResults<NameID> {
        let start = Instant::now();
        let mut searcher = Searcher::new(self, query);
        let results = CollectTopK::new(query.size).collect(&mut searcher);
        debug!("search for {:?} took {}", query, NiceDuration::since(start));
        results
    }

    /// Return the name ID used to the index the given document id.
    ///
    /// This panics if the given document id does not correspond to an indexed
    /// document.
    fn docid_to_nameid(&self, docid: DocID) -> NameID {
        LE::read_u64(&self.idmap[8 * (docid as usize)..])
    }

    /// Return the length, in terms, of the given document.
    ///
    /// This panics if the given document id does not correspond to an indexed
    /// document.
    fn document_length(&self, docid: DocID) -> u64 {
        LE::read_u16(&self.norms[2 * (docid as usize)..]) as u64
    }
}

/// A collector for gathering the top K results from a search.
///
/// This maintains a min-heap of search results. When a new result is
/// considered, it is compared against the worst result in the heap. If the
/// candidate is worse, then it is discarded. Otherwise, it is shuffled into
/// the heap.
struct CollectTopK {
    /// The total number of hits to collect.
    k: usize,
    /// The min-heap, according to score. Note that since BinaryHeap is a
    /// max-heap by default, we reverse the comparison to get a min-heap.
    queue: BinaryHeap<cmp::Reverse<Scored<NameID>>>,
    /// A set for deduplicating results. Namely, multiple doc IDs can map to
    /// the same name ID. This set makes sure we only collect one name ID.
    ///
    /// We map name IDs to scores. In this way, we always report the best
    /// scoring match.
    byid: FnvHashMap<NameID, f64>,
}

impl CollectTopK {
    /// Build a new collector that collects at most `k` results.
    fn new(k: usize) -> CollectTopK {
        CollectTopK {
            k: k,
            queue: BinaryHeap::with_capacity(k),
            byid: FnvHashMap::default(),
        }
    }

    /// Collect the top K results from the given searcher using the given
    /// index reader. Return the results with normalized scores sorted in
    /// order of best-to-worst.
    fn collect(mut self, searcher: &mut Searcher) -> SearchResults<NameID> {
        if self.k == 0 {
            return SearchResults::new();
        }
        let index = searcher.index();
        let (mut count, mut push_count) = (0, 0);
        for scored_with_docid in searcher {
            count += 1;
            let scored = scored_with_docid.map(|v| index.docid_to_nameid(v));
            // Since multiple names can correspond to a single IMDb title,
            // we dedup our results here. That is, if our result set
            // already contains this result, then update the score if need
            // be, and then move on.
            if let Some(&score) = self.byid.get(scored.value()) {
                if scored.score() > score {
                    self.byid.insert(*scored.value(), scored.score());
                }
                continue;
            }

            let mut dopush = self.queue.len() < self.k;
            if !dopush {
                // This unwrap is OK because k > 0 and queue is non-empty.
                let worst = self.queue.peek_mut().unwrap();
                // If our queue is full, then we should only push if this
                // doc id has a better score than the worst one in the queue.
                if worst.0 < scored {
                    self.byid.remove(worst.0.value());
                    binary_heap::PeekMut::pop(worst);
                    dopush = true;
                }
            }
            if dopush {
                push_count += 1;
                self.byid.insert(*scored.value(), scored.score());
                self.queue.push(cmp::Reverse(scored));
            }
        }
        debug!(
            "collect count: {:?}, collect push count: {:?}",
            count, push_count
        );

        // Pull out the results from our heap and normalize the scores.
        let mut results = SearchResults::from_min_heap(&mut self.queue);
        results.normalize();
        results
    }
}

/// A searcher for resolving fulltext queries.
///
/// A searcher takes a fulltext query, usually typed by an end user, along with
/// a scoring function and produces a stream of matching results with scores
/// computed via the provided function. Results are always yielded in
/// ascending order with respect to document IDs, which are internal IDs
/// assigned to each name in the index.
///
/// This searcher combines a bit of smarts to handle stop words, usually
/// referred to as "dynamic stop word detection." Namely, after the searcher
/// splits the query into ngrams, it partitions the ngrams into infrequently
/// occurring ngrams and frequently occurring ngrams, according to some
/// hard-coded threshold. Each group is then turned into a `Disjunction`
/// query. The searcher then visits every doc ID that matches the infrequently
/// occurring disjunction. When a score is computed for a doc ID, then its
/// score is increased if the frequently occurring disjunction also contains
/// that same doc ID. Otherwise, the frequently occurring disjunction isn't
/// consulted at all, which permits skipping the score calculation for a
/// potentially large number of doc IDs.
///
/// When two partitions cannot be created (e.g., all of the terms are
/// infrequently occurring or all of the terms are frequently occurring), then
/// only one disjunction query is used and no skipping logic is employed. That
/// means that a query consisting of all high frequency terms could be quite
/// slow.
///
/// This does of course sacrifice recall for a performance benefit, but so do
/// all filtering strategies based on stop words. The benefit of this "dynamic"
/// approach is that stop word detection is tailored exactly to the corpus, and
/// that stop words can still influence scoring. That means queries like "the
/// matrix" will match "The Matrix" better than "Matrix" (which is a legitimate
/// example, try it).
struct Searcher<'i> {
    /// A handle to the index.
    index: &'i IndexReader,
    /// The primary disjunction query that drives results. Typically, this
    /// corresponds to the infrequent terms in the query.
    primary: Disjunction<'i>,
    /// A disjunction of only high frequency terms. When the query consists
    /// of exclusively high frequency terms, then this is empty (which matches
    /// nothing) and `primary` is set to the disjunction of terms.
    high: Disjunction<'i>,
}

impl<'i> Searcher<'i> {
    /// Create a new searcher.
    fn new(idx: &'i IndexReader, query: &NameQuery) -> Searcher<'i> {
        let num_docs = idx.config.num_documents as f64;
        let (mut low, mut high) = (vec![], vec![]);
        let (mut low_terms, mut high_terms) = (vec![], vec![]);

        let name = normalize_query(&query.name);
        let mut query_len = 0;
        let mut multiset = FnvHashMap::default();
        idx.config
            .ngram_type
            .iter(idx.config.ngram_size, &name, |term| {
                *multiset.entry(term).or_insert(0) += 1;
                query_len += 1;
            });
        for (term, &count) in multiset.iter() {
            let postings = PostingIter::new(idx, query.scorer, count, term);
            let ratio = (postings.len() as f64) / num_docs;
            if ratio < query.stop_word_ratio {
                low.push(postings);
                low_terms.push(format!("{}:{}:{:0.6}", term, count, ratio));
            } else {
                high.push(postings);
                high_terms.push(format!("{}:{}:{:0.6}", term, count, ratio));
            }
        }
        debug!("starting search for: {:?}", name);
        debug!("{:?} low frequency terms: {:?}", low.len(), low_terms);
        debug!("{:?} high frequency terms: {:?}", high.len(), high_terms);

        if low.is_empty() {
            Searcher {
                index: idx,
                primary: Disjunction::new(idx, query_len, query.scorer, high),
                high: Disjunction::empty(idx, query.scorer),
            }
        } else {
            Searcher {
                index: idx,
                primary: Disjunction::new(idx, query_len, query.scorer, low),
                high: Disjunction::new(idx, query_len, query.scorer, high),
            }
        }
    }

    /// Return a reference to the underlying index reader.
    fn index(&self) -> &'i IndexReader {
        self.index
    }
}

impl<'i> Iterator for Searcher<'i> {
    type Item = Scored<DocID>;

    fn next(&mut self) -> Option<Scored<DocID>> {
        // This is pretty simple. We drive the iterator via the primary
        // disjunction, which is usually a disjunction of infrequently
        // occurring ngrams.
        let mut scored = match self.primary.next() {
            None => return None,
            Some(scored) => scored,
        };
        // We then skip our frequently occurring disjunction to the doc ID
        // yielded above. Any frequently occurring ngrams found then improve
        // this score. This makes queries like 'the matrix' match 'The Matrix'
        // better than 'Matrix'.
        if let Some(other_scored) = self.high.skip_to(*scored.value()) {
            scored = scored.map_score(|s| s + other_scored.score());
        }
        Some(scored)
    }
}

/// A disjunction over a collection of ngrams. A disjunction yields scored
/// document IDs for every document that contains any of the terms in this
/// disjunction. The more ngrams that match the document in the disjunction,
/// the better the score.
struct Disjunction<'i> {
    /// A handle to the underlying index that we're searching.
    index: &'i IndexReader,
    /// The number of ngrams in the original query.
    ///
    /// This is not necessarily equivalent to the number of ngrams in this
    /// specific disjunction. Namely, this is used to compute scores, and it
    /// is important that scores are computed using the total number of ngrams
    /// and not the number of ngrams in a specific disjunction. For example,
    /// if a query consisted of 8 infrequent ngrams and 1 frequent ngram, then
    /// the disjunction containing the single frequent ngram would contribute a
    /// disproportionately high score.
    query_len: f64,
    /// The scoring function to use.
    scorer: NameScorer,
    /// A min-heap of posting iterators. Each posting iterator corresponds to
    /// an iterator over (doc ID, frequency) pairs for a single ngram, sorted
    /// by doc ID in ascending order.
    ///
    /// A min-heap is a classic way of optimally computing a disjunction over
    /// an arbitrary number of ordered streams.
    queue: BinaryHeap<PostingIter<'i>>,
    /// Whether this disjunction has been exhausted or not.
    is_done: bool,
}

impl<'i> Disjunction<'i> {
    /// Create a new disjunction over the given posting iterators.
    fn new(
        index: &'i IndexReader,
        query_len: usize,
        scorer: NameScorer,
        posting_iters: Vec<PostingIter<'i>>,
    ) -> Disjunction<'i> {
        let mut queue = BinaryHeap::new();
        for postings in posting_iters {
            queue.push(postings);
        }
        let is_done = queue.is_empty();
        let query_len = query_len as f64;
        Disjunction {
            index,
            query_len,
            scorer,
            queue,
            is_done,
        }
    }

    /// Create an empty disjunction that never matches anything.
    fn empty(index: &'i IndexReader, scorer: NameScorer) -> Disjunction<'i> {
        Disjunction {
            index: index,
            query_len: 0.0,
            scorer: scorer,
            queue: BinaryHeap::new(),
            is_done: true,
        }
    }

    /// Skip this disjunction such that all posting iterators are either
    /// positioned at the smallest doc ID greater than the given doc ID.
    ///
    /// If any posting iterator contains the given doc ID, then it is scored
    /// and returned. The score incorporates all posting iterators that contain
    /// the given doc ID.
    fn skip_to(&mut self, target_docid: DocID) -> Option<Scored<DocID>> {
        if self.is_done {
            return None;
        }
        let mut found = false;
        // loop invariant: loop until all posting iterators are either
        // positioned directly at the target doc ID (in which case, `found`
        // is set to that doc ID) or beyond the target doc ID. If none of the
        // iterators contain the target doc ID, then `found` remains `None`.
        loop {
            // This unwrap is OK because we're only here if we have a
            // non-empty queue.
            let mut postings = self.queue.peek_mut().unwrap();
            if postings.docid().map_or(true, |x| x >= target_docid) {
                found = found || Some(target_docid) == postings.docid();
                // This is the smallest posting iterator, which means all
                // iterators are now either at or beyond target_docid.
                break;
            }
            // Skip through this iterator until we're at or beyond the target
            // doc ID.
            while postings.docid().map_or(false, |x| x < target_docid) {
                postings.next();
            }
            found = found || Some(target_docid) == postings.docid();
        }
        if !found {
            return None;
        }
        // We're here if we found our target doc ID, which means at least one
        // posting iterator is pointing to the doc ID and it is necessarily
        // the minimum doc ID of all the posting iterators in this disjunction.
        // Therefore, advance such that all posting iterators are beyond the
        // target doc ID.
        //
        // (If we didn't find the target doc ID, then the loop invariant above
        // guarantees that we are already passed the target doc ID.)
        self.next()
    }
}

impl<'i> Iterator for Disjunction<'i> {
    type Item = Scored<DocID>;

    fn next(&mut self) -> Option<Scored<DocID>> {
        if self.is_done {
            return None;
        }
        // Find our next matching ngram.
        let mut scored1 = {
            // This unwrap is OK because we're only here if we have a
            // non-empty queue.
            let mut postings = self.queue.peek_mut().unwrap();
            match postings.score() {
                None => {
                    self.is_done = true;
                    return None;
                }
                Some(scored) => {
                    postings.next();
                    scored
                }
            }
        };
        // Discover if any of the other posting iterators also match this
        // ngram.
        loop {
            // This unwrap is OK because we're only here if we have a
            // non-empty queue.
            let mut postings = self.queue.peek_mut().unwrap();
            match postings.score() {
                None => break,
                Some(scored2) => {
                    // If the smallest posting iterator isn't equivalent to
                    // the doc ID found above, then we've found all of the
                    // matching terms for this doc ID that we'll find.
                    if scored1.value() != scored2.value() {
                        break;
                    }
                    scored1 = scored1.map_score(|s| s + scored2.score());
                    postings.next();
                }
            }
        }
        // Some of our scorers are more convenient to compute at the
        // disjunction level rather than at the term level.
        if let NameScorer::Jaccard = self.scorer {
            // When using Jaccard, the score returned by the posting
            // iterator is always 1. Thus, `scored.score` represents the
            // total number of terms that matched this document. In other
            // words, it is the cardinality of the intersection of terms
            // between the query and our candidate, `|A ∩ B|`.
            //
            // `query_len` represents the total number of terms in our query
            // (not just the number of terms in this disjunction!), and
            // `doc_len` represents the total number of terms in our candidate.
            // Thus, since `|A u B| = |A| + |B| - |A ∩ B|`, we have that
            // `|A u B| = query_len + doc_len - scored.score`. And finally, the
            // Jaccard index is `|A ∩ B| / |A u B|`.
            let doc_len = self.index.document_length(*scored1.value()) as f64;
            let union = self.query_len + doc_len - scored1.score();
            scored1 = scored1.map_score(|s| s / union);
        } else if let NameScorer::QueryRatio = self.scorer {
            // This is like Jaccard, but our score is computely purely as the
            // ratio of query terms that matched this document.
            scored1 = scored1.map_score(|s| s / self.query_len)
        }
        Some(scored1)
    }
}

/// An iterator over a postings list for a specific ngram.
///
/// A postings list is a sequence of pairs, where each pair has a document
/// ID and a frequency. The document ID indicates that the ngram is in the
/// text indexed for that ID, and the frequency counts the number of times
/// that ngram occurs in the document.
///
/// To save space, each pair is encoded using 32 bits. Frequencies are capped
/// at a maximum of 15, which fit into the high 4 bits. The low 28 bits contain
/// the doc ID.
///
/// The postings list starts with a single 32-bit little endian
/// integer that represents the document frequency of the ngram. This in turn
/// determines how many pairs to read. In other words, a posting list is a
/// length prefixed array of 32 bit little endian integer values.
///
/// This type is intended to be used in a max-heap, and orients its Ord
/// definition such that the heap becomes a min-heap. The ordering criteria
/// is derived from only the docid.
#[derive(Clone)]
struct PostingIter<'i> {
    /// A handle to the underlying index.
    index: &'i IndexReader,
    /// The scoring function to use.
    scorer: NameScorer,
    /// The number of times the term for these postings appeared in the
    /// original query. This increases the score proportionally.
    count: f64,
    /// The raw bytes of the posting list. The number of bytes is
    /// exactly equivalent to `4 * document-frequency(ngram)`, where
    /// `document-frequency(ngram)` is the total number of documents in which
    /// `ngram` occurs.
    ///
    /// This does not include the length prefix.
    postings: &'i [u8],
    /// The document frequency of this term.
    len: usize,
    /// The current posting. This is `None` once this iterator is exhausted.
    posting: Option<Posting>,
    /// A docid used for sorting postings. When the iterator is exhausted,
    /// this is greater than the maximum doc id. Otherwise, this is always
    /// equivalent to posting.docid.
    ///
    /// We do this for efficiency by avoiding going through the optional
    /// Posting.
    docid: DocID,
    /// The OkapiBM25 IDF score. This is invariant across all items in a
    /// posting list, so we compute it once at construction. This saves a
    /// call to `log` for every doc ID visited.
    okapi_idf: f64,
}

/// A single entry in a posting list.
#[derive(Clone, Copy, Debug)]
struct Posting {
    /// The document id.
    docid: DocID,
    /// The frequency, i.e., the number of times the ngram occurred in the
    /// document identified by the docid.
    frequency: u32,
}

impl Posting {
    /// Read the next posting pair (doc ID and frequency) from the given
    /// postings list. If the list is empty, then return `None`.
    fn read(slice: &[u8]) -> Option<Posting> {
        if slice.is_empty() {
            None
        } else {
            let v = LE::read_u32(slice);
            Some(Posting {
                docid: v & MAX_DOC_ID,
                frequency: v >> 28,
            })
        }
    }
}

impl<'i> PostingIter<'i> {
    /// Create a new posting iterator for the given term in the given index.
    /// Scores will be computed with the given scoring function.
    ///
    /// `count` should be the number of times this term occurred in the
    /// original query string.
    fn new(
        index: &'i IndexReader,
        scorer: NameScorer,
        count: usize,
        term: &str,
    ) -> PostingIter<'i> {
        let mut postings = &*index.postings;
        let offset = match index.ngram.get(term.as_bytes()) {
            Some(offset) => offset as usize,
            None => {
                // If the term isn't in the index, then return an exhausted
                // iterator.
                return PostingIter {
                    index: index,
                    scorer: scorer,
                    count: 0.0,
                    postings: &[],
                    len: 0,
                    posting: None,
                    docid: MAX_DOC_ID + 1,
                    okapi_idf: 0.0,
                };
            }
        };
        postings = &postings[offset..];
        let len = LE::read_u32(postings) as usize;
        postings = &postings[4..];

        let corpus_count = index.config.num_documents as f64;
        let df = len as f64;
        let okapi_idf = (1.0 + (corpus_count - df + 0.5) / (df + 0.5)).log2();
        let mut it = PostingIter {
            index: index,
            scorer: scorer,
            count: count as f64,
            postings: &postings[..4 * len],
            len: len,
            posting: None,
            docid: 0,
            okapi_idf: okapi_idf,
        };
        // Advance to the first posting.
        it.next();
        it
    }

    /// Return the current posting. If this iterator has been exhausted, then
    /// this returns `None`.
    fn posting(&self) -> Option<Posting> {
        self.posting
    }

    /// Returns the document frequency for the term corresponding to these
    /// postings.
    fn len(&self) -> usize {
        self.len
    }

    /// Return the current document ID. If this iterator has been exhausted,
    /// then this returns `None`.
    fn docid(&self) -> Option<DocID> {
        self.posting().map(|p| p.docid)
    }

    /// Return the score with the current document ID. If this iterator has
    /// been exhausted, then this returns `None`.
    fn score(&self) -> Option<Scored<DocID>> {
        match self.scorer {
            NameScorer::OkapiBM25 => self.score_okapibm25(),
            NameScorer::TFIDF => self.score_tfidf(),
            NameScorer::Jaccard => self.score_jaccard(),
            NameScorer::QueryRatio => self.score_query_ratio(),
        }
        .map(|scored| scored.map_score(|s| s * self.count))
    }

    /// Score the current doc ID using Okapi BM25. It's similarish to TF-IDF,
    /// but uses a document length normalization term.
    fn score_okapibm25(&self) -> Option<Scored<DocID>> {
        let post = match self.posting() {
            None => return None,
            Some(post) => post,
        };

        let k1 = 1.2;
        let b = 0.75;
        let doc_len = self.index.document_length(post.docid);
        let norm = (doc_len as f64) / self.index.config.avg_document_len;
        let tf = post.frequency as f64;

        let num = tf * (k1 + 1.0);
        let den = tf + k1 * (1.0 - b + b * norm);
        let score = (num / den) * self.okapi_idf;
        let capped = if score < 0.0 { 0.0 } else { score };
        Some(Scored::new(post.docid).with_score(capped))
    }

    /// Score the current doc ID using the traditional TF-IDF ranking function.
    fn score_tfidf(&self) -> Option<Scored<DocID>> {
        let post = match self.posting() {
            None => return None,
            Some(post) => post,
        };

        let corpus_docs = self.index.config.num_documents as f64;
        let term_docs = self.len as f64;
        let tf = post.frequency as f64;
        let idf = (corpus_docs / (1.0 + term_docs)).log2();
        let score = tf * idf;
        Some(Scored::new(post.docid).with_score(score))
    }

    /// Score the current doc ID using the Jaccard index, which measures the
    /// overlap between two sets.
    ///
    /// Note that this always returns `1.0`. The Jaccard index itself must be
    /// computed by the disjunction scorer.
    fn score_jaccard(&self) -> Option<Scored<DocID>> {
        self.posting().map(|p| Scored::new(p.docid).with_score(1.0))
    }

    /// Score the current doc ID using the ratio of terms in the query that
    /// matched the terms in this doc ID.
    ///
    /// Note that this always returns `1.0`. The query ratio itself must be
    /// computed by the disjunction scorer.
    fn score_query_ratio(&self) -> Option<Scored<DocID>> {
        self.posting().map(|p| Scored::new(p.docid).with_score(1.0))
    }
}

impl<'i> Iterator for PostingIter<'i> {
    type Item = Posting;

    fn next(&mut self) -> Option<Posting> {
        self.posting = match Posting::read(self.postings) {
            None => {
                self.docid = MAX_DOC_ID + 1;
                None
            }
            Some(p) => {
                self.postings = &self.postings[4..];
                self.docid = p.docid;
                Some(p)
            }
        };
        self.posting
    }
}

impl<'i> Eq for PostingIter<'i> {}

impl<'i> PartialEq for PostingIter<'i> {
    fn eq(&self, other: &PostingIter<'i>) -> bool {
        self.docid == other.docid
    }
}

impl<'i> Ord for PostingIter<'i> {
    fn cmp(&self, other: &PostingIter<'i>) -> cmp::Ordering {
        // std::collections::BinaryHeap is a max-heap and we need a
        // min-heap, so write this as-if it were a max-heap, then reverse it.
        // Note that exhausted searchers should always have the lowest
        // priority, and therefore, be considered maximal.
        self.docid.cmp(&other.docid).reverse()
    }
}

impl<'i> PartialOrd for PostingIter<'i> {
    fn partial_cmp(&self, other: &PostingIter<'i>) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// A writer for indexing names to disk.
///
/// A writer opens and writes to several files simultaneously, which keeps the
/// implementation simple.
///
/// The index writer cannot stream the postings or term index, since the term
/// index requires its ngrams to be inserted in sorted order. Postings lists
/// are written as length prefixed sequences, so we need to know the lengths
/// of all our postings lists before writing them.
pub struct IndexWriter {
    /// A builder for the ngram term index.
    ///
    /// This isn't used until the caller indicates that it is done indexing
    /// names. At which point, we insert all ngrams into the FST in sorted
    /// order. Each ngram is mapped to the beginning of its correspond
    /// postings list.
    ngram: fst::MapBuilder<io::BufWriter<File>>,
    /// The type of ngram extraction to use.
    ngram_type: NgramType,
    /// The size of ngrams to generate.
    ngram_size: usize,
    /// A writer for postings lists.
    ///
    /// This isn't written to until the caller indicates that it is done
    /// indexing names. At which point, every posting list is written as a
    /// length prefixed array, in the same order that terms are written to the
    /// term index.
    postings: CursorWriter<io::BufWriter<File>>,
    /// A map from document ID to name ID. This is written to in a streaming
    /// fashion during indexing. The ID map consists of N 64-bit little
    /// endian integers, where N is the total number of names indexed.
    ///
    /// The document ID (the position in this map) is a unique internal
    /// identifier assigned to each name, while the name ID is an identifier
    /// provided by the caller. Multiple document IDs may map to the same
    /// name ID (e.g., for indexing alternate names).
    idmap: CursorWriter<io::BufWriter<File>>,
    /// A map from document ID to document length, where the length corresponds
    /// to the number of ngrams in the document. The map consists of N 16-bit
    /// little endian integers, where N is the total number of names indexed.
    ///
    /// The document lengths are used at query time as normalization
    /// parameters. They are written in a streaming fashion during the indexing
    /// process.
    norms: CursorWriter<io::BufWriter<File>>,
    /// A JSON formatted configuration file that includes some aggregate
    /// statistics (such as the average document length, in ngrams) and the
    /// ngram configuration. The ngram configuration in particular is used at
    /// query time to make sure that query-time uses the same analysis as
    /// index-time.
    ///
    /// This is written at the end of the indexing process.
    config: CursorWriter<io::BufWriter<File>>,
    /// An in-memory map from ngram to its corresponding postings list. Once
    /// indexing is done, this is written to disk via the FST term index and
    /// postings list writers documented above.
    terms: FnvHashMap<String, Postings>,
    /// The next document ID, starting at 0. Each name added gets assigned its
    /// own unique document ID. Queries read document IDs from the postings
    /// list, but are mapped back to name IDs using the `idmap` before being
    /// returned to the caller.
    next_docid: DocID,
    /// The average document length, in ngrams, for every name indexed. This is
    /// used along with document lengths to compute normalization terms for
    /// scoring at query time.
    avg_document_len: f64,
}

/// A single postings list.
#[derive(Clone, Debug, Default)]
struct Postings {
    /// A sorted list of postings, in order of ascending document IDs.
    list: Vec<Posting>,
}

impl IndexWriter {
    /// Open an index for writing to the given directory. Any previous name
    /// index in the given directory is overwritten.
    ///
    /// The given ngram configuration is used to transform all indexed names
    /// into terms for the inverted index.
    pub fn open<P: AsRef<Path>>(
        dir: P,
        ngram_type: NgramType,
        ngram_size: usize,
    ) -> Result<IndexWriter> {
        let dir = dir.as_ref();

        let ngram = fst_map_builder_file(dir.join(NGRAM))?;
        let postings = CursorWriter::from_path(dir.join(POSTINGS))?;
        let idmap = CursorWriter::from_path(dir.join(IDMAP))?;
        let norms = CursorWriter::from_path(dir.join(NORMS))?;
        let config = CursorWriter::from_path(dir.join(CONFIG))?;
        Ok(IndexWriter {
            ngram: ngram,
            ngram_type: ngram_type,
            ngram_size: ngram_size,
            postings: postings,
            idmap: idmap,
            norms: norms,
            config: config,
            terms: FnvHashMap::default(),
            next_docid: 0,
            avg_document_len: 0.0,
        })
    }

    /// Finish writing names and serialize the index to disk.
    pub fn finish(mut self) -> Result<()> {
        let num_docs = self.num_docs();
        let mut ngram_to_postings: Vec<(String, Postings)> = self.terms.into_iter().collect();
        // We could use a BTreeMap and get out our keys in sorted order, but
        // the overhead of inserting into the BTreeMap dwarfs the savings we
        // get from pre-sorted keys.
        ngram_to_postings.sort_by(|&(ref t1, _), &(ref t2, _)| t1.cmp(t2));

        for (term, postings) in ngram_to_postings {
            let pos = self.postings.position() as u64;
            self.ngram
                .insert(term.as_bytes(), pos)
                .map_err(Error::fst)?;
            self.postings
                .write_u32(postings.list.len() as u32)
                .context(ErrorKind::Io)?;
            for posting in postings.list {
                let freq = cmp::min(15, posting.frequency);
                let v = (freq << 28) | posting.docid;
                self.postings.write_u32(v).context(ErrorKind::Io)?;
            }
        }

        serde_json::to_writer_pretty(
            &mut self.config,
            &Config {
                ngram_type: self.ngram_type,
                ngram_size: self.ngram_size,
                avg_document_len: self.avg_document_len,
                num_documents: num_docs as u64,
            },
        )
        .map_err(|e| Error::config(e.to_string()))?;
        self.ngram.finish().map_err(Error::fst)?;
        self.idmap.flush().context(ErrorKind::Io)?;
        self.postings.flush().context(ErrorKind::Io)?;
        self.norms.flush().context(ErrorKind::Io)?;
        self.config.flush().context(ErrorKind::Io)?;
        Ok(())
    }

    /// Inserts the given name to this index, and associates it with the
    /// provided `NameID`. Multiple names may be associated with the same
    /// `NameID`.
    pub fn insert(&mut self, name_id: NameID, name: &str) -> Result<()> {
        let docid = self.next_docid(name_id)?;
        let name = normalize_query(name);
        let mut count = 0u16; // document length in number of ngrams
        self.ngram_type
            .clone()
            .iter(self.ngram_size, &name, |ngram| {
                self.insert_term(docid, ngram);
                // If a document length exceeds 2^16, then it is far too long for
                // a name anyway, so we cap it at 2^16.
                count = count.saturating_add(1);
            });
        // Update our mean document length (in ngrams).
        self.avg_document_len += (count as f64 - self.avg_document_len) / (self.num_docs() as f64);
        // Write the document length to disk, which is used as a normalization
        // term for some scorers (like Okapi-BM25).
        self.norms.write_u16(count).context(ErrorKind::Io)?;
        Ok(())
    }

    /// Add a single term that is part of a name identified by the given docid.
    /// This updates the postings for this term, or creates a new posting if
    /// this is the first time this term has been seen.
    fn insert_term(&mut self, docid: DocID, term: &str) {
        if let Some(posts) = self.terms.get_mut(term) {
            posts.posting(docid).frequency += 1;
            return;
        }
        let mut list = Postings::default();
        list.posting(docid).frequency = 1;
        self.terms.insert(term.to_string(), list);
    }

    /// Retrieve a fresh doc id, and associate it with the given name id.
    fn next_docid(&mut self, name_id: NameID) -> Result<DocID> {
        let docid = self.next_docid;
        self.idmap.write_u64(name_id).context(ErrorKind::Io)?;
        self.next_docid = match self.next_docid.checked_add(1) {
            None => bug!("exhausted doc ids"),
            Some(next_docid) => next_docid,
        };
        if self.next_docid > MAX_DOC_ID {
            let max = MAX_DOC_ID + 1; // docids are 0-indexed
            bug!("exceeded maximum number of names ({})", max);
        }
        Ok(docid)
    }

    /// Return the total number of documents have been assigned doc ids.
    fn num_docs(&self) -> u32 {
        self.next_docid
    }
}

impl Postings {
    /// Return a mutable reference to the posting for the given docid. If one
    /// doesn't exist, then create one (with a zero frequency) and return it.
    fn posting(&mut self, docid: DocID) -> &mut Posting {
        if self.list.last().map_or(true, |x| x.docid != docid) {
            self.list.push(Posting {
                docid: docid,
                frequency: 0,
            });
        }
        // This unwrap is OK because if the list was empty when this method was
        // called, then we added an element above, and is thus now non-empty.
        self.list.last_mut().unwrap()
    }
}

/// The type of scorer that the name index should use.
///
/// The default is OkapiBM25. If you aren't sure which scorer to use, then
/// stick with the default.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NameScorer {
    /// OkapiBM25 is a TF-IDF-like ranking function, which takes name length
    /// into account.
    OkapiBM25,
    /// TFIDF is the traditional TF-IDF ranking function, which does not
    /// incorporate document length.
    TFIDF,
    /// Jaccard is a ranking function determined by computing the similarity
    /// of ngrams between the query and a name in the index. The similarity
    /// is computed by dividing the number of ngrams in common by the total
    /// number of distinct ngrams in both the query and the name combined.
    Jaccard,
    /// QueryRatio is a ranking function that represents the ratio of query
    /// terms that matched a name. It is computed by dividing the number of
    /// ngrams in common by the total number of ngrams in the query only.
    QueryRatio,
}

impl NameScorer {
    /// Returns a list of strings representing the possible scorer values.
    pub fn possible_names() -> &'static [&'static str] {
        &["okapibm25", "tfidf", "jaccard", "queryratio"]
    }

    /// Return a string representation of this scorer.
    ///
    /// The string returned can be parsed back into a `NameScorer`.
    pub fn as_str(&self) -> &'static str {
        match *self {
            NameScorer::OkapiBM25 => "okapibm25",
            NameScorer::TFIDF => "tfidf",
            NameScorer::Jaccard => "jaccard",
            NameScorer::QueryRatio => "queryratio",
        }
    }
}

impl Default for NameScorer {
    fn default() -> NameScorer {
        NameScorer::OkapiBM25
    }
}

impl fmt::Display for NameScorer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for NameScorer {
    type Err = Error;

    fn from_str(s: &str) -> Result<NameScorer> {
        match s {
            "okapibm25" => Ok(NameScorer::OkapiBM25),
            "tfidf" => Ok(NameScorer::TFIDF),
            "jaccard" => Ok(NameScorer::Jaccard),
            "queryratio" => Ok(NameScorer::QueryRatio),
            unk => Err(Error::unknown_scorer(unk)),
        }
    }
}

/// The style of ngram extraction to use.
///
/// The same style of ngram extraction is always used at index time and at
/// query time.
///
/// Each ngram type uses the ngram size configuration differently.
///
/// All ngram styles used Unicode codepoints as the definition of a character.
/// For example, a 3-gram might contain up to 4 bytes, if it contains 3 Unicode
/// codepoints that each require 4 UTF-8 code units.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum NgramType {
    /// A windowing ngram.
    ///
    /// This is the tradition style of ngram, where sliding window of size
    /// `N` is moved across the entire content to be index. For example, the
    /// 3-grams for the string `homer` are hom, ome and mer.
    #[serde(rename = "window")]
    Window,
    /// An edge ngram.
    ///
    /// This style of ngram produces ever longer ngrams, where each ngram is
    /// anchored to the start of a word. Words are determined simply by
    /// splitting whitespace.
    ///
    /// For example, the edge ngrams of `homer simpson`, where the max ngram
    /// size is 5, would be: hom, home, homer, sim, simp, simps. Generally,
    /// for this ngram type, one wants to use a large maximum ngram size.
    /// Perhaps somewhere close to the maximum number of ngrams in any word
    /// in the corpus.
    ///
    /// Note that there is no way to set the minimum ngram size (which is 3).
    #[serde(rename = "edge")]
    Edge,
}

/// The minimum size of an ngram emitted by the edge ngram iterator.
const MIN_EDGE_NGRAM_SIZE: usize = 3;

impl NgramType {
    /// Return all possible ngram types.
    pub fn possible_names() -> &'static [&'static str] {
        &["window", "edge"]
    }

    /// Return a string representation of this type.
    pub fn as_str(&self) -> &'static str {
        match *self {
            NgramType::Window => "window",
            NgramType::Edge => "edge",
        }
    }

    /// Execute the given function over each ngram in the text provided using
    /// the given size configuration.
    ///
    /// We don't use normal Rust iterators here because an internal iterator
    /// is much easier to implement.
    fn iter<'t, F: FnMut(&'t str)>(&self, size: usize, text: &'t str, f: F) {
        match *self {
            NgramType::Window => NgramType::iter_window(size, text, f),
            NgramType::Edge => NgramType::iter_edge(size, text, f),
        }
    }

    fn iter_window<'t, F: FnMut(&'t str)>(size: usize, text: &'t str, mut f: F) {
        if size == 0 {
            return;
        }
        let end_skip = text.chars().take(size).count().saturating_sub(1);
        let start = text.char_indices();
        let end = text.char_indices().skip(end_skip);
        for ((s, _), (e, c)) in start.zip(end) {
            f(&text[s..e + c.len_utf8()]);
        }
    }

    fn iter_edge<'t, F: FnMut(&'t str)>(max_size: usize, text: &'t str, mut f: F) {
        if max_size == 0 {
            return;
        }
        for word in text.split_whitespace() {
            let end_skip = word
                .chars()
                .take(MIN_EDGE_NGRAM_SIZE)
                .count()
                .saturating_sub(1);
            let mut size = end_skip + 1;
            for (end, c) in word.char_indices().skip(end_skip) {
                f(&word[..end + c.len_utf8()]);
                size += 1;
                if size > max_size {
                    break;
                }
            }
        }
    }
}

impl Default for NgramType {
    fn default() -> NgramType {
        NgramType::Window
    }
}

impl fmt::Display for NgramType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for NgramType {
    type Err = Error;

    fn from_str(s: &str) -> Result<NgramType> {
        match s {
            "window" => Ok(NgramType::Window),
            "edge" => Ok(NgramType::Edge),
            unk => Err(Error::unknown_ngram_type(unk)),
        }
    }
}

fn normalize_query(s: &str) -> String {
    // We might consider doing Unicode normalization here, but it probably
    // doesn't matter too much on a predominantly ASCII data set.
    s.to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use index::tests::TestContext;

    // Test the actual name index.

    /// Creates a name index, where each name provided is assigned its own
    /// unique ID, starting at 0.
    fn create_index(index_dir: &Path, names: &[&str]) -> IndexReader {
        let mut wtr = IndexWriter::open(index_dir, NgramType::Window, 3).unwrap();
        for (i, name) in names.iter().enumerate() {
            wtr.insert(i as u64, name).unwrap();
        }
        wtr.finish().unwrap();

        IndexReader::open(index_dir).unwrap()
    }

    /// Build a name query, and disable the dynamic stop word detection.
    ///
    /// It would be nice to test the stop word detection, but it makes writing
    /// unit tests very difficult unfortunately.
    fn name_query(name: &str) -> NameQuery {
        NameQuery::new(name).with_stop_word_ratio(0.0)
    }

    fn ids(results: &[Scored<NameID>]) -> Vec<NameID> {
        let mut ids: Vec<_> = results.iter().map(|r| *r.value()).collect();
        ids.sort();
        ids
    }

    /// Some names involving bruce.
    const BRUCES: &'static [&'static str] = &[
        "Bruce Springsteen", // 0
        "Bruce Kulick",      // 1
        "Bruce Arians",      // 2
        "Bruce Smith",       // 3
        "Bruce Willis",      // 4
        "Bruce Wayne",       // 5
        "Bruce Banner",      // 6
    ];

    #[test]
    #[allow(clippy::float_cmp)]
    fn names_bruces_1() {
        let ctx = TestContext::new("small");
        let idx = create_index(ctx.index_dir(), BRUCES);
        let query = name_query("bruce");
        let results = idx.search(&query).into_vec();

        // This query matches everything.
        assert_eq!(results.len(), 7);
        // The top two hits are the shortest documents, because of Okapi-BM25's
        // length normalization.
        assert_eq!(results[0].score(), 1.0);
        assert_eq!(results[1].score(), 1.0);
        assert_eq!(ids(&results[0..2]), vec![3, 5]);
    }

    #[test]
    fn names_bruces_2() {
        let ctx = TestContext::new("small");
        let idx = create_index(ctx.index_dir(), BRUCES);
        let query = name_query("e w");
        let results = idx.search(&query).into_vec();

        // The 'e w' ngram is only in two documents: Bruce Willis and
        // Bruce Wayne. Since Wayne is shorter than Willis, it should always
        // be first.
        assert_eq!(results.len(), 2);
        assert_eq!(*results[0].value(), 5);
        assert_eq!(*results[1].value(), 4);
    }

    #[test]
    fn names_bruces_3() {
        let ctx = TestContext::new("small");
        let idx = create_index(ctx.index_dir(), BRUCES);
        let query = name_query("Springsteen");
        let results = idx.search(&query).into_vec();

        assert_eq!(results.len(), 1);
        assert_eq!(*results[0].value(), 0);
    }

    #[test]
    fn names_bruces_4() {
        let ctx = TestContext::new("small");
        let idx = create_index(ctx.index_dir(), BRUCES);
        let query = name_query("Springsteen Kulick Arians Smith Willis Wayne Banner");
        let results = idx.search(&query).into_vec();

        // This query should hit everything.
        assert_eq!(results.len(), 7);
    }

    // Test our various ngram strategies.

    fn ngrams_window(n: usize, text: &str) -> Vec<&str> {
        let mut grams = vec![];
        NgramType::Window.iter(n, text, |gram| grams.push(gram));
        grams
    }

    fn ngrams_edge(n: usize, text: &str) -> Vec<&str> {
        let mut grams = vec![];
        NgramType::Edge.iter(n, text, |gram| grams.push(gram));
        grams
    }

    #[test]
    #[should_panic]
    fn ngrams_window_zero_banned() {
        assert_eq!(ngrams_window(0, "abc"), vec!["abc"]);
    }

    #[test]
    fn ngrams_window_weird_sizes() {
        assert_eq!(
            ngrams_window(2, "abcdef"),
            vec!["ab", "bc", "cd", "de", "ef",]
        );
        assert_eq!(
            ngrams_window(1, "abcdef"),
            vec!["a", "b", "c", "d", "e", "f",]
        );
        assert_eq!(ngrams_window(2, "ab"), vec!["ab",]);
        assert_eq!(ngrams_window(1, "ab"), vec!["a", "b",]);
        assert_eq!(ngrams_window(1, "a"), vec!["a",]);
        assert_eq!(ngrams_window(1, ""), Vec::<&str>::new());
    }

    #[test]
    fn ngrams_window_ascii() {
        assert_eq!(
            ngrams_window(3, "abcdef"),
            vec!["abc", "bcd", "cde", "def",]
        );
        assert_eq!(ngrams_window(3, "abcde"), vec!["abc", "bcd", "cde",]);
        assert_eq!(ngrams_window(3, "abcd"), vec!["abc", "bcd",]);
        assert_eq!(ngrams_window(3, "abc"), vec!["abc",]);
        assert_eq!(ngrams_window(3, "ab"), vec!["ab",]);
        assert_eq!(ngrams_window(3, "a"), vec!["a",]);
        assert_eq!(ngrams_window(3, ""), Vec::<&str>::new());
    }

    #[test]
    fn ngrams_window_non_ascii() {
        assert_eq!(
            ngrams_window(3, "αβγφδε"),
            vec!["αβγ", "βγφ", "γφδ", "φδε",]
        );
        assert_eq!(
            ngrams_window(3, "αβγφδ"),
            vec!["αβγ", "βγφ", "γφδ",]
        );
        assert_eq!(ngrams_window(3, "αβγφ"), vec!["αβγ", "βγφ",]);
        assert_eq!(ngrams_window(3, "αβγ"), vec!["αβγ",]);
        assert_eq!(ngrams_window(3, "αβ"), vec!["αβ",]);
        assert_eq!(ngrams_window(3, "α"), vec!["α",]);
    }

    #[test]
    fn ngrams_edge_ascii() {
        assert_eq!(
            ngrams_edge(5, "homer simpson"),
            vec!["hom", "home", "homer", "sim", "simp", "simps",]
        );
        assert_eq!(ngrams_edge(5, "h"), vec!["h",]);
        assert_eq!(ngrams_edge(5, "ho"), vec!["ho",]);
        assert_eq!(ngrams_edge(5, "hom"), vec!["hom",]);
        assert_eq!(ngrams_edge(5, "home"), vec!["hom", "home",]);
    }

    #[test]
    fn ngrams_edge_non_ascii() {
        assert_eq!(
            ngrams_edge(5, "δεαβγφδε δε"),
            vec!["δεα", "δεαβ", "δεαβγ", "δε",]
        );
    }
}
