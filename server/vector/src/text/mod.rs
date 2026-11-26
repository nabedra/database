//! Analyzers of `full_text` columns: Unicode word boundaries (UAX #29), lowercase, and a Snowball
//! stemmer where the language has one. The same analyzer turns a row's text and a `match` query into
//! terms, so both meet in the same dictionary.
//!
//! An analyzer is part of the index format: a checkpoint names [`Analyzer::identity`], and a
//! checkpoint of another identity is not loaded.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use rust_stemmers::{Algorithm, Stemmer};
use unicode_segmentation::UnicodeSegmentation;

use crate::types::{Error, Result};

/// Languages with a stemmer, and `simple`, which only splits and lowercases. A name matches the
/// server's rule for `full_text` languages, `[a-z_]{1,32}`.
pub const LANGUAGES: &[&str] = &[
    "arabic",
    "danish",
    "dutch",
    "english",
    "finnish",
    "french",
    "german",
    "greek",
    "hungarian",
    "italian",
    "norwegian",
    "portuguese",
    "romanian",
    "russian",
    "simple",
    "spanish",
    "swedish",
    "tamil",
    "turkish",
];

/// Longer words are left out, as no one searches for them and they would bloat the dictionary.
pub const MAX_TERM_BYTES: usize = 64;

/// Bumped whenever a text yields other terms than before.
const VERSION: u32 = 1;

fn algorithm(language: &str) -> Option<Option<Algorithm>> {
    Some(Some(match language {
        "arabic" => Algorithm::Arabic,
        "danish" => Algorithm::Danish,
        "dutch" => Algorithm::Dutch,
        "english" => Algorithm::English,
        "finnish" => Algorithm::Finnish,
        "french" => Algorithm::French,
        "german" => Algorithm::German,
        "greek" => Algorithm::Greek,
        "hungarian" => Algorithm::Hungarian,
        "italian" => Algorithm::Italian,
        "norwegian" => Algorithm::Norwegian,
        "portuguese" => Algorithm::Portuguese,
        "romanian" => Algorithm::Romanian,
        "russian" => Algorithm::Russian,
        "spanish" => Algorithm::Spanish,
        "swedish" => Algorithm::Swedish,
        "tamil" => Algorithm::Tamil,
        "turkish" => Algorithm::Turkish,
        "simple" => return Some(None),
        _ => return None,
    }))
}

/// The terms of a text: each distinct term with its count, in byte order, and the number of words.
#[derive(Clone, PartialEq, Debug, Default)]
pub struct Terms {
    pub terms: Vec<(String, u32)>,
    pub words: u64,
}

pub struct Analyzer {
    language: &'static str,
    stemmer: Option<Stemmer>,
}

impl Clone for Analyzer {
    fn clone(&self) -> Self {
        Analyzer {
            language: self.language,
            stemmer: algorithm(self.language).flatten().map(Stemmer::create),
        }
    }
}

impl std::fmt::Debug for Analyzer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Analyzer({})", self.language)
    }
}

impl PartialEq for Analyzer {
    fn eq(&self, other: &Self) -> bool {
        self.language == other.language
    }
}

impl Analyzer {
    /// The analyzer of a language of [`LANGUAGES`].
    pub fn new(language: &str) -> Result<Analyzer> {
        let unknown = Error::InvalidQuery("unknown full-text language");
        let language = *LANGUAGES
            .iter()
            .find(|l| **l == language)
            .ok_or(unknown.clone())?;
        let stemmer = algorithm(language).ok_or(unknown)?.map(Stemmer::create);
        Ok(Analyzer { language, stemmer })
    }

    pub fn language(&self) -> &'static str {
        self.language
    }

    /// Names the analyzer and its version in checkpoints.
    pub fn identity(&self) -> String {
        format!("text/{}/{VERSION}", self.language)
    }

    /// Calls `f` with every term of `text`, in text order.
    pub fn each(&self, text: &str, mut f: impl FnMut(&str)) {
        for word in text.unicode_words() {
            // Of the whole word, so that a final sigma becomes one.
            let lower = word.to_lowercase();
            // Lowercase may grow a word, so the limit applies after it.
            if lower.len() > MAX_TERM_BYTES {
                continue;
            }
            match &self.stemmer {
                Some(stemmer) => {
                    let stem = stemmer.stem(&lower);
                    if !stem.is_empty() {
                        f(&stem);
                    }
                }
                None => f(&lower),
            }
        }
    }

    /// The distinct terms of `text` with their counts.
    pub fn terms(&self, text: &str) -> Terms {
        let mut counts: BTreeMap<String, u32> = BTreeMap::new();
        let mut words = 0u64;
        self.each(text, |term| {
            words += 1;
            match counts.get_mut(term) {
                Some(n) => *n = n.saturating_add(1),
                None => {
                    counts.insert(term.to_owned(), 1);
                }
            }
        });
        Terms {
            terms: counts.into_iter().collect(),
            words,
        }
    }
}
