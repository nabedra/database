//! Analyzers of `full_text` columns. A text is first prepared: NFC (decision L10), then without
//! format characters (L11). Then come Unicode word boundaries (UAX #29), lowercase, and a Snowball
//! stemmer where the language has one. The same analyzer turns a row's text and a `match` query into
//! terms, so both meet in the same dictionary.
//!
//! An analyzer is part of the index format: a checkpoint names [`Analyzer::format`], and a
//! checkpoint of another one is not loaded.

#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::collections::BTreeMap;

use rust_stemmers::{Algorithm, Stemmer};
use unicode_normalization::{IsNormalized, UnicodeNormalization, is_nfc_quick};
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

/// Bumped whenever a language's words yield other terms than before.
const VERSION: u32 = 1;

/// Bumped whenever the preparation of a text before its words changes. 1: none. 2: NFC, then
/// without format characters.
const PREPARATION: u32 = 2;

/// Format characters, general category Cf of Unicode 16.0: soft hyphen, bidi controls, zero-width
/// characters, joiners, the BOM, invisible operators, tags. They are not seen, so they must not make
/// another word.
const FORMAT: &[(u32, u32)] = &[
    (0x00AD, 0x00AD),
    (0x0600, 0x0605),
    (0x061C, 0x061C),
    (0x06DD, 0x06DD),
    (0x070F, 0x070F),
    (0x0890, 0x0891),
    (0x08E2, 0x08E2),
    (0x180E, 0x180E),
    (0x200B, 0x200F),
    (0x202A, 0x202E),
    (0x2060, 0x2064),
    (0x2066, 0x206F),
    (0xFEFF, 0xFEFF),
    (0xFFF9, 0xFFFB),
    (0x110BD, 0x110BD),
    (0x110CD, 0x110CD),
    (0x13430, 0x1343F),
    (0x1BCA0, 0x1BCA3),
    (0x1D173, 0x1D17A),
    (0xE0001, 0xE0001),
    (0xE0020, 0xE007F),
];

fn is_format(c: char) -> bool {
    let c = c as u32;
    let i = FORMAT.partition_point(|&(_, hi)| hi < c);
    FORMAT.get(i).is_some_and(|&(lo, _)| lo <= c)
}

/// NFC, then without format characters. Dropping one can leave marks that compose, so the rest is
/// composed again.
fn prepare(text: &str) -> Cow<'_, str> {
    if text.is_ascii() {
        return Cow::Borrowed(text);
    }
    let nfc = is_nfc_quick(text.chars()) == IsNormalized::Yes;
    let formats = text.chars().any(is_format);
    match (nfc, formats) {
        (true, false) => Cow::Borrowed(text),
        (_, false) => Cow::Owned(text.nfc().collect()),
        (_, true) => {
            let kept: String = text.nfc().filter(|c| !is_format(*c)).collect();
            Cow::Owned(kept.nfc().collect())
        }
    }
}

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

    /// Names the language and the version of its words.
    pub fn identity(&self) -> String {
        format!("text/{}/{VERSION}", self.language)
    }

    /// Names everything that decides the terms of a text: the identity and the preparation. A
    /// checkpoint of another format is not loaded.
    ///
    /// The preparation has a version of its own, rather than a bump of `VERSION`, because the
    /// identity also says which words a language yields: the tests of the adversarial tester pin it
    /// at `/1` and forge a manifest of `/2`, and both stay meaningful this way. A checkpoint written
    /// before NFC names `text/<language>/1` without `+prepare/2`, and is refused.
    pub fn format(&self) -> String {
        format!("{}+prepare/{PREPARATION}", self.identity())
    }

    /// Calls `f` with every term of `text`, in text order.
    pub fn each(&self, text: &str, mut f: impl FnMut(&str)) {
        let text = prepare(text);
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
