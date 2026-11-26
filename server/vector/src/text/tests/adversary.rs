//! Adversarial tests of task G, analyzers: every language, odd Unicode, long words, empty queries,
//! and the lead's decisions L9 (one language set) and L10 (NFC). A `&str` cannot hold a lone
//! surrogate, so the API cannot receive one.

use nabedra_check::Gen;
use nabedra_check::prop::{Config as Runs, check_with};

use super::super::*;
use crate::impact::{
    Config, Doc, ImpactIndex, Kind, Op, Page, Probe, Query, RamStore, Record, Row,
};
use crate::index::ReadAt;
use crate::types::{ColumnType, Error, MAX_LIMIT, RangeId, Value};
use std::sync::Arc;

const RANGE: RangeId = RangeId(1);

fn names(a: &Analyzer, text: &str) -> Vec<String> {
    let mut out = Vec::new();
    a.each(text, |t| out.push(t.to_owned()));
    out
}

/// A text of one language and its first matching query: a sentence about dogs.
const SAMPLES: &[(&str, &str)] = &[
    ("arabic", "كتب الطالب الدرس الجديد في المدرسة"),
    ("danish", "hunden løber hurtigt gennem skoven"),
    ("dutch", "de honden lopen snel door het bos"),
    ("english", "the dogs were running quickly through forests"),
    ("finnish", "koirat juoksevat nopeasti metsässä"),
    ("french", "les chiens courent rapidement dans la forêt"),
    ("german", "die Hunde laufen schnell durch den Wald"),
    ("greek", "οι σκύλοι τρέχουν γρήγορα στο δάσος"),
    ("hungarian", "a kutyák gyorsan futnak az erdőben"),
    ("italian", "i cani corrono velocemente nel bosco"),
    ("norwegian", "hundene løper raskt gjennom skogen"),
    ("portuguese", "os cães correm rapidamente pela floresta"),
    ("romanian", "câinii aleargă repede prin pădure"),
    ("russian", "собаки быстро бегут через лес"),
    ("simple", "the dogs were running quickly through forests"),
    ("spanish", "los perros corren rápidamente por el bosque"),
    ("swedish", "hundarna springer snabbt genom skogen"),
    ("tamil", "நாய்கள் வேகமாக காட்டில் ஓடுகின்றன"),
];

fn one_doc_index(language: &str, texts: &[&str]) -> ImpactIndex {
    let cfg = Config {
        segment_rows: 3,
        ..Config::new(Kind::Text(language.into()), vec![ColumnType::Tags])
    };
    let mut ix = ImpactIndex::new(cfg, RamStore::default(), None).unwrap();
    ix.begin_load(RANGE, 0, 0).unwrap();
    ix.finish_load(RANGE).unwrap();
    ix.caught_up(RANGE, 0).unwrap();
    let records = texts
        .iter()
        .enumerate()
        .map(|(i, t)| Record {
            at: i as u64 + 1,
            change: crate::impact::Change::Row {
                key: Arc::new((i as u64).to_be_bytes()),
                commit_ts: (i as u64 + 1) * 10,
                row: Some(Row {
                    doc: Some(Doc::Text((*t).into())),
                    cols: vec![Value::Tags(Arc::new([]))],
                    image: Arc::new((i as u64).to_be_bytes()),
                }),
            },
        })
        .collect();
    let page = Page {
        after: 0,
        through: texts.len() as u64,
        records,
    };
    ix.apply(RANGE, &page).unwrap();
    ix
}

fn find(ix: &mut ImpactIndex, q: &str) -> Vec<u64> {
    let pin = ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 })]).unwrap();
    let query = Query {
        op: Op::Match,
        probe: Probe::Text(q.into()),
        limit: MAX_LIMIT,
        filter: None,
        exact: true,
        stats: None,
    };
    let hits = ix.search(&pin, &query).unwrap();
    assert!(
        hits.iter().all(|h| h.score.is_finite() && h.score > 0.0),
        "{q:?}: {hits:?}"
    );
    hits.iter()
        .map(|h| u64::from_be_bytes((&*h.key).try_into().unwrap()))
        .collect()
}

#[test]
fn vs17_adv_the_language_set_is_exactly_the_eighteen_stemmed_and_simple() {
    let want = [
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
    ];
    // 17 names above plus turkish, the 18th stemmed language.
    let mut all: Vec<&str> = want.to_vec();
    all.push("turkish");
    all.sort_unstable();
    assert_eq!(LANGUAGES.len(), 19);
    assert_eq!(LANGUAGES, &all[..]);
    // L9: one set. A name outside it never builds an index or an analyzer.
    for bad in [
        "klingon", "English", "", "turkish ", "en", "hebrew", "chinese", "\u{0}",
    ] {
        assert!(Analyzer::new(bad).is_err(), "{bad:?}");
        let cfg = Config::new(Kind::Text(bad.into()), vec![]);
        assert!(
            ImpactIndex::new(cfg, RamStore::default(), None).is_err(),
            "{bad:?}"
        );
    }
    for l in LANGUAGES {
        let cfg = Config::new(Kind::Text((*l).into()), vec![]);
        assert!(
            ImpactIndex::new(cfg, RamStore::default(), None).is_ok(),
            "{l}"
        );
    }
}

#[test]
fn vs17_adv_every_language_finds_its_own_sentence_by_word_by_phrase_and_in_capitals() {
    assert_eq!(SAMPLES.len() + 1, LANGUAGES.len(), "turkish below");
    let mut samples: Vec<(&str, &str)> = SAMPLES.to_vec();
    samples.push(("turkish", "köpekler ormanda hızlıca koşuyor"));
    for (lang, text) in samples {
        let a = Analyzer::new(lang).unwrap();
        let t = a.terms(text);
        assert!(t.terms.len() >= 3, "{lang}: {t:?}");
        assert!(t.words as usize >= t.terms.len());
        // The doc and a decoy that shares none of the words.
        let mut ix = one_doc_index(lang, &["zzzzqq", text, "xxqqw"]);
        assert_eq!(find(&mut ix, text), [1], "{lang}: the sentence");
        assert_eq!(
            find(&mut ix, &text.to_uppercase()),
            [1],
            "{lang}: in capitals"
        );
        for w in text.split(' ') {
            assert_eq!(find(&mut ix, w), [1], "{lang}: the word {w:?}");
        }
        assert!(find(&mut ix, "").is_empty() && find(&mut ix, " ,.").is_empty());
    }
}

#[test]
fn vs17_adv_each_language_folds_inflections_with_its_own_stemmer() {
    // Pairs that Snowball's stemmer of that language maps to one stem; another language's, or
    // `simple`, keeps them apart (checked against `simple`).
    let pairs = [
        ("english", "running", "runs"),
        ("english", "dogs", "dog"),
        ("german", "häuser", "haus"),
        ("french", "chiens", "chien"),
        ("spanish", "perros", "perro"),
        ("russian", "собаки", "собака"),
        ("dutch", "honden", "hond"),
        ("italian", "cani", "cane"),
        ("swedish", "hundarna", "hundar"),
        ("danish", "hundene", "hunden"),
        ("norwegian", "hundene", "hundens"),
        ("finnish", "koirat", "koira"),
        ("portuguese", "cachorros", "cachorro"),
        ("romanian", "câinii", "câine"),
        ("turkish", "köpekler", "köpek"),
    ];
    let simple = Analyzer::new("simple").unwrap();
    let mut bad = Vec::new();
    for (lang, a, b) in pairs {
        let an = Analyzer::new(lang).unwrap();
        if names(&an, a) != names(&an, b) {
            bad.push(format!(
                "{lang}: {a} {:?} and {b} {:?}",
                names(&an, a),
                names(&an, b)
            ));
        }
        assert_ne!(
            names(&simple, a),
            names(&simple, b),
            "{lang}: simple keeps {a} and {b} apart"
        );
    }
    assert!(bad.is_empty(), "{bad:#?}");
    // The stems differ from language to language: the same word is not folded alike.
    let en = Analyzer::new("english").unwrap();
    let de = Analyzer::new("german").unwrap();
    assert_ne!(names(&en, "häuser"), names(&de, "häuser"));
}

/// Pieces of odd text: combining marks, joiners and emoji sequences, bidi controls, scripts
/// without spaces, digits and punctuation that do and do not split words.
const PIECES: &[&str] = &[
    "a",
    "Z",
    "é",
    "e\u{301}",
    "e\u{301}\u{302}\u{303}",
    "\u{301}",
    "\u{200d}",
    "\u{200c}",
    "👩\u{200d}👩\u{200d}👧",
    "👍🏽",
    "🇫🇷",
    "\u{202e}",
    "\u{202c}",
    "שלום",
    "مرحبا",
    "中文搜索",
    "日本語",
    "한국어",
    "ไทย",
    "ཀ",
    "क्ष",
    "ß",
    "İ",
    "Σ",
    "ς",
    "ǅ",
    "ﬃ",
    "\u{fffd}",
    "\u{0}",
    "1",
    "3.14",
    "a_b",
    "x-y",
    "o'clock",
    "\u{2019}",
    " ",
    "\t",
    "\n",
    ".",
    ",",
    "ㅤ",
    "\u{feff}",
    "Ⅻ",
    "𝔘",
    "\u{e000}",
];

fn odd_text(g: &mut Gen, max: usize) -> String {
    let n = g.below(max as u64 + 1) as usize;
    (0..n)
        .map(|_| *g.pick(PIECES))
        .collect::<Vec<_>>()
        .join(if g.bool() { "" } else { " " })
}

#[test]
fn vs17_adv_odd_unicode_round_trips_through_every_analyzer_and_the_index() {
    check_with(
        Runs {
            cases: 120,
            ..Runs::default()
        },
        "vs17_adv_odd_unicode_round_trips_through_every_analyzer_and_the_index",
        |g| {
            let lang = *g.pick(LANGUAGES);
            let (a, b) = (odd_text(g, 14), odd_text(g, 14));
            let an = Analyzer::new(lang).unwrap();
            let mut ix = one_doc_index(lang, &[&a, &b, "plain words here"]);
            for (i, t) in [&a, &b].into_iter().enumerate() {
                let terms = an.terms(t);
                let found = find(&mut ix, t);
                let want = !terms.terms.is_empty();
                if found.contains(&(i as u64)) != want {
                    return Err(format!(
                        "{lang}: {t:?} has terms {terms:?} and finds {found:?}"
                    ));
                }
                // Terms are bounded, sorted, distinct, and add up.
                let sum: u64 = terms.terms.iter().map(|(_, n)| u64::from(*n)).sum();
                let ok = sum == terms.words
                    && terms.terms.windows(2).all(|w| w[0].0 < w[1].0)
                    && terms
                        .terms
                        .iter()
                        .all(|(s, _)| !s.is_empty() && s.len() <= MAX_TERM_BYTES);
                if !ok {
                    return Err(format!("{lang}: {t:?} gives {terms:?}"));
                }
            }
            Ok(())
        },
    );
}

#[test]
fn vs17_adv_cjk_without_spaces_and_emoji_sequences_are_split_or_ignored_not_lost() {
    let a = Analyzer::new("simple").unwrap();
    // UAX #29 puts each ideograph in a word of its own, and keeps a Katakana run whole.
    assert_eq!(names(&a, "中文搜索"), ["中", "文", "搜", "索"]);
    assert_eq!(names(&a, "カタカナ"), ["カタカナ"]);
    // A combining mark stays in its word, a ZWJ emoji sequence has no word.
    assert_eq!(names(&a, "Cafe\u{301}"), ["cafe\u{301}"]);
    assert!(names(&a, "👩\u{200d}👩\u{200d}👧").is_empty());
    assert_eq!(names(&a, "שלום עולם").len(), 2);
    let mut ix = one_doc_index("simple", &["搜索引擎", "引擎"]);
    assert_eq!(
        find(&mut ix, "引擎"),
        [1, 0],
        "the shorter document ranks first"
    );
}

#[test]
fn vs17_adv_words_at_the_byte_limit_in_every_language_and_stems_never_exceed_it() {
    check_with(
        Runs {
            cases: 400,
            ..Runs::default()
        },
        "vs17_adv_words_at_the_byte_limit_in_every_language_and_stems_never_exceed_it",
        |g| {
            const FRAGMENTS: &[&str] = &[
                "ing", "ed", "ation", "ational", "ies", "ness", "ement", "ments", "ly", "ally",
                "iz", "at", "bl", "ss", "ous", "ive", "ator", "en", "ern", "ungen", "heit", "keit",
                "ische", "ere", "er", "ar", "ir", "mente", "ción", "eaux", "ов", "ами", "ость",
                "лар", "ler", "ların", "lık", "ban", "ben", "ok", "ek", "ei", "ije", "ele", "ilor",
                "ului", "ssä", "lla", "han", "ene", "ande", "ing", "ς", "ων", "ους", "ή", "ي",
                "ات", "ون", "ட்", "ங்", "ள்", "ß", "ä", "ö", "ü", "é", "ç", "ñ", "ø", "å", "ă", "ş",
                "ț", "ğ", "ı",
            ];
            let lang = *g.pick(LANGUAGES);
            let an = Analyzer::new(lang).unwrap();
            let mut w = String::new();
            let target = 50 + g.below(20) as usize;
            while w.len() < target {
                w.push_str(*g.pick(FRAGMENTS));
                if g.chance(1, 3) {
                    w.push(*g.pick(&['a', 'e', 'i', 'o', 'u', 'y', 'b', 'l', 'k', 'r', 's', 't']));
                }
            }
            let t = an.terms(&w);
            for (s, _) in &t.terms {
                if s.is_empty() || s.len() > MAX_TERM_BYTES {
                    return Err(format!(
                        "{lang}: {w:?} ({} bytes) gives a term of {} bytes: {s:?}",
                        w.len(),
                        s.len()
                    ));
                }
            }
            Ok(())
        },
    );
}

#[test]
fn vs17_adv_very_long_words_and_texts_are_cheap_and_dropped_or_kept_by_the_limit() {
    let a = Analyzer::new("english").unwrap();
    for n in [MAX_TERM_BYTES - 1, MAX_TERM_BYTES, MAX_TERM_BYTES + 1] {
        let w = "q".repeat(n);
        assert_eq!(names(&a, &w).len(), usize::from(n <= MAX_TERM_BYTES), "{n}");
    }
    // Two bytes per letter: 32 fit, 33 do not.
    assert_eq!(
        names(&Analyzer::new("simple").unwrap(), &"é".repeat(32)).len(),
        1
    );
    assert_eq!(
        names(&Analyzer::new("simple").unwrap(), &"é".repeat(33)).len(),
        0
    );
    // A dropped word is no word: it does not count in the length of the document.
    let t = a.terms(&format!("{} short", "z".repeat(100)));
    assert_eq!((t.terms.len(), t.words), (1, 1));
    // Megabytes of one word, of one letter words, and a million bytes of marks.
    let t = std::time::Instant::now();
    assert!(a.terms(&"a".repeat(4 << 20)).terms.is_empty());
    assert_eq!(a.terms(&"é ".repeat(1 << 20)).words, 1 << 20);
    assert!(
        a.terms(&format!("a{}", "\u{301}".repeat(1 << 20)))
            .terms
            .is_empty()
    );
    assert!(a.terms(&"\u{200d}".repeat(1 << 20)).terms.is_empty());
    assert!(t.elapsed().as_secs() < 30, "{:?}", t.elapsed());
    // A document of a megabyte word and a query of the same word both match nothing, and a
    // document of the long word plus a short one still matches the short one.
    let mut ix = one_doc_index(
        "english",
        &[&format!("{} short", "z".repeat(1 << 20)), "other"],
    );
    assert_eq!(find(&mut ix, "short"), [0]);
    assert!(find(&mut ix, &"z".repeat(1 << 20)).is_empty());
}

#[test]
fn vs17_adv_an_empty_query_matches_nothing_and_asks_for_no_counts() {
    let mut ix = one_doc_index("english", &["some words", ""]);
    for q in ["", " ", "\n\t", "...", "🙂", "\u{200d}", &"z".repeat(100)] {
        assert!(find(&mut ix, q).is_empty(), "{q:?}");
        let c = ix.counts(&[RANGE], q).unwrap();
        assert_eq!((c.docs, c.df.len()), (2, 0), "{q:?}");
    }
    // A document with no words is a row, and no query finds it.
    assert!(find(&mut ix, "some").iter().all(|k| *k == 0));
    let e = Query {
        op: Op::Match,
        probe: Probe::Text(String::new()),
        limit: 1,
        filter: None,
        exact: false,
        stats: Some(crate::impact::Stats {
            docs: 2,
            words: 2,
            df: vec![],
        }),
    };
    let pin = ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 })]).unwrap();
    assert_eq!(ix.search(&pin, &e).unwrap(), []);
    let wrong = Query {
        stats: Some(crate::impact::Stats {
            docs: 2,
            words: 2,
            df: vec![1],
        }),
        ..e
    };
    assert!(matches!(
        ix.search(&pin, &wrong),
        Err(Error::InvalidQuery(_))
    ));
}

#[test]
fn vs17_adv_identities_differ_per_language_and_hold_still() {
    let ids: Vec<String> = LANGUAGES
        .iter()
        .map(|l| Analyzer::new(l).unwrap().identity())
        .collect();
    let set: std::collections::BTreeSet<&String> = ids.iter().collect();
    assert_eq!(set.len(), LANGUAGES.len());
    assert!(
        ids.iter()
            .all(|i| i.starts_with("text/") && i.ends_with("/1"))
    );
    assert_eq!(
        Analyzer::new("english").unwrap().identity(),
        Analyzer::new("english").unwrap().clone().identity()
    );
}

// ---------------------------------------------------------------------------------------------
// Decisions that G does not implement yet. These fail today, and name the decision.

#[test]
#[ignore = "L10: text is not NFC-normalized yet (decision L10, task G follow-up)"]
fn vs17_adv_l10_composed_and_decomposed_text_yield_the_same_terms() {
    for lang in ["simple", "english", "french", "german"] {
        let a = Analyzer::new(lang).unwrap();
        for (composed, decomposed) in [
            ("café", "cafe\u{301}"),
            ("Å", "A\u{30a}"),
            ("\u{212b}", "\u{c5}"),
            ("한", "\u{1112}\u{1161}\u{11ab}"),
            ("ǅ", "ǅ"),
        ] {
            assert_eq!(
                a.terms(composed),
                a.terms(decomposed),
                "{lang}: {composed:?} and {decomposed:?}"
            );
        }
    }
    let mut ix = one_doc_index("simple", &["café", "cafe\u{301}"]);
    assert_eq!(find(&mut ix, "café").len(), 2);
    assert_eq!(find(&mut ix, "cafe\u{301}").len(), 2);
}

#[test]
#[ignore = "gap: invisible format characters stay inside a term (RLM, bidi controls, soft hyphen), so the same visible word does not match"]
fn vs17_adv_invisible_format_characters_do_not_make_a_different_word() {
    let a = Analyzer::new("simple").unwrap();
    for (visible, marked) in [
        ("abc", "\u{202e}abc\u{202c}"),
        ("שלום", "שלום\u{200f}"),
        ("cooperate", "co\u{ad}operate"),
        ("word", "wor\u{200b}d"),
        ("word", "\u{feff}word"),
    ] {
        assert_eq!(names(&a, visible), names(&a, marked), "{marked:?}");
    }
}

#[test]
fn vs17_adv_the_limit_counts_bytes_after_lowercase_and_is_exactly_sixty_four() {
    let a = Analyzer::new("simple").unwrap();
    for (n, kept) in [(63usize, true), (64, true), (65, false)] {
        assert_eq!(
            names(&a, &"q".repeat(n)).len(),
            usize::from(kept),
            "{n} bytes"
        );
    }
    // `İ` is two bytes and lowercases to three: 32 of them are 64 bytes and 96 after lowercase.
    assert!(names(&a, &"İ".repeat(32)).is_empty());
    assert_eq!(names(&a, &"İ".repeat(21)).len(), 1);
    assert_eq!(MAX_TERM_BYTES, 64);
}

#[test]
fn vs17_adv_no_two_languages_are_the_same_analyzer() {
    let words: Vec<&str> = SAMPLES
        .iter()
        .flat_map(|(_, t)| t.split(' '))
        .chain([
            "kutyák",
            "köpekler",
            "chanteurs",
            "hundar",
            "hunden",
            "hundene",
        ])
        .collect();
    let stems = |l: &str| {
        let a = Analyzer::new(l).unwrap();
        words.iter().map(|w| names(&a, w)).collect::<Vec<_>>()
    };
    for (i, a) in LANGUAGES.iter().enumerate() {
        for b in &LANGUAGES[i + 1..] {
            assert_ne!(stems(a), stems(b), "{a} and {b} stem alike");
        }
    }
}
