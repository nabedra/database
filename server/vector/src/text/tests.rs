//! VS17 for analyzers: what a text yields, and that no text breaks them.

use nabedra_check::Gen;
use nabedra_check::prop::{Config as Runs, check_with};

mod adversary;

use super::*;

fn names(a: &Analyzer, text: &str) -> Vec<String> {
    let mut out = Vec::new();
    a.each(text, |t| out.push(t.to_owned()));
    out
}

#[test]
fn vs17_words_are_split_lowercased_and_stemmed() {
    let en = Analyzer::new("english").unwrap();
    assert_eq!(
        names(&en, "Running RUNNERS, the runner's run!"),
        ["run", "runner", "the", "runner", "run"]
    );
    let simple = Analyzer::new("simple").unwrap();
    assert_eq!(names(&simple, "Running RUNNERS"), ["running", "runners"]);
    // Word boundaries of UAX #29: a decimal number and an apostrophe stay inside a word, a hyphen
    // and a slash split.
    assert_eq!(
        names(&simple, "3.14 can't e-mail a/b"),
        ["3.14", "can't", "e", "mail", "a", "b"]
    );
    let de = Analyzer::new("german").unwrap();
    assert_eq!(names(&de, "Häuser"), names(&de, "häuser"));
    let t = en.terms("run running runs");
    assert_eq!(t.terms, [("run".to_owned(), 3)]);
    assert_eq!(t.words, 3);
}

#[test]
fn vs17_a_final_sigma_is_lowercased_as_one() {
    let a = Analyzer::new("simple").unwrap();
    assert_eq!(names(&a, "ΟΔΟΣ"), ["οδος"]);
    assert_eq!(names(&a, "ΟΔΟΣ"), names(&a, "οδος"));
}

#[test]
fn vs17_words_beyond_the_limit_are_left_out() {
    let a = Analyzer::new("simple").unwrap();
    let fits = "a".repeat(MAX_TERM_BYTES);
    let long = "a".repeat(MAX_TERM_BYTES + 1);
    assert_eq!(
        names(&a, &format!("{fits} {long} b")),
        [fits.clone(), "b".to_owned()]
    );
    // `İ` lowercases to two code points, so the word grows beyond the limit.
    let grows = "İ".repeat(MAX_TERM_BYTES / 2);
    assert_eq!(grows.len(), MAX_TERM_BYTES);
    assert_eq!(names(&a, &grows), Vec::<String>::new());
}

#[test]
fn vs17_languages_are_known_and_fit_the_server_rule() {
    for l in LANGUAGES {
        let a = Analyzer::new(l).unwrap();
        assert_eq!(a.language(), *l);
        assert_eq!(a.clone(), a);
        let fits =
            (1..=32).contains(&l.len()) && l.bytes().all(|c| c.is_ascii_lowercase() || c == b'_');
        assert!(fits, "{l}");
        assert!(!names(&a, "word").is_empty(), "{l}");
    }
    for l in ["", "English", "klingon", "en", "english "] {
        assert_eq!(
            Analyzer::new(l),
            Err(Error::InvalidQuery("unknown full-text language"))
        );
    }
    assert_ne!(
        Analyzer::new("english").unwrap().identity(),
        Analyzer::new("simple").unwrap().identity()
    );
}

/// Characters that break naive code: combining marks, joiners, emoji sequences, bidi controls,
/// the replacement character a lone surrogate becomes, NUL, scripts without spaces, case changes
/// that change the length.
const ODD: &[&str] = &[
    "a",
    "Z",
    "1",
    " ",
    "\u{0}",
    "\u{301}",
    "\u{200d}",
    "\u{200b}",
    "\u{fe0f}",
    "👩",
    "🏽",
    "🇫🇷",
    "\u{202e}",
    "\u{fffd}",
    "\u{feff}",
    "ß",
    "ẞ",
    "İ",
    "ı",
    "Σ",
    "ς",
    "ǅ",
    "ﬃ",
    "한",
    "ไทย",
    "中",
    "日本",
    "ا",
    "ل",
    "ـ",
    "॥",
    "क्ष",
    "ö",
    "o\u{308}",
    "'",
    "-",
    ".",
    ",",
    "_",
    "\n",
    "\t",
    "\u{2028}",
    "Ⅻ",
    "½",
    "𝔘",
    "\u{e000}",
];

fn odd_text(g: &mut Gen, max: usize) -> String {
    g.vec(max, |g| *g.pick(ODD)).concat()
}

#[test]
fn vs17_no_text_breaks_an_analyzer() {
    let analyzers: Vec<Analyzer> = LANGUAGES
        .iter()
        .map(|l| Analyzer::new(l).unwrap())
        .collect();
    check_with(
        Runs {
            cases: 300,
            ..Runs::default()
        },
        "vs17_no_text_breaks_an_analyzer",
        |g| {
            let text = odd_text(g, 40);
            for a in &analyzers {
                let t = a.terms(&text);
                let total: u64 = t.terms.iter().map(|(_, n)| u64::from(*n)).sum();
                if total != t.words || t.words > text.chars().count() as u64 {
                    return Err(format!("{}: {text:?} counts {t:?}", a.language()));
                }
                let sorted = t.terms.windows(2).all(|w| w[0].0 < w[1].0);
                let bounded = t
                    .terms
                    .iter()
                    .all(|(s, _)| !s.is_empty() && s.len() <= MAX_TERM_BYTES);
                if !sorted || !bounded {
                    return Err(format!("{}: {text:?} yields {t:?}", a.language()));
                }
                // The same text yields the same terms.
                if a.terms(&text) != t {
                    return Err(format!("{}: {text:?} is not deterministic", a.language()));
                }
            }
            Ok(())
        },
    );
}

#[test]
fn vs17_a_huge_text_is_analyzed_in_bounds() {
    let a = Analyzer::new("english").unwrap();
    let one = "word ".repeat(200_000);
    let t = a.terms(&one);
    assert_eq!((t.terms.len(), t.words), (1, 200_000));
    let many: String = (0..100_000).map(|i| format!("w{i} ")).collect();
    assert_eq!(a.terms(&many).terms.len(), 100_000);
    assert_eq!(a.terms("").words, 0);
    assert_eq!(a.terms(" ,.;!? \n").words, 0);
}

/// A format character between a letter and its mark goes, and the two compose (L10 after L11).
#[test]
fn vs17_a_format_character_inside_a_letter_leaves_it_whole() {
    let a = Analyzer::new("simple").unwrap();
    for marked in ["cafe\u{ad}\u{301}", "cafe\u{200b}\u{301}", "ca\u{2060}fe\u{301}", "café\u{feff}"] {
        assert_eq!(names(&a, marked), ["caf\u{e9}"], "{marked:?}");
    }
    assert_eq!(a.terms("cafe\u{ad}\u{301}"), a.terms("café"));
}
