//! Test switches that put a broken rule back, so that a test can show it catches the break. Product
//! builds have none.

#[cfg(test)]
thread_local! {
    pub static MUTANT: std::cell::Cell<&'static str> = const { std::cell::Cell::new("none") };
}

#[cfg(test)]
pub fn mutant(name: &str) -> bool {
    MUTANT.with(|m| m.get() == name)
}

#[cfg(not(test))]
#[inline(always)]
pub fn mutant(_name: &str) -> bool {
    false
}
