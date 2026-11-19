mod admin;
mod clock;
mod controller;
mod discovery;
mod protocol;
mod transport;
mod worker;

pub use admin::{Command as AdminCommand, run as run_admin};

pub fn run_controller(path: &std::path::Path) -> std::io::Result<()> {
    controller::run(path)
}

pub fn run_worker(path: &std::path::Path) -> std::io::Result<()> {
    worker::run(path)
}
