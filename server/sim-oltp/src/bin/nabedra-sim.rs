use std::process::ExitCode;

use nabedra_sim_oltp::sweep::{Command, run};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let command = match (args.next().as_deref(), args.next()) {
        (Some("run"), None) => Command::Run,
        (Some("sweep"), None) => Command::Sweep,
        (Some("shrink"), None) => Command::Shrink,
        _ => {
            eprintln!("usage: nabedra-sim run|sweep|shrink");
            return ExitCode::from(2);
        }
    };
    match run(command) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("OLTP simulation: {error}");
            ExitCode::from(2)
        }
    }
}
