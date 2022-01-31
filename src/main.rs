use std::path::{Path, PathBuf};

use lmdb::{Environment, EnvironmentFlags};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Cli {
    /// Path to LMDB database.
    input_db: PathBuf,
}

fn main() {
    let opts = Cli::from_args();

    let env = Environment::new()
        .set_flags(EnvironmentFlags::READ_ONLY)
        .open(&opts.input_db)
        .expect("failed to open db");

    println!("Openend LMDB environment at {}", opts.input_db.display());
}
