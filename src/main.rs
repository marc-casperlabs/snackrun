use std::{
    collections::HashMap,
    ops::Add,
    path::{Path, PathBuf},
};

use generic_array::typenum;
use indicatif::ProgressIterator;
use lmdb::{Cursor, Environment, EnvironmentFlags, Transaction};
use structopt::StructOpt;
use stuffer_shack::StufferShack;

#[derive(Debug, StructOpt)]
struct Cli {
    /// Path to LMDB database.
    input_db: PathBuf,

    /// Stuffer shack to output to.
    #[structopt(long, short)]
    output: Option<PathBuf>,

    /// Key size to use
    #[structopt(long, short, default_value = "32")]
    key_size: u16,

    /// Database to copy.
    #[structopt(long, short)]
    db_name: Option<String>,
}

fn main() {
    let opts = Cli::from_args();

    let env = Environment::new()
        .set_flags(EnvironmentFlags::NO_SUB_DIR | EnvironmentFlags::READ_ONLY)
        .set_max_dbs(16)
        .open(&opts.input_db)
        .expect("failed to open db");

    println!("openend LMDB environment at {}", opts.input_db.display());

    // List all databases, just in case.
    let root_db = env.open_db(None).expect("could not open root db");
    for (key, value) in env
        .begin_ro_txn()
        .unwrap()
        .open_ro_cursor(root_db)
        .unwrap()
        .iter_start()
    {
        println!("db seen: {}", String::from_utf8_lossy(key));
    }

    // This is inaccurate, but all the `lmdb` crate exposes. Other crates have per-db stats.
    let entry_count = env.stat().unwrap().entries();
    println!("number of entries in all dbs: {:?}", entry_count);

    if let (Some(ref db_name), Some(ref output)) = (opts.db_name, opts.output) {
        assert!(!output.exists());

        let mut shack: StufferShack<typenum::U32> =
            StufferShack::open_disk(output).expect("could not open stuffer shack");

        let mut key_lens: HashMap<usize, u64> = HashMap::new();
        let mut value_lens: HashMap<usize, u64> = HashMap::new();

        env.begin_ro_txn()
            .unwrap()
            .get(root_db, db_name)
            .expect("could not retrieve source db key in root");

        let target_db = env.open_db(None).expect("could not open source db");
        println!("found {}", db_name);

        // Now we can transfor over every key.
        let txn = env.begin_ro_txn().unwrap();
        let db_iter = txn.open_ro_cursor(root_db).unwrap().iter_start();
        for (key, value) in db_iter.progress_count(entry_count as u64) {
            // Count entries for stats.
            *key_lens.entry(key.len()).or_default() += 1;
            *value_lens.entry(value.len()).or_default() += 1;

            // Write to shack, padding or shortening the key.
            let mut new_key = [0xFFu8; 32];
            let l = key.len().min(32);
            new_key[..l].copy_from_slice(&key[..l]);
            shack.write(new_key.into(), value);
        }

        println!("done");

        println!("keys lengths: {:#?}", key_lens);
        println!("value lengths: {:#?}", value_lens);
    }
}
