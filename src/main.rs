use std::path::{Path, PathBuf};

use indicatif::ProgressIterator;
use lmdb::{Cursor, Environment, EnvironmentFlags, Transaction};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Cli {
    /// Path to LMDB database.
    input_db: PathBuf,

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

    if let Some(ref db_name) = opts.db_name {
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
            //  println!("{:?} {:?}", key, value);
        }

        println!("done");
    }
}
