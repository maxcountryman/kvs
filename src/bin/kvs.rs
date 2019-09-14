use std::path::Path;
use std::process::exit;

use structopt::StructOpt;

use kvs::{KvStore, KvsError, Result};

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs", about = "Stores key-value pairs.")]
enum Kvs {
    #[structopt(name = "set")]
    Set {
        #[structopt(index = 1, required = true)]
        key: String,
        #[structopt(index = 2, required = true)]
        value: String,
    },

    #[structopt(name = "get")]
    Get {
        #[structopt(index = 1, required = true)]
        key: String,
    },

    #[structopt(name = "rm")]
    Remove {
        #[structopt(index = 1, required = true)]
        key: String,
    },
}

fn main() -> Result<()> {
    let log = Path::new("./");
    let mut store = KvStore::open(log)?;

    match Kvs::from_args() {
        Kvs::Set { key, value } => store.set(key, value),
        Kvs::Get { key } => match store.get(key)? {
            None => {
                println!("Key not found");
                exit(0);
            }
            Some(value) => {
                println!("{}", value);
                exit(0);
            }
        },
        Kvs::Remove { key } => match store.remove(key) {
            Err(KvsError::KeyNotFound) => {
                println!("Key not found");
                exit(1);
            }
            Err(e) => Err(e),
            Ok(_) => Ok(()),
        },
    }
}
