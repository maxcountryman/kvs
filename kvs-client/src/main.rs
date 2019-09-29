use std::net::SocketAddr;
use std::process::exit;

use structopt::StructOpt;

use kvs::error;
use kvs::KvsClient;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-client", about = "Kvs client interface.")]
struct Opt {
    #[structopt(subcommand)]
    command: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    #[structopt(name = "set")]
    Set {
        #[structopt(index = 1, required = true)]
        key: String,
        #[structopt(index = 2, required = true)]
        value: String,
        #[structopt(short, long, parse(try_from_str), default_value = DEFAULT_LISTENING_ADDRESS)]
        addr: SocketAddr,
    },

    #[structopt(name = "get")]
    Get {
        #[structopt(index = 1, required = true)]
        key: String,
        #[structopt(short, long, required = false, default_value = DEFAULT_LISTENING_ADDRESS)]
        addr: SocketAddr,
    },

    #[structopt(name = "rm")]
    Remove {
        #[structopt(index = 1, required = true)]
        key: String,
        #[structopt(short, long, required = false, default_value = DEFAULT_LISTENING_ADDRESS)]
        addr: SocketAddr,
    },
}

fn main() {
    let opt = Opt::from_args();
    if let Err(e) = run(opt) {
        eprintln!("{}", e);
        exit(1);
    }
}

fn run(opt: Opt) -> error::Result<()> {
    match opt.command {
        Command::Get { key, addr } => {
            let client = KvsClient::connect(addr)?;
            if let Some(value) = client.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Command::Set { key, value, addr } => {
            let client = KvsClient::connect(addr)?;
            client.set(key, value)?;
        }
        Command::Remove { key, addr } => {
            let client = KvsClient::connect(addr)?;
            client.remove(key)?;
        }
    }

    Ok(())
}
