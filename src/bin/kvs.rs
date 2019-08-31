use structopt::StructOpt;

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

fn main() {
    let opt = Kvs::from_args();

    match opt {
        Kvs::Set { key: _, value: _ } => panic!("unimplemented"),
        Kvs::Get { key: _ } => panic!("unimplemented"),
        Kvs::Remove { key: _ } => panic!("unimplemented"),
    }
}
