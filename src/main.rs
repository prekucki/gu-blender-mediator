mod args;

use structopt::StructOpt as _;

fn main() {
    let args = args::Args::from_args();
    println!("a={:?}", args);
}
