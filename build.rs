use std::env;
use structopt::clap::*;
include!("src/args.rs");

fn main() {
    let outdir = match env::var_os("OUT_DIR") {
        None => return,
        Some(outdir) => outdir,
    };
    Args::clap().gen_completions("gu-blender-mediator", Shell::Bash, outdir);
    println!("rerun-if-changed:src/args.rs")
}
