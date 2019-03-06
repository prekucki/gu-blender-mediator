use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub struct Args {
    #[structopt(short = "p", long = "listen-port", default_value = "0")]
    listen_port: u16,
}
