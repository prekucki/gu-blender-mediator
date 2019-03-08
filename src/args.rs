use std::net::SocketAddr;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub struct Args {
    #[structopt(short = "p", long = "listen-port", default_value = "0")]
    pub listen_port: u16,

    pub gw_addr: String,
}
