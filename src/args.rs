use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub struct Args {
    #[structopt(short = "p", long = "listen-port", default_value = "0")]
    pub listen_port: u16,

    // Address for gateway server
    #[structopt(long = "gw", default_value = "http://127.0.0.1:55001/")]
    pub gw_addr: String,

    // Address for webdav server,
    #[structopt(long = "dav", default_value = "http://127.0.0.1:55010")]
    pub dav_addr: String,
}
