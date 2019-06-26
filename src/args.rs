use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub struct Args {
    #[structopt(short = "p", long = "listen-port", default_value = "0")]
    pub listen_port: u16,

    #[structopt(long = "local")]
    pub local: bool,

    #[structopt(short="s", long = "work-dir", default_value = "")]
    pub work_dir : String,
}
