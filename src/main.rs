use actix::prelude::*;
use actix_web::{http, server, App, HttpRequest, Responder};
use structopt::StructOpt;

use gateway::Gateway;

mod args;

mod blender;
mod dav;
mod gateway;
mod joinact;
mod subtask_worker;
mod task_worker;
mod workman;

fn index(_r: HttpRequest) -> impl Responder {
    format!("Hello")
}

fn main() {
    env_logger::init();
    let args = args::Args::from_args();

    let _gw = Gateway::new(args.dav_addr, args.gw_addr).start();
    //let client = ::hyper::client::Client::
    //golem_gw_api::apis::configuration::Configuration::new()

    server::new(|| App::new().route("/", http::Method::GET, index))
        .bind("127.0.0.1:33433")
        .unwrap()
        .run()
}
