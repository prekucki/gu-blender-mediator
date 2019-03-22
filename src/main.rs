use std::collections::HashMap;
use std::ops::Deref as _;
use std::rc::Rc;
use std::time::Duration;

use actix::prelude::*;
use actix_web::{http, server, App, HttpRequest, Responder};
use futures::prelude::*;
use golem_gw_api::models::Subtask;
use gu_client::NodeId;
use hyper;
use structopt::StructOpt;

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

use gateway::Gateway;

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
