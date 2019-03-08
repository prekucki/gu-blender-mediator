mod args;

use actix::prelude::*;
use actix_web::{http, server, App, HttpRequest, Path, Responder};
use futures::prelude::*;
use hyper;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::Deref as _;
use std::rc::Rc;
use std::time::Duration;
use structopt::StructOpt;

fn index(r: HttpRequest) -> impl Responder {
    format!("Hello")
}

struct TaskWorker {
    api: Rc<dyn golem_gw_api::apis::DefaultApi>,
    task: golem_gw_api::models::Task,
    node_id: String,
}

impl TaskWorker {
    fn new(
        api: &Rc<dyn golem_gw_api::apis::DefaultApi>,
        node_id: &str,
        task: &golem_gw_api::models::Task,
    ) -> Self {
        TaskWorker {
            api: api.clone(),
            node_id: node_id.to_owned(),
            task: task.clone(),
        }
    }
}

impl Actor for TaskWorker {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let _ = ctx.spawn(
            self.api
                .want_to_compute_task(&self.node_id, self.task.task_id())
                .into_actor(self)
                .and_then(|m, act, ctx| fut::ok(eprintln!("message={:?}", m)))
                .map_err(|e, _, _| eprintln!("err={:?}", e)),
        );

    }
}

struct Gateway {
    base_url: String,
    api: Option<std::rc::Rc<dyn golem_gw_api::apis::DefaultApi>>,
    last_event_id: i64,
    tasks: HashMap<String, Addr<TaskWorker>>,
}

impl Gateway {
    fn new(base_url: String) -> Gateway {
        Gateway {
            base_url,
            api: None,
            last_event_id: -1,
            tasks: HashMap::new(),
        }
    }

    fn api(&self) -> &golem_gw_api::apis::DefaultApi {
        self.api.as_ref().unwrap().as_ref()
    }

    fn init_api(&mut self) -> &golem_gw_api::apis::DefaultApi {
        let http_client = hyper::client::Client::new();
        let mut api_configuration =
            golem_gw_api::apis::configuration::Configuration::new(http_client);
        api_configuration.base_path = self.base_url.clone();
        let api = Rc::new(golem_gw_api::apis::DefaultApiClient::new(Rc::new(
            api_configuration,
        )));

        self.api = Some(api);
        self.api.as_ref().unwrap().as_ref()
    }

    fn node_id(&self) -> &str {
        "0x72cde436f012107b3b1968475b5bd6b2c9a2b948"
    }

    fn task_type(&self) -> &str {
        "Blender"
    }

    fn new_subscription(&self) -> impl Future<Item = (), Error = failure::Error> {
        self.api()
            .subscribe(
                self.node_id(),
                self.task_type(),
                golem_gw_api::models::Subscription::new(10, 6, 4 * 1024 * 1024, 4 * 1024 * 1024)
                    .with_performance(1000f32),
            )
            .and_then(|s| Ok(eprintln!("status={:?}", s)))
            .from_err()
    }

    fn poll_events(
        &self,
        _ctx: &mut <Self as Actor>::Context,
    ) -> impl Future<Item = Vec<golem_gw_api::models::Event>, Error = failure::Error> {
        self.api()
            .fetch_events(self.node_id(), self.task_type(), self.last_event_id)
            .from_err()
    }

    fn ack_event(&mut self, ev: &golem_gw_api::models::Event) {
        eprintln!("event_id={}/{}: {:?}", ev.event_id(), self.last_event_id, ev);
        if self.last_event_id < ev.event_id() {
            self.last_event_id = ev.event_id();
        }
    }

    fn process_event(
        &mut self,
        ev: &golem_gw_api::models::Event,
        ctx: &mut <Self as Actor>::Context,
    ) {
        if let Some(task) = ev.task() {
            let worker = TaskWorker::new(self.api.as_ref().unwrap(), self.node_id(), task).start();
            self.ack_event(ev);
        } else {
            eprintln!("ev={:?}", ev);
        }
    }

    fn pump_events(
        &self,
        ctx: &mut <Self as Actor>::Context,
    ) -> impl ActorFuture<Actor = Self, Item = (), Error = ()> {
        ctx.run_interval(Duration::from_secs(1), |act, ctx| {
            let f = act
                .poll_events(ctx)
                .map_err(|e| eprintln!("err={}", e))
                .into_actor(act)
                .and_then(|events, act, ctx| {
                    eprintln!("evs");
                    for ev in events {
                        act.process_event(&ev, ctx)
                    }
                    fut::ok(())
                });
            ctx.spawn(f);
        });
        fut::ok(())
    }
}

impl Actor for Gateway {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let api = self.init_api();

        let f = self
            .new_subscription()
            .map_err(|e| eprintln!("error {:?}", e));
        ctx.spawn(
            f.into_actor(self)
                .and_then(|_, act, ctx| act.pump_events(ctx)),
        );
    }
}

fn main() {
    let args = args::Args::from_args();

    let gw = Gateway::new(args.gw_addr).start();
    //let client = ::hyper::client::Client::
    //golem_gw_api::apis::configuration::Configuration::new()

    server::new(|| App::new().route("/", http::Method::GET, index))
        .bind("127.0.0.1:33433")
        .unwrap()
        .run()
}
