mod args;

use crate::workman::WorkMan;
use actix::prelude::*;
use actix_web::{http, server, App, HttpRequest, Path, Responder};
use futures::prelude::*;
use golem_gw_api::models::Subtask;
use gu_client::NodeId;
use hyper;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::Deref as _;
use std::rc::Rc;
use std::time::Duration;
use structopt::StructOpt;

mod blender;
mod dav;
mod joinact;
mod workman;

fn index(r: HttpRequest) -> impl Responder {
    format!("Hello")
}

#[derive(Default, Debug)]
struct State {
    resource_ready: bool,
    spec_ready: bool,
}

impl State {
    #[inline]
    fn is_ready(&self) -> bool {
        self.resource_ready && self.spec_ready
    }
}

struct TaskWorker {
    gw_url : String,
    api: Rc<dyn golem_gw_api::apis::DefaultApi>,
    hub_session: gu_client::r#async::HubSessionRef,
    deployment: Option<gu_client::r#async::PeerSession>,
    spec: Option<blender::BlenderTaskSpec>,
    task: golem_gw_api::models::Task,
    subtask_id: Option<String>,
    node_id: String,
    peer_id: Option<NodeId>,
    state: State,
    output_uri: String,
}

impl TaskWorker {
    fn new(
        gw_url : String,
        api: &Rc<dyn golem_gw_api::apis::DefaultApi>,
        hub_session: gu_client::r#async::HubSessionRef,
        node_id: &str,
        task: &golem_gw_api::models::Task,
    ) -> Self {
        TaskWorker {
            gw_url,
            api: api.clone(),
            hub_session,
            node_id: node_id.to_owned(),
            task: task.clone(),
            peer_id: None,
            deployment: None,
            state: State::default(),
            output_uri: String::default(),
            spec: None,
            subtask_id: None,
        }
    }

    fn resource_ready(&mut self, ctx: &mut <Self as Actor>::Context) {
        self.state.resource_ready = true;
        eprintln!("new state={:?}", self.state);
        if self.state.is_ready() {
            self.start_processing(ctx)
        }
    }

    fn spec_ready(&mut self, ctx: &mut <Self as Actor>::Context) {
        self.state.spec_ready = true;
        eprintln!("new state={:?}", self.state);
        if self.state.is_ready() {
            self.start_processing(ctx)
        }
    }

    fn start_processing(&mut self, ctx: &mut <Self as Actor>::Context) {
        use gu_client::model::envman::{Command, ResourceFormat};

        let deployment = match self.deployment.as_ref() {
            Some(d) => d.clone(),
            None => {
                eprintln!("!!! deployment not ready !!!");
                return;
            }
        };

        let output_file_name = self.spec.as_ref().unwrap().expected_output_file_name();
        let output_path = format!("/golem/output/{}", output_file_name);
        let output_uri = format!("{}/{}", self.output_uri, output_file_name);
        let result_path = format!("{}/output", self.task.task_id());

        eprintln!(
            "starting blendering!! file={}, output={}",
            output_path, output_uri
        );
        let compute = self.hub_session.new_blob().and_then(move |b| {
            deployment.update(vec![
                Command::Open,
                Command::Wait,
                Command::UploadFile {
                    uri: b.uri(),
                    file_path: output_path.clone(),
                    format: ResourceFormat::Raw,
                },
                Command::UploadFile {
                    uri: output_uri,
                    file_path: output_path,
                    format: ResourceFormat::Raw,
                },
            ])
        });

        ctx.spawn(
            compute
                .into_actor(self)
                .map_err(|e, _, _| eprintln!("fail blendering"))
                .and_then(|r, act: &mut TaskWorker, ctx| {
                    eprintln!("done results in {}", result_path);
                    act.api
                        .subtask_result(
                            &act.node_id,
                            act.subtask_id.as_ref().unwrap(),
                            golem_gw_api::models::SubtaskResult::new("succeeded".into(), result_path),
                        )
                        .map_err(|e| eprintln!("fail send result: {}", e))
                        .and_then(|r| Ok(eprintln!("done")))
                        .into_actor(act)
                }),
        );
        let _ = ctx.spawn(
            self.api
                .confirm_subtask(&self.node_id, self.task.task_id())
                .into_actor(self)
                .map_err(|e, act, _| eprintln!("confirm subtask failure: {}", e))
                .and_then(|r, _, _| {
                    eprintln!("subtask confirmend");
                    fut::ok(())
                }),
        );
    }
}

struct DoSubTask(Subtask);

impl Message for DoSubTask {
    type Result = Result<(), gu_client::error::Error>;
}

impl Handler<DoSubTask> for TaskWorker {
    type Result = ActorResponse<TaskWorker, (), gu_client::error::Error>;

    fn handle(&mut self, msg: DoSubTask, ctx: &mut Self::Context) -> Self::Result {
        use gu_client::model::envman::Command;
        eprintln!("update subtask {:?}", msg.0);
        let mut extra_data: blender::BlenderTaskSpec =
            serde_json::from_value(msg.0.extra_data().clone()).unwrap();
        extra_data.normalize_path();
        eprintln!("\n\n{:?}\n\n", extra_data);
        self.spec = Some(extra_data.clone());
        self.subtask_id = Some(msg.0.subtask_id().clone());

        let deployment = match self.deployment.as_ref() {
            Some(d) => d,
            None => {
                return ActorResponse::reply(Err(gu_client::error::Error::Other(
                    "deployment not ready".into(),
                )));
            }
        };

        let upload_spec = deployment.update(vec![Command::WriteFile {
            file_path: "golem/resources/spec.json".to_string(),
            content: serde_json::to_string(&extra_data).unwrap(),
        }]);

        ActorResponse::r#async(upload_spec.into_actor(self).and_then(
            |r, act: &mut TaskWorker, ctx| {
                act.spec_ready(ctx);
                fut::ok(eprintln!("spec: {:?}", r))
            },
        ))
    }
}

struct DoResource(golem_gw_api::models::Resource);

impl Message for DoResource {
    type Result = Result<(), gu_client::error::Error>;
}

impl Handler<DoResource> for TaskWorker {
    type Result = ActorResponse<TaskWorker, (), gu_client::error::Error>;

    fn handle(&mut self, msg: DoResource, ctx: &mut Self::Context) -> Self::Result {
        use gu_client::model::envman::{Command, ResourceFormat};

        let r = &msg.0;
        let zip_uri = format!("{}/{}/{}", self.gw_url, r.path(), r.task_id());
        let task_uri = format!("{}/{}", self.gw_url, r.task_id());

        let deployment = match self.deployment.as_ref() {
            Some(d) => d,
            None => {
                return ActorResponse::reply(Err(gu_client::error::Error::Other(
                    "deployment not ready".into(),
                )));
            }
        };

        let upload_zip = deployment.update(vec![Command::DownloadFile {
            uri: zip_uri,
            file_path: "/golem/resources/gu.zip".to_string(),
            format: ResourceFormat::Raw,
        }]);

        let create_output = dav::DavPath::new(task_uri.parse().unwrap())
            .mkdir("output")
            .into_actor(self)
            .map_err(|e, _, _| eprintln!("unable to create output dir"))
            .and_then(|r, act: &mut TaskWorker, _| {
                act.output_uri = r.to_string();
                fut::ok(())
            });

        let _ = ctx.spawn(create_output);

        eprintln!("resource = {:?}", msg.0);
        ActorResponse::r#async(upload_zip.into_actor(self).and_then(
            |r, act: &mut TaskWorker, ctx| {
                act.resource_ready(ctx);
                fut::ok(eprintln!("download-file: {:?}", r))
            },
        ))
    }
}

impl Actor for TaskWorker {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let ack_task = self
            .api
            .want_to_compute_task(&self.node_id, self.task.task_id())
            .into_actor(self)
            .and_then(|m, act, ctx| fut::ok(eprintln!("message={:?}", m)))
            .map_err(|e, _, _| eprintln!("err={:?}", e));

        let create_deployment =
            workman::reserve(self.task.task_id(), (*self.task.deadline()) as u64)
                .map_err(|_| /*TODO*/())
                .into_actor(self)
                .and_then(|peer_id, act: &mut TaskWorker, ctx| {
                    act.peer_id = Some(peer_id);
                    act.hub_session
                        .add_peers(vec![peer_id])
                        .into_actor(act)
                        .map_err(|e, _, _| eprintln!("fail to add peer: {}", e))
                        .and_then(|_, act: &mut TaskWorker, ctx| {
                            blender::blender_deployment_spec(
                                act.hub_session.peer(act.peer_id.unwrap()),
                                true,
                            )
                            .into_actor(act)
                            .map_err(|e, _, _| eprintln!("unable to create deployment: {}", e))
                            .and_then(
                                |deployment, act: &mut TaskWorker, _| {
                                    act.deployment = Some(deployment);
                                    fut::ok(())
                                },
                            )
                        })
                });

        ctx.wait(joinact::join_act_fut(ack_task, create_deployment).and_then(|_, _, _| fut::ok(())))
    }
}

struct Gateway {
    gw_url : String,
    base_url: String,
    api: Option<std::rc::Rc<dyn golem_gw_api::apis::DefaultApi>>,
    hub_session: Option<gu_client::r#async::HubSessionRef>,
    last_event_id: i64,
    tasks: HashMap<String, Addr<TaskWorker>>,
}

impl Gateway {
    fn new(gw_url : String, base_url: String) -> Gateway {
        Gateway {
            gw_url,
            base_url,
            api: None,
            last_event_id: -1,
            tasks: HashMap::new(),
            hub_session: None,
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
                golem_gw_api::models::Subscription::new(1, 6, 3 * 1024 * 1024 * 512, 3 * 1024 * 1024 * 512)
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
        eprintln!(
            "event_id={}/{}: {:?}",
            ev.event_id(),
            self.last_event_id,
            ev
        );
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
            eprintln!("processing event={:?}", ev);
            let worker = TaskWorker::new(
                self.gw_url.clone(),
                self.api.as_ref().unwrap(),
                self.hub_session.as_ref().unwrap().clone(),
                self.node_id(),
                task,
            )
            .start();
            self.tasks.insert(task.task_id().to_owned(), worker);
            self.ack_event(ev);
        } else if let Some(subtask) = ev.subtask() {
            if let Some(worker) = self.tasks.get(subtask.task_id()) {
                worker.do_send(DoSubTask(subtask.clone()))
            } else {
                eprintln!("no worker for: {}", subtask.task_id());
            }
            self.ack_event(ev);
        } else if let Some(resource) = ev.resource() {
            if let Some(worker) = self.tasks.get(resource.task_id()) {
                worker.do_send(DoResource(resource.clone()))
            } else {
                eprintln!("no worker for: {}", resource.task_id());
            }
            self.ack_event(ev);
        } else {
            eprintln!("invalid event={:?}", ev);
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

        let hub_connection = gu_client::r#async::HubConnection::default();

        let create_hub_session = hub_connection
            .new_session(gu_client::model::session::HubSessionSpec {
                expires: None,
                allocation: gu_client::model::session::AllocationMode::AUTO,
                name: Some(format!("gu-mediator bledering")),
                tags: std::collections::BTreeSet::new(),
            })
            .into_actor(self)
            .map_err(|e, _, ctx| {
                eprintln!("failed to create hub session: {}", e);
                ctx.stop()
            })
            .and_then(|h, mut act, _| {
                act.hub_session = Some(h);
                fut::ok(())
            });

        ctx.spawn(create_hub_session);

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

    let gw = Gateway::new(args.dav_addr, args.gw_addr).start();
    //let client = ::hyper::client::Client::
    //golem_gw_api::apis::configuration::Configuration::new()

    server::new(|| App::new().route("/", http::Method::GET, index))
        .bind("127.0.0.1:33433")
        .unwrap()
        .run()
}
