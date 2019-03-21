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
mod joinact;
mod workman;

fn index(_r: HttpRequest) -> impl Responder {
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
    gw_url: String,
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
        gw_url: String,
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
        eprintln!("resource ready: state: {:?}", self.state);
        if self.state.is_ready() {
            self.start_processing(ctx)
        }
    }

    fn spec_ready(&mut self, _ctx: &mut <Self as Actor>::Context) {
        self.state.spec_ready = true;
        eprintln!("spec ready; state: {:?}", self.state);
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
            "\n\nstarting blendering!!\n  subtask={}, file={}, output={}\n",
            self.subtask_id.clone().unwrap(),
            output_path,
            output_uri
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
                .map_err(|e, _, _| eprintln!("\n\nblendering failed!!\n  err: {}", e))
                .and_then(|r, act: &mut TaskWorker, _ctx| {
                    eprintln!(
                        "\n\nblendering done!!\n  results in: {}\n  {:?}",
                        result_path, r
                    );
                    act.api
                        .subtask_result(
                            &act.node_id,
                            act.subtask_id.as_ref().unwrap(),
                            golem_gw_api::models::SubtaskResult::new(
                                "succeeded".into(),
                                result_path,
                            ),
                        )
                        .map_err(|e| eprintln!("fail send result: {}", e))
                        .and_then(|_r| Ok(eprintln!("sending results done")))
                        .into_actor(act)
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

        let mut extra_data: blender::BlenderTaskSpec =
            blender::decode(msg.0.extra_data().clone()).unwrap();

        extra_data.normalize_path();
        eprintln!(
            "\n\nsubtask {} extra data: {:?}\n\n",
            msg.0.subtask_id(),
            extra_data
        );
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

        let _ = ctx.spawn(
            self.api
                .confirm_subtask(&self.node_id, self.subtask_id.as_ref().unwrap())
                .into_actor(self)
                .map_err(|e, act, _| {
                    eprintln!("subtask {:?} confirmation failure: {}", act.subtask_id, e)
                })
                .and_then(|_r, act, _| {
                    eprintln!("subtask {:?} confirmed", act.subtask_id);
                    fut::ok(())
                }),
        );

        let upload_spec = deployment.update(vec![Command::WriteFile {
            file_path: "golem/resources/spec.json".to_string(),
            content: serde_json::to_string(&extra_data).unwrap(),
        }]);

        ActorResponse::r#async(upload_spec.into_actor(self).and_then(
            |_r, act: &mut TaskWorker, ctx| {
                act.spec_ready(ctx);
                fut::ok(())
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
            .map_err(|_e, act, _| {
                eprintln!(
                    "unable to create output dir at {}/{}",
                    act.gw_url,
                    act.task.task_id()
                )
            })
            .and_then(|r, act: &mut TaskWorker, _| {
                act.output_uri = r.to_string();
                fut::ok(())
            });

        let _ = ctx.spawn(create_output);

        eprintln!("got resource; path: {}", r.path());
        ActorResponse::r#async(upload_zip.into_actor(self).and_then(
            |r, act: &mut TaskWorker, ctx| {
                act.resource_ready(ctx);
                fut::ok(eprintln!(
                    "download-file for {}/{}: {:?}",
                    act.gw_url,
                    act.task.task_id(),
                    r
                ))
            },
        ))
    }
}

struct DoSubtaskVerification(golem_gw_api::models::SubtaskVerification);

impl Message for DoSubtaskVerification {
    type Result = Result<(), gu_client::error::Error>;
}

impl Handler<DoSubtaskVerification> for TaskWorker {
    type Result = ActorResponse<TaskWorker, (), gu_client::error::Error>;

    fn handle(&mut self, msg: DoSubtaskVerification, _ctx: &mut Self::Context) -> Self::Result {
        let s_v = &msg.0;
        if s_v.verification_result() != "OK" {
            let reason = s_v
                .reason()
                .expect("negative verification should have reason");
            eprintln!(
                "verification of {} failure : {:?}",
                s_v.subtask_id(),
                reason
            );
            return ActorResponse::reply(Err(gu_client::error::Error::Other(format!(
                "subtask {} result not accepted: {}",
                s_v.subtask_id(),
                reason
            ))));
        }

        eprintln!("subtask {} verified successfully", s_v.subtask_id());
        ActorResponse::r#async(
            self.api
                .want_to_compute_task(&self.node_id, self.task.task_id())
                .into_actor(self)
                .and_then(|m, _, _| fut::ok(eprintln!("want to compute (next) task send: {:?}", m)))
                .map_err(|e, _, _| {
                    eprintln!("want to compute (next) task failed: {:?}", e);
                    gu_client::error::Error::Other(e.to_string())
                }),
        )
    }
}

impl TaskWorker {
    fn create_deployment(&self) -> Box<dyn ActorFuture<Actor = TaskWorker, Item = (), Error = ()>> {
        Box::new(
            workman::reserve(self.task.task_id(), (*self.task.deadline()) as u64)
                .map_err(|_| /*TODO*/())
                .into_actor(self)
                .and_then(|peer_id, act: &mut TaskWorker, _| {
                    act.peer_id = Some(peer_id);
                    act.hub_session
                        .add_peers(vec![peer_id])
                        .into_actor(act)
                        .map_err(|e, act, _| {
                            eprintln!("fail to add peer {:?}: {}", act.peer_id.unwrap(), e)
                        })
                        .and_then(|_, act: &mut TaskWorker, _| {
                            blender::blender_deployment_spec(
                                act.hub_session.peer(act.peer_id.unwrap()),
                                true,
                            )
                            .into_actor(act)
                            .map_err(|e, act, _| {
                                eprintln!(
                                    "unable to create deployment @ peer: {:?}, err: {}",
                                    act.peer_id.unwrap(),
                                    e
                                )
                            })
                            .and_then(
                                |deployment, act: &mut TaskWorker, _| {
                                    act.deployment = Some(deployment);
                                    fut::ok(())
                                },
                            )
                        })
                }),
        )
    }

    fn create_deployment_with_retry(
        &self,
        retry_cnt: u32,
    ) -> Box<dyn ActorFuture<Actor = TaskWorker, Item = (), Error = ()>> {
        Box::new(self.create_deployment().then(move |r, act, _| match r {
            Ok(v) => actix::fut::Either::A(fut::ok(v)),
            Err(e) => {
                if retry_cnt > 0 {
                    actix::fut::Either::B(act.create_deployment_with_retry(retry_cnt - 1))
                } else {
                    actix::fut::Either::A(fut::err(e))
                }
            }
        }))
    }
}

impl Actor for TaskWorker {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let get_subtask = self
            .api
            .want_to_compute_task(&self.node_id, self.task.task_id())
            .into_actor(self)
            .and_then(|m, _, _| fut::ok(eprintln!("want to compute (first) task send: {:?}", m)))
            .map_err(|e, _, _| eprintln!("want to compute (first) task failed: {:?}", e));

        ctx.wait(
            joinact::join_act_fut(get_subtask, self.create_deployment_with_retry(5))
                .and_then(|_, _, _| fut::ok(())),
        )
    }
}

struct Gateway {
    gw_url: String,
    base_url: String,
    api: Option<std::rc::Rc<dyn golem_gw_api::apis::DefaultApi>>,
    hub_session: Option<gu_client::r#async::HubSessionRef>,
    last_event_id: i64,
    tasks: HashMap<String, Addr<TaskWorker>>,
}

impl Gateway {
    fn new(gw_url: String, base_url: String) -> Gateway {
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
        log::info!("Brass Gateway url={}", self.base_url);
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
                golem_gw_api::models::Subscription::new(
                    1,
                    6,
                    3 * 1024 * 1024 * 512,
                    3 * 1024 * 1024 * 512,
                )
                .with_performance(1000f32),
            )
            .and_then(|s| Ok(eprintln!("status: {:?}", s)))
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

    fn ack_event(&mut self, event_id: i64, event_hash: &str) {
        eprintln!(
            "event processed: {}/{}: {}",
            event_id, self.last_event_id, event_hash
        );
        if self.last_event_id < event_id {
            self.last_event_id = event_id;
        }
    }

    fn process_event(
        &mut self,
        ev: &golem_gw_api::models::Event,
        _ctx: &mut <Self as Actor>::Context,
    ) {
        if let Some(task) = ev.task() {
            let worker = TaskWorker::new(
                self.gw_url.clone(),
                self.api.as_ref().unwrap(),
                self.hub_session.as_ref().unwrap().clone(),
                self.node_id(),
                task,
            )
            .start();
            self.tasks.insert(task.task_id().to_owned(), worker);
            self.ack_event(ev.event_id(), task.task_id());
        } else if let Some(subtask) = ev.subtask() {
            if let Some(worker) = self.tasks.get(subtask.task_id()) {
                worker.do_send(DoSubTask(subtask.clone()))
            } else {
                eprintln!("no worker for: {}", subtask.task_id());
            }
            self.ack_event(ev.event_id(), subtask.subtask_id());
        } else if let Some(resource) = ev.resource() {
            if let Some(worker) = self.tasks.get(resource.task_id()) {
                worker.do_send(DoResource(resource.clone()))
            } else {
                eprintln!("no worker for: {}", resource.task_id());
            }
            self.ack_event(ev.event_id(), resource.path());
        } else if let Some(subtask_verification) = ev.subtask_verification() {
            if let Some(worker) = self.tasks.get(subtask_verification.task_id()) {
                worker.do_send(DoSubtaskVerification(subtask_verification.clone()))
            } else {
                eprintln!("no worker for: {}", subtask_verification.task_id());
            }
            self.ack_event(
                ev.event_id(),
                &format!(
                    "subtask {} verification: {}",
                    subtask_verification.subtask_id(),
                    subtask_verification.verification_result()
                ),
            );
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
                .map_err(|e| eprintln!("polling events failed: {}", e))
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
        let _api = self.init_api();

        let hub_connection = gu_client::r#async::HubConnection::default();

        let create_hub_session = hub_connection
            .new_session(gu_client::model::session::HubSessionSpec {
                expires: None,
                allocation: gu_client::model::session::AllocationMode::AUTO,
                name: Some(format!("gu-mediator blendering")),
                tags: std::collections::BTreeSet::new(),
            })
            .into_actor(self)
            .map_err(|e, act, ctx| {
                eprintln!("failed to create hub session {:?}: {}", act.hub_session, e);
                ctx.stop()
            })
            .and_then(|h, mut act, _| {
                act.hub_session = Some(h);
                fut::ok(())
            });

        ctx.spawn(create_hub_session);

        let f = self
            .new_subscription()
            .into_actor(self)
            .map_err(|e, _act: &mut Gateway, ctx| {
                log::error!("Unable to update subscription: {}", e);
                ctx.stop()
            });
        ctx.spawn(f.and_then(|_, act, ctx| act.pump_events(ctx)));
    }
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
