use super::task_worker::{DoResource, DoSubTask, DoSubtaskVerification, TaskWorker};
use actix::prelude::*;
/** Module responsible for signle HUB session.

Traces given hub session.

**/
use futures::prelude::*;
use gu_client::r#async::HubSessionRef;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

struct GwSessionConfiguration {
    /// Etherium address for receiveing payments.
    eth_addr: String,
}

struct GatewaySession {
    hub_session: HubSessionRef,
}

pub struct Gateway {
    gw_url: String,
    base_url: String,
    api: Option<std::rc::Rc<dyn golem_gw_api::apis::DefaultApi>>,
    hub_session: Option<gu_client::r#async::HubSessionRef>,
    last_event_id: i64,
    tasks: HashMap<String, Addr<TaskWorker>>,
}

impl Gateway {
    pub fn new(gw_url: String, base_url: String) -> Gateway {
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
