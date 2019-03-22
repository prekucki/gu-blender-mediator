use futures::prelude::*;
use actix::prelude::*;
use gu_client::NodeId;
use golem_gw_api::models::Subtask;
use super::blender;
use std::rc::Rc;
use super::{dav, workman, joinact::join_act_fut};

pub struct TaskWorker {
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

pub struct DoSubTask(pub Subtask);

impl Message for DoSubTask {
    type Result = Result<(), gu_client::error::Error>;
}

pub struct DoResource(pub golem_gw_api::models::Resource);

impl Message for DoResource {
    type Result = Result<(), gu_client::error::Error>;
}

pub struct DoSubtaskVerification(pub golem_gw_api::models::SubtaskVerification);

impl Message for DoSubtaskVerification {
    type Result = Result<(), gu_client::error::Error>;
}



impl TaskWorker {
    pub fn new(
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
            self.subtask_id.clone().unwrap(), output_path, output_uri
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
                    eprintln!("\n\nblendering done!!\n  results in: {}\n  {:?}", result_path, r);
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


impl Handler<DoSubTask> for TaskWorker {
    type Result = ActorResponse<TaskWorker, (), gu_client::error::Error>;

    fn handle(&mut self, msg: DoSubTask, ctx: &mut Self::Context) -> Self::Result {
        use gu_client::model::envman::Command;

        let mut extra_data: blender::BlenderTaskSpec =
            blender::decode(msg.0.extra_data().clone()).unwrap();

        extra_data.normalize_path();
        eprintln!("\n\nsubtask {} extra data: {:?}\n\n", msg.0.subtask_id(), extra_data);
        self.spec = Some(extra_data.clone());
        self.subtask_id = Some(msg.0.subtask_id().clone());


        let _ = ctx.spawn(
            self.api
                .confirm_subtask(&self.node_id, self.subtask_id.as_ref().unwrap())
                .into_actor(self)
                .map_err(|e, act, _| eprintln!("subtask {:?} confirmation failure: {}",
                                               act.subtask_id, e))
                .and_then(|_r, act, _| {
                    eprintln!("subtask {:?} confirmed", act.subtask_id);
                    fut::ok(())
                }),
        );

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
            |_r, act: &mut TaskWorker, ctx| {
                act.spec_ready(ctx);
                fut::ok(())
            },
        ))
    }
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
            .map_err(|_e, act, _| eprintln!("unable to create output dir at {}/{}", act.gw_url, act.task.task_id()))
            .and_then(|r, act: &mut TaskWorker, _| {
                act.output_uri = r.to_string();
                fut::ok(())
            });

        let _ = ctx.spawn(create_output);

        eprintln!("got resource; path: {}", r.path());
        ActorResponse::r#async(upload_zip.into_actor(self).and_then(
            |r, act: &mut TaskWorker, ctx| {
                act.resource_ready(ctx);
                fut::ok(eprintln!("download-file for {}/{}: {:?}", act.gw_url, act.task.task_id(), r))
            },
        ))
    }
}

impl Handler<DoSubtaskVerification> for TaskWorker {
    type Result = ActorResponse<TaskWorker, (), gu_client::error::Error>;

    fn handle(&mut self, msg: DoSubtaskVerification, _ctx: &mut Self::Context) -> Self::Result {

        let s_v = &msg.0;
        if s_v.verification_result() != "OK" {
            let reason = s_v
                .reason()
                .expect("negative verification should have reason");
            eprintln!("verification of {} failure : {:?}", s_v.subtask_id(), reason);
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

impl Actor for TaskWorker {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let ack_task = self
            .api
            .want_to_compute_task(&self.node_id, self.task.task_id())
            .into_actor(self)
            .and_then(|m, _, _| fut::ok(eprintln!("want to compute (first) task send: {:?}", m)))
            .map_err(|e, _, _| eprintln!("want to compute (first) task failed: {:?}", e));

        let create_deployment =
            workman::reserve(self.task.task_id(), (*self.task.deadline()) as u64)
                .map_err(|_| /*TODO*/())
                .into_actor(self)
                .and_then(|peer_id, act: &mut TaskWorker, _ctx| {
                    act.peer_id = Some(peer_id);
                    act.hub_session
                        .add_peers(vec![peer_id])
                        .into_actor(act)
                        .map_err(|e, act, _| eprintln!("fail to add peer {:?}: {}", act.peer_id.unwrap(), e))
                        .and_then(|_, act: &mut TaskWorker, _| {
                            blender::blender_deployment_spec(
                                act.hub_session.peer(act.peer_id.unwrap()),
                                true,
                            )
                                .into_actor(act)
                                .map_err(|e, act, _| {
                                    eprintln!("unable to create deployment @ peer: {:?}, err: {}", act.peer_id.unwrap(), e);
                                    // TODO: try to re-deploy or use another peer instead
//                                let _cancel = act.api
//                                    .cancel_subtask(&act.node_id, act.subtask_id.as_ref().unwrap())
//                                    .into_actor(act)
//                                    .and_then(|_, act, _| fut::ok(eprintln!("subtask {:?} cancelled", act.peer_id.unwrap())))
//                                    .map_err(|e, act, _| {
//                                        eprintln!("fail to cancel subtask {}: {}", act.subtask_id.as_ref().unwrap(), e);
//                                        gu_client::error::Error::Other(e.to_string())
//                                    });
//                                ()
                                })
                                .and_then(
                                    |deployment, act: &mut TaskWorker, _| {
                                        act.deployment = Some(deployment);
                                        fut::ok(())
                                    },
                                )
                        })
                });

        ctx.wait(join_act_fut(ack_task, create_deployment).and_then(|_, _, _| fut::ok(())))
    }
}
