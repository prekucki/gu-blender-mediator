use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use actix::prelude::*;
use actix::Context;
use failure::*;
use futures::prelude::*;
use gu_client::{r#async::HubConnection, NodeId};
use rand::prelude::*;

#[derive(Debug, Fail)]
#[fail(display = "no free node")]
pub struct NoFreeNode;

#[derive(Debug)]
struct Reservation {
    task_id: String,
    reserved_until: SystemTime,
}

impl Reservation {
    fn new(task_id: String, deadline: u64) -> Reservation {
        let reserved_until = UNIX_EPOCH + Duration::from_secs(deadline);

        Reservation {
            task_id,
            reserved_until,
        }
    }

    fn is_valid(&self) -> bool {
        let now = SystemTime::now();

        self.reserved_until >= now
    }
}

pub struct WorkMan {
    connection: HubConnection,
    reservations: HashMap<NodeId, Reservation>,
}

impl Default for WorkMan {
    fn default() -> Self {
        let connection = HubConnection::default();
        let reservations = HashMap::new();
        WorkMan {
            connection,
            reservations,
        }
    }
}

impl Actor for WorkMan {
    type Context = Context<Self>;
}

impl WorkMan {
    fn is_free_to_use(&self, peer_id: NodeId) -> bool {
        self.reservations
            .get(&peer_id)
            .map(|r| !r.is_valid())
            .unwrap_or(true)
    }
}

impl Supervised for WorkMan {}
impl SystemService for WorkMan {}

struct GiveMeNode {
    task_id: String,
    deadline: u64,
}

impl Message for GiveMeNode {
    type Result = Result<NodeId, NoFreeNode>;
}

impl Handler<GiveMeNode> for WorkMan {
    type Result = ActorResponse<Self, NodeId, NoFreeNode>;

    fn handle(&mut self, msg: GiveMeNode, _ctx: &mut Self::Context) -> Self::Result {
        ActorResponse::r#async(
            self.connection
                .list_peers()
                .into_actor(self)
                .map_err(|_, _act, _ctx| NoFreeNode)
                .and_then(move |peers, act, _ctx| {
                    let c: Vec<NodeId> = peers
                        .map(|p| p.node_id)
                        .filter(|&p| act.is_free_to_use(p))
                        .collect();

                    use rand::seq::SliceRandom;
                    let mut rng = thread_rng();

                    if let Some(&it) = c.choose(&mut rng) {
                        act.reservations
                            .insert(it.clone(), Reservation::new(msg.task_id, msg.deadline));
                        fut::ok(it)
                    } else {
                        fut::err(NoFreeNode)
                    }
                }),
        )
    }
}

pub fn reserve(task_id: &str, deadline: u64) -> impl Future<Item = NodeId, Error = NoFreeNode> {
    let task = task_id.to_owned();
    WorkMan::from_registry()
        .send(GiveMeNode {
            task_id: task.clone(),
            deadline,
        })
        .then(move |r| match r {
            Ok(Ok(node_id)) => {
                eprintln!("reserved peer {:?} for subtask {:?} until {:?}", node_id, task, deadline);

                Ok(node_id)
            },
            Err(e) => {
                eprintln!("reservation error: {}", e);
                Err(NoFreeNode)
            }
            Ok(Err(_)) => Err(NoFreeNode),
        })
}
