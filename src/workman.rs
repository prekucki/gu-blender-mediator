use actix::prelude::*;
use actix::Context;
use failure::*;
use futures::prelude::*;
use gu_client::{r#async::HubConnection, NodeId};
use rand::prelude::*;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
        eprintln!("{:?} reserved until {:?}", task_id, reserved_until);

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

    fn handle(&mut self, msg: GiveMeNode, ctx: &mut Self::Context) -> Self::Result {
        ActorResponse::r#async(
            self.connection
                .list_peers()
                .into_actor(self)
                .map_err(|_, _act, _ctx| NoFreeNode)
                .and_then(move |peers, act, ctx| {
                    let c: Vec<NodeId> = peers
                        .map(|p| p.node_id)
                        .filter(|&p| act.is_free_to_use(p))
                        .collect();

                    use rand::seq::SliceRandom;
                    let mut rng = thread_rng();

                    if let Some(&it) = c.choose(&mut rng) {
                        act.reservations
                            .insert(it.clone(), Reservation::new(msg.task_id, msg.deadline));
                        //fut::ok(it)
                        fut::ok(
                            // reqc desktop: "0xf6140a03926b0801cd891d2d128ebd8dffbda252"
                            // 2rec mac "0xc61f511ac893743475962776b01c7e65c309ced6"
                            // awokado
                            "0x5512cfe2e1a7f8feb9826eeeb327bd3dc7e0ffaf"
                                .parse()
                                .unwrap(),
                        )
                    } else {
                        fut::err(NoFreeNode)
                    }
                }),
        )
    }
}

pub fn reserve(task_id: &str, deadline: u64) -> impl Future<Item = NodeId, Error = NoFreeNode> {
    WorkMan::from_registry()
        .send(GiveMeNode {
            task_id: task_id.to_owned(),
            deadline,
        })
        .then(|r| match r {
            Ok(Ok(v)) => Ok(v),
            Err(e) => {
                eprintln!("reservation error: {}", e);
                Err(NoFreeNode)
            }
            Ok(Err(_)) => Err(NoFreeNode),
        })
}
