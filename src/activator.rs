use actix::prelude::*;
use std::sync::RwLock;
use std::collections::HashMap;
use gu_client::r#async::HubConnection;
use crate::gateway::Gateway;
use futures::Future;
use crate::error::Error;

pub struct Activator {
    gateways : RwLock<HashMap<u64, Addr<Gateway>>>,
    hub_connection : HubConnection,
}

impl Activator {

    pub fn session_gateway(&self, session_id : u64) -> Option<Addr<Gateway>> {
        let result = {
            self.gateways.read().unwrap().get(&session_id).map(|addr| addr.clone())
        };

        if let Some(addr) = result {
            if addr.connected() {
                Some(addr)
            }
            else {
                let mut w = self.gateways.write().unwrap();
                let prev = match w.remove(&session_id) {
                    Some(prev) => prev,
                    None => return None
                };
                if prev.connected() {
                    w.insert(session_id, prev.clone());
                    Some(prev)
                }
                else {
                    None
                }
            }
        }
        else {
            result
        }
    }

    pub fn active_sessions(&self) -> Vec<u64> {
        self.gateways.read().unwrap().keys().cloned().collect()
    }

    pub fn activate_gateway(&self, session_id : u64) -> impl Future<Item=Addr<Gateway>, Error=Error> {
        self.hub_connection.hub_session(session_id).config().map_err(|e| Error::Other(format!("{}", e))).and_then(|m| {
            //serde_json::from_value(serde_json::to_value(m.entry)?).map_err(Error::from)
            Ok(unimplemented!())
        })
    }

}