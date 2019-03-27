use actix_web::{client, http::Method, HttpMessage};
use futures::future::*;
use log::error;
use serde::Deserialize;
use serde_derive::*;

#[derive(Serialize)]
enum Command {
    #[serde(rename_all = "camelCase")]
    RegisterCommand { cmd_name: String, url: String },
}

pub fn register_server(url: &str) -> impl Future<Item = (), Error = ()> {
    let command = Command::RegisterCommand {
        cmd_name: "gu-blender-mediator".into(),
        url: url.into(),
    };

    client::ClientRequest::build()
        .method(Method::PATCH)
        .uri("http://127.0.0.1:61622/service/local")
        .json(command)
        .into_future()
        .map_err(|e| eprintln!("hub connection error: {}", e))
        .and_then(|r| {
            r.send()
                .map_err(|e| eprintln!("hub connection error: {}", e))
        })
        .and_then(|r| {
            r.json()
                .map_err(|e| eprintln!("hub connection error: {}", e))
        })
        .and_then(|v: serde_json::Value| Ok(log::info!("registed service [{}]", v.to_string())))
}
