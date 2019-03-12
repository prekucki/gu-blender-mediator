use futures::{future, prelude::*};
use gu_client::model::envman::{CreateSession, Image};
use gu_client::r#async::{Peer, PeerSession};
use serde_derive::*;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BlenderTaskSpec {
    crops: Vec<Crop>,
    samples: u32,
    resolution: (u32, u32),
    frames: Vec<u32>,
    scene_file: Option<String>,
    output_format: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Crop {
    borders_x: (f64, f64),
    borders_y: (f64, f64),
    outfilebasename: String,
}

impl BlenderTaskSpec {
    pub fn normalize_path(&mut self) {
        self.scene_file = self.scene_file.take().map(|f| {
            if f.starts_with("/golem/resources") {
                f[("/golem/resources".len() + 1)..].to_owned()
            } else {
                f
            }
        });
    }

    pub fn expected_output_file_name(&self) -> String {
        self.frames
            .iter()
            .map(|&frame| {
                self.crops
                    .iter()
                    .map(move |c| format!("{}{:04}.png", c.outfilebasename, frame))
            })
            .flatten()
            .next()
            .unwrap()
    }
}

pub fn blender_deployment_spec(
    peer: Peer,
    docker: bool,
) -> impl Future<Item = PeerSession, Error = gu_client::error::Error> + 'static {
    if !docker {
        future::Either::A(peer.new_session(CreateSession {
            env_type: "hd".to_string(),
            image: Image {
                url: "http://52.31.143.91/images/x86_64/linux/gu-blender.hdi".to_string(),
                hash: "SHA1:213fad4e020ded42e6a949f61cb660cb69bc9845".to_string(),
            },
            name: "".to_string(),
            tags: vec!["gu:render".into(), "gu:blender".into()],
            note: None,
            options: (),
        }))
    } else {
        use gu_client::model::dockerman::*;

        future::Either::B(
            peer.new_session(CreateSession::<CreateOptions> {
                env_type: "docker".to_string(),
                image: Image {
                    url: "prekucki/gu-render-blender".to_string(),
                    hash: "sha256:53d11e6866835986b625e9fb07aa73b31dc667da39fe04f56da0ef06a50e0083"
                        .to_string(),
                },
                name: "".to_string(),
                tags: vec!["gu:render".into(), "gu:blender".into()],
                note: None,
                options: CreateOptions {
                    volumes: vec![
                        VolumeDef::BindRw {
                            src: "resources".into(),
                            target: "/golem/resources".into(),
                        },
                        VolumeDef::BindRw {
                            src: "output".into(),
                            target: "/golem/output".into(),
                        },
                    ],
                    cmd: None,
                },
            }),
        )
    }
}
