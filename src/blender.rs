use std::io;

use failure::Fail;
use futures::{future, prelude::*};
use gu_client::model::envman::{CreateSession, Image};
use gu_client::r#async::{Peer, PeerSession};
use serde_derive::*;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OldBlenderTaskSpec {
    frames: Vec<u32>,
    outfilebasename: String,
    output_format: String,
    scene_file: Option<String>,
    script_src: String,
}

#[derive(Default, Debug)]
struct ScriptData {
    resolution_x: Option<u32>,
    resolution_y: Option<u32>,
    border_max_x: Option<f64>,
    border_min_x: Option<f64>,
    border_min_y: Option<f64>,
    border_max_y: Option<f64>,
    use_compositing: Option<bool>,
}

#[inline]
fn unwrap_val<T>(val: Option<T>, name: &'static str) -> Result<T, ErrorMissingField> {
    match val {
        Some(v) => Ok(v),
        None => Err(ErrorMissingField(name)),
    }
}

macro_rules! data_getter {
    ($name:ident : $t:ty) => {
        #[inline]
        fn $name(&self) -> Result<$t, ErrorMissingField> {
            unwrap_val(self.$name, stringify!($name))
        }
    }
}

impl ScriptData {
    fn update_from_script(&mut self, key: &str, val: &str) -> failure::Fallible<()> {
        match key {
            "resolution_x" => self.resolution_x = Some(val.parse()?),
            "resolution_y" => self.resolution_y = Some(val.parse()?),
            "border_max_x" => self.border_max_x = Some(val.parse()?),
            "border_min_x" => self.border_min_x = Some(val.parse()?),
            "border_max_y" => self.border_max_y = Some(val.parse()?),
            "border_min_y" => self.border_min_y = Some(val.parse()?),
            "use_compositing" => {
                self.use_compositing = Some(match val {
                    "True" | "bool(True)" => true,
                    "False" | "bool(False)" => false,
                    _ => Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("invalid use_compositing='{}'", val),
                    ))?,
                })
            }
            _ => (),
        };
        Ok(())
    }

    data_getter! { resolution_x : u32 }
    data_getter! { resolution_y : u32 }

    data_getter! { border_max_x : f64 }
    data_getter! { border_min_x : f64 }
    data_getter! { border_max_y : f64 }
    data_getter! { border_min_y : f64 }
}

lazy_static::lazy_static! {
        static ref RE: regex::Regex = regex::Regex::new(r"bpy\.context\.scene\.render\.([a-zA-Z0-9_]+)\s*=\s*([^\s]+)").unwrap();
}

#[derive(Debug, Fail)]
#[fail(display = "missing field: {}", _0)]
struct ErrorMissingField(&'static str);

impl OldBlenderTaskSpec {
    fn parse_script(&self) -> failure::Fallible<ScriptData> {
        use std::ops::Index;
        let mut data = ScriptData::default();

        for c in RE.captures_iter(&self.script_src) {
            data.update_from_script(c.index(1), c.index(2))?;
        }

        Ok(data)
    }

    fn into_spec(self) -> failure::Fallible<BlenderSubtaskSpec> {
        let data = self.parse_script()?;

        Ok(BlenderSubtaskSpec {
            samples: 0,
            resolution: (data.resolution_x()?, data.resolution_y()?),
            frames: self.frames,
            scene_file: self.scene_file,
            output_format: self.output_format,
            crops: vec![Crop {
                borders_x: (data.border_min_x()?, data.border_max_x()?),
                borders_y: (data.border_min_y()?, data.border_max_y()?),
                outfilebasename: self.outfilebasename,
            }],
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BlenderSubtaskSpec {
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

impl BlenderSubtaskSpec {
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

impl std::fmt::Display for BlenderSubtaskSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "BlenderTaskSpec (scene: {}, frames: {:?}, res: {:?})",
            self.scene_file.as_ref().unwrap(),
            self.frames,
            self.resolution
        )
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
                    net: None
                },
            }),
        )
    }
}

pub fn decode(extra_data: serde_json::Value) -> Result<BlenderSubtaskSpec, failure::Error> {
    match serde_json::from_value(extra_data.clone()) {
        Ok(v) => return Ok(v),
        _ => (),
    };
    let old_spec: OldBlenderTaskSpec = serde_json::from_value(extra_data)?;

    old_spec.into_spec()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse() {
        let b = OldBlenderTaskSpec {
            frames: vec![],
            outfilebasename: "".to_string(),
            output_format: "".to_string(),
            scene_file: None,
            script_src: r#"

bpy.context.scene.render.tile_x = tile_size
bpy.context.scene.render.tile_y = tile_size
bpy.context.scene.render.resolution_x = 320
bpy.context.scene.render.resolution_y = 240
bpy.context.scene.render.resolution_percentage = 100
bpy.context.scene.render.use_border = True
bpy.context.scene.render.use_crop_to_border = True
bpy.context.scene.render.border_max_x = 1.0
bpy.context.scene.render.border_min_x = 0.0
bpy.context.scene.render.border_min_y = 0.0
bpy.context.scene.render.border_max_y = 1.0
bpy.context.scene.render.use_compositing = bool(False)

            "#
            .to_string(),
        };

        eprintln!("done");
        eprintln!("v={:?}", b.into_spec().unwrap())
    }

}
