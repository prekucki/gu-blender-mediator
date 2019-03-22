use actix_web::client;
use actix_web::http;
use actix_web::http::Method;
use actix_web::http::Uri;
use bytes::Bytes;
use failure::*;
use futures::prelude::*;
use futures::stream::Stream;
use lazy_static::*;

lazy_static! {
    static ref MKCOL: Method = Method::from_bytes(b"MKCOL").unwrap();
}

pub struct DavPath {
    uri: String,
}

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "http status {}", _0)]
    HttpStatus { status: u16, uri: String },
    #[fail(display = "{}", _0)]
    SendRequest(client::SendRequestError),
    #[fail(display = "{}", _0)]
    HttpError(actix_web::Error),
}

macro_rules! err_convert {
    ($id:ident ($from:ty) ) => {
        impl From<$from> for Error {
            fn from(e: $from) -> Self {
                Error::$id(e)
            }
        }
    };
}

err_convert!(SendRequest(client::SendRequestError));
err_convert!(HttpError(actix_web::Error));

impl DavPath {
    pub fn new(uri: Uri) -> DavPath {
        DavPath {
            uri: uri.to_string(),
        }
    }

    pub fn to_string(&self) -> String {
        self.uri.clone()
    }

    pub fn mkdir(&self, dir_name: &str) -> impl Future<Item = DavPath, Error = Error> {
        let new_uri = if self.uri.ends_with("/") {
            format!("{}{}", self.uri, dir_name)
        } else {
            format!("{}/{}", self.uri, dir_name)
        };

        client::ClientRequest::build()
            .method(MKCOL.clone())
            .uri(&new_uri)
            .finish()
            .into_future()
            .from_err()
            .and_then(|r| r.send().from_err())
            .and_then(move |r| match r.status() {
                http::StatusCode::CREATED => Ok(DavPath { uri: new_uri }),
                status => Err(Error::HttpStatus {
                    status: status.as_u16(),
                    uri: new_uri,
                }),
            })
    }

    pub fn upload<E: Into<actix_web::Error>, S: Stream<Item = Bytes, Error = E> + 'static>(
        &self,
        file_name: &str,
        bytes: S,
    ) -> impl Future<Item = DavPath, Error = Error> {
        let new_uri = if self.uri.ends_with("/") {
            format!("{}{}", self.uri, file_name)
        } else {
            format!("{}/{}", self.uri, file_name)
        };

        client::put(&new_uri)
            .streaming(bytes)
            .into_future()
            .from_err()
            .and_then(|r| r.send().from_err())
            .and_then(|r| {
                if r.status().is_success() {
                    Ok(DavPath { uri: new_uri })
                } else {
                    Err(Error::HttpStatus{ status: r.status().as_u16(), uri: new_uri})
                }
            })
    }
}
