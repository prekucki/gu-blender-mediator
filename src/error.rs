use failure::*;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "{}", _0)]
    Other(String),

    #[fail(display = "mailbox error {}", _0)]
    MailboxError(actix::MailboxError),

    #[fail(display = "{}", _0)]
    JsonErr(#[cause] serde_json::error::Error),
}

pub fn other(msg: &str) -> Error {
    Error::Other(msg.into())
}

impl From<actix::MailboxError> for Error {
    fn from(e: actix::MailboxError) -> Self {
        Error::MailboxError(e)
    }
}


impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::JsonErr(e)
    }
}