use std::{
    error::Error as StdError,
    fmt::{self, Display},
    io,
};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    FileAccess(String),
    WrongObjectType(String),
    OutOfMemory,
}

impl From<io::Error> for Error {
    fn from(ioe: io::Error) -> Self {
        Error::Io(ioe)
    }
}

impl StdError for Error {
    fn description(&self) -> &str {
        ""
    }
}

impl Display for Error {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        Ok(())
    }
}
