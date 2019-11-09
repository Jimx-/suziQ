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
    DataCorrupted(String),
    ProgramLimitExceed(String),
    InvalidState(String),
    InvalidArgument(String),
    OutOfMemory,
}

impl From<io::Error> for Error {
    fn from(ioe: io::Error) -> Self {
        Error::Io(ioe)
    }
}

impl StdError for Error {
    fn description(&self) -> &str {
        use self::Error::*;

        match *self {
            FileAccess(ref e)
            | WrongObjectType(ref e)
            | DataCorrupted(ref e)
            | ProgramLimitExceed(ref e)
            | InvalidState(ref e)
            | InvalidArgument(ref e) => &*e,
            Io(ref e) => e.description(),
            OutOfMemory => "out of memory",
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        use self::Error::*;

        match *self {
            FileAccess(ref e) => write!(f, "File access error: {}", e),
            WrongObjectType(ref e) => write!(f, "Wrong objet type: {}", e),
            DataCorrupted(ref e) => write!(f, "Data corrupted: {}", e),
            ProgramLimitExceed(ref e) => write!(f, "Program limit exceed: {}", e),
            InvalidState(ref e) => write!(f, "Invalid state: {}", e),
            InvalidArgument(ref e) => write!(f, "Invalid argument: {}", e),
            Io(ref e) => write!(f, "IO error: {}", e),
            OutOfMemory => write!(f, "Out of memory."),
        }
    }
}
