mod segment;
mod wal;

pub use self::wal::{Wal, WalConfig};

pub type LogPointer = u64;
