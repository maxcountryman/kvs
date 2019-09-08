#![deny(clippy::all, missing_docs)]
//! A key-value store.

#[macro_use]
extern crate failure_derive;

pub use entry::Entry;
pub use error::{KvsError, Result};
pub use kv::KvStore;

mod entry;
mod error;
mod kv;
