#![deny(missing_docs)]
//! A key-value store.

#[macro_use]
extern crate failure_derive;

pub use error::{KvsError, Result};
pub use kv::KvStore;

mod error;
mod kv;
