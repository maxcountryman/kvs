#![deny(clippy::all, missing_docs)]
//! A key-value store.

#[macro_use]
extern crate failure_derive;

#[macro_use]
extern crate log;

pub use client::KvsClient;
pub use engines::{KvStore, KvsEngine, SledKvsEngine};
pub use entry::{from_reader, Entry};
pub use error::{KvsError, Result};
pub use server::KvsServer;

mod client;
mod engines;
mod entry;
mod request;
mod response;
mod server;

/// Error module.
pub mod error;
