use sled::{Db, Tree};

use crate::error;
use crate::KvsError;

use super::KvsEngine;

/// Wrapper of `sled::Db`
#[derive(Clone)]
pub struct SledKvsEngine(Db);

impl SledKvsEngine {
    /// Creates a `SledKvsEngine` from `sled::Db`.
    pub fn new(db: Db) -> Self {
        Self(db)
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&self, key: impl Into<String>, value: impl Into<String>) -> error::Result<()> {
        let tree: &Tree = &self.0;
        tree.set(key.into(), value.into().into_bytes())
            .map(|_| ())?;
        tree.flush()?;
        Ok(())
    }

    fn get(&self, key: impl Into<String>) -> error::Result<Option<String>> {
        let tree: &Tree = &self.0;
        Ok(tree
            .get(key.into())?
            .map(|i_vec| AsRef::<[u8]>::as_ref(&i_vec).to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }

    fn remove(&self, key: impl Into<String>) -> error::Result<()> {
        let tree: &Tree = &self.0;
        tree.del(key.into())?.ok_or(KvsError::KeyNotFound)?;
        tree.flush()?;
        Ok(())
    }
}
