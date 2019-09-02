use std::collections::HashMap;
use std::fs::{create_dir, rename, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use ron::de::from_str;
use ron::ser::to_string;
use serde::{Deserialize, Serialize};

use crate::{KvsError, Result};

/// A key-value store which is backed by a write-ahead log.
pub struct KvStore<'a> {
    keydir: HashMap<String, u64>,
    log_file: File,
    log_dir: &'a Path,
}

impl<'a> KvStore<'a> {
    /// Creates a new key-value store.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    /// use kvs::KvStore;
    ///
    /// let mut store = KvStore::open(Path::new("./")).unwrap();
    /// store.set(String::from("foo"), String::from("bar"));
    /// ```
    pub fn open(log_dir: &'a Path) -> Result<Self> {
        if !log_dir.exists() {
            create_dir(log_dir)?;
        }

        let log_file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(log_dir.join(".wal.0"))?;

        let mut keydir = HashMap::new();

        let mut log_reader = BufReader::new(log_file.try_clone()?);
        let mut pos = 0;
        for line in log_reader.by_ref().lines() {
            let line = line?;

            // TODO: Fix unwrap.
            let cmd: Command = from_str(line.as_str()).unwrap();
            match cmd {
                Command::Set(key, ..) => {
                    keydir.insert(key.to_owned(), pos as u64);
                }
                Command::Rm(key) => {
                    keydir.remove(&key);
                }
            };

            // The size of the String in bytes plus the newline.
            pos += line.len() + 1;
        }

        Ok(Self {
            keydir,
            log_dir,
            log_file,
        })
    }

    /// Sets a key-value pair in the store.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    /// use kvs::KvStore;
    ///
    /// let mut store = KvStore::open(Path::new("./")).unwrap();
    /// store.set(String::from("foo"), String::from("bar")).unwrap();
    ///
    /// let value = store.get(String::from("foo")).unwrap();
    /// assert_eq!(value, Some(String::from("bar")));
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = to_string(&Command::Set(key.to_owned(), value))?;

        self.log_file.seek(SeekFrom::End(0))?;
        writeln!(self.log_file, "{}", cmd)?;

        let pos = self.log_file.seek(SeekFrom::Current(0))?;

        let key_pos = pos - (cmd.len() + 1) as u64;
        self.keydir.insert(key, key_pos);

        // Arbitrary compaction.
        if pos >= 100000 {
            self.compact()?;
        }

        Ok(())
    }

    /// Returns the value corresponding to the key. If the key doesn't exist, the returns `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    /// use kvs::KvStore;
    ///
    /// let mut store = KvStore::open(Path::new("./")).unwrap();
    /// store.set(String::from("foo"), String::from("bar")).unwrap();
    ///
    /// let value = store.get(String::from("foo")).unwrap();
    /// assert_eq!(value, Some(String::from("bar")));
    ///
    /// let value = store.get(String::from("baz")).unwrap();
    /// assert_eq!(value, None);
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(pos) = self.keydir.get(&key) {
            let mut line = String::new();

            let mut log_reader = BufReader::new(&mut self.log_file);
            log_reader.seek(SeekFrom::Start(*pos))?;
            log_reader.read_line(&mut line)?;

            // TODO: Fix unwrap.
            let cmd: Command = from_str(line.as_str()).unwrap();
            if let Command::Set(.., value) = cmd {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    /// Removes a key from the store.
    ///
    /// # Errors
    ///
    /// Trying to remove a nonexistent will result in a [`KvsError::KeyNotFound`] error.
    /// 
    /// [`KvsError::KeyNotFound`]: enum.KvsError.html#variant.KeyNotFound
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    /// use kvs::KvStore;
    ///
    /// let mut store = KvStore::open(Path::new("./")).unwrap();
    /// store.set(String::from("foo"), String::from("bar")).unwrap();
    /// store.remove(String::from("foo")).unwrap();
    ///
    /// let value = store.get(String::from("foo")).unwrap();
    /// assert_eq!(value, None);
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.keydir.contains_key(&key) {
            let cmd = to_string(&Command::Rm(key.to_owned()))?;

            self.log_file.seek(SeekFrom::End(0))?;
            writeln!(self.log_file, "{}", cmd)?;

            self.keydir.remove(&key);

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// Compacts write-ahead log.
    ///
    /// This is naive compaction. It works by creating a parallel log, reading all the active keys
    /// in the `keydir`, and then writing those keys to the new log. As this is happening we build
    /// a new key directory. Once complete, the original log is made to point to this new log and
    /// the old `keydir` replaced with the new.
    fn compact(&mut self) -> Result<()> {
        let mut log_file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(self.log_dir.join(".wal.hint"))?;

        let mut keydir: HashMap<String, u64> = HashMap::new();

        for (key, pos) in self.keydir.iter() {
            let mut line = String::new();
            let mut log_reader = BufReader::new(&mut self.log_file);

            log_reader.seek(SeekFrom::Start(*pos))?;
            log_reader.read_line(&mut line)?;

            if let Command::Set(.., value) = from_str(line.as_str()).unwrap() {
                let cmd = to_string(&Command::Set(key.to_owned(), value))?;
                log_file.seek(SeekFrom::End(0))?;
                writeln!(log_file, "{}", cmd)?;

                let pos = log_file.seek(SeekFrom::Current(0))?;
                let key_pos = pos - (cmd.len() + 1) as u64;

                keydir.insert(key.to_owned(), key_pos);
            };
        }

        rename(self.log_dir.join(".wal.hint"), self.log_dir.join(".wal.0"))?;

        self.log_file = OpenOptions::new()
            .write(true)
            .read(true)
            .open(self.log_dir.join(".wal.0"))?;

        self.keydir = keydir;

        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Command {
    Set(String, String),
    Rm(String),
}
