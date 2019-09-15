use std::collections::{BTreeMap, HashMap};
use std::convert::TryInto;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

use crate::entry::{self, Entry};
use crate::error;
use crate::KvsError;

use super::KvsEngine;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

type Generation = u64;
type Readers = HashMap<Generation, BufReaderWithPos<File>>;
type KeyDir = BTreeMap<String, EntryPos>;

/// A key-value store which is backed by write-ahead logging.
pub struct KvStore {
    log_dir: PathBuf,
    readers: Readers,
    writer: BufWriterWithPos<File>,
    keydir: KeyDir,
    current_gen: Generation,
    uncompacted: u64,
}

impl KvStore {
    /// Creates a new key-value store.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    /// use kvs::{KvStore, KvsEngine};
    ///
    /// let mut store = KvStore::open(Path::new("./")).unwrap();
    /// store.set("foo", "bar");
    /// ```
    pub fn open(log_dir: impl Into<PathBuf>) -> error::Result<Self> {
        let mut log_dir = log_dir.into();
        log_dir.push(".kvsdata/");

        fs::create_dir_all(&log_dir)?;

        let mut keydir = BTreeMap::new();
        let mut readers = HashMap::new();

        let gen_list = sorted_gen_list(&log_dir)?;
        let mut uncompacted = 0;

        for &gen in &gen_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&log_dir, gen))?)?;
            uncompacted += load(gen, &mut reader, &mut keydir)?;
            readers.insert(gen, reader);
        }

        let current_gen = gen_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&log_dir, current_gen, &mut readers)?;

        Ok(Self {
            log_dir,
            readers,
            writer,
            keydir,
            current_gen,
            uncompacted,
        })
    }

    /// Compacts write-ahead log.
    fn compact(&mut self) -> error::Result<()> {
        let compaction_gen = self.current_gen + 1;
        self.current_gen += 2;

        self.writer = self.new_log_file(self.current_gen)?;

        let mut compaction_writer = self.new_log_file(compaction_gen)?;

        let mut new_pos = 0;
        for entry_pos in self.keydir.values_mut() {
            let reader = self
                .readers
                .get_mut(&entry_pos.gen)
                .expect("Cannot find log reader");
            if reader.pos != entry_pos.pos {
                reader.seek(SeekFrom::Start(entry_pos.pos))?;
            }

            let mut entry_reader = reader.take(entry_pos.len);
            let len = io::copy(&mut entry_reader, &mut compaction_writer)?;
            *entry_pos = (compaction_gen, new_pos..new_pos + len).into();
            new_pos += len;
        }
        compaction_writer.flush()?;

        let stale_gens: Vec<_> = self
            .readers
            .keys()
            .filter(|&&gen| gen < compaction_gen)
            .cloned()
            .collect();

        for stale_gen in stale_gens {
            self.readers.remove(&stale_gen);
            fs::remove_file(log_path(&self.log_dir, stale_gen))?;
        }

        self.uncompacted = 0;

        Ok(())
    }

    fn new_log_file(&mut self, gen: Generation) -> error::Result<BufWriterWithPos<File>> {
        new_log_file(&self.log_dir, gen, &mut self.readers)
    }
}

impl KvsEngine for KvStore {
    /// Sets a key-value pair in the store.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    /// use kvs::{KvStore, KvsEngine};
    ///
    /// let mut store = KvStore::open(Path::new("./")).unwrap();
    /// store.set("foo", "bar").unwrap();
    ///
    /// let value = store.get("foo").unwrap();
    /// assert_eq!(value, Some(String::from("bar")));
    /// ```
    fn set(&mut self, key: impl Into<String>, value: impl Into<String>) -> error::Result<()> {
        let key = key.into();
        let value = value.into();

        let entry = Entry::set(key.clone(), value);
        let pos = self.writer.pos;
        entry::to_writer(&mut self.writer, &entry)?;
        self.writer.flush()?;
        if let Some(old_entry) = self
            .keydir
            .insert(key, (self.current_gen, pos..self.writer.pos).into())
        {
            self.uncompacted += old_entry.len;
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    /// Returns the value corresponding to the key. If the key doesn't exist,
    /// the returns `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    /// use kvs::{KvStore, KvsEngine};
    ///
    /// let mut store = KvStore::open(Path::new("./")).unwrap();
    /// store.set("foo", "bar").unwrap();
    ///
    /// let value = store.get("foo").unwrap();
    /// assert_eq!(value, Some(String::from("bar")));
    ///
    /// let value = store.get("baz").unwrap();
    /// assert_eq!(value, None);
    /// ```
    fn get(&mut self, key: impl Into<String>) -> error::Result<Option<String>> {
        let key = key.into();
        if let Some(entry_pos) = self.keydir.get(&key) {
            let reader = self
                .readers
                .get_mut(&entry_pos.gen)
                .expect("Cannot find log reader");
            reader.seek(SeekFrom::Start(entry_pos.pos))?;
            let mut entry_reader = reader.take(entry_pos.len);
            let entry = entry::from_reader(&mut entry_reader)?;
            Ok(entry.value)
        } else {
            Ok(None)
        }
    }

    /// Removes a key from the store.
    ///
    /// # Errors
    ///
    /// Trying to remove a nonexistent will result in a
    /// [`KvsError::KeyNotFound`] error.
    ///
    /// [`KvsError::KeyNotFound`]: enum.KvsError.html#variant.KeyNotFound
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    /// use kvs::{KvStore, KvsEngine};
    ///
    /// let mut store = KvStore::open(Path::new("./")).unwrap();
    /// store.set("foo", "bar").unwrap();
    /// store.remove("foo").unwrap();
    ///
    /// let value = store.get("foo").unwrap();
    /// assert_eq!(value, None);
    /// ```
    fn remove(&mut self, key: impl Into<String>) -> error::Result<()> {
        let key = key.into();
        if self.keydir.contains_key(&key) {
            let entry = Entry::remove(key);
            entry::to_writer(&mut self.writer, &entry)?;
            self.writer.flush()?;

            if let Entry {
                key, value: None, ..
            } = entry
            {
                let old_entry = self.keydir.remove(&key).expect("Key not found in keydir");
                self.uncompacted += old_entry.len;
            }

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}

fn new_log_file(
    log_dir: &Path,
    gen: Generation,
    readers: &mut Readers,
) -> error::Result<BufWriterWithPos<File>> {
    let path = log_path(&log_dir, gen);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
    readers.insert(gen, BufReaderWithPos::new(File::open(&path)?)?);
    Ok(writer)
}

fn sorted_gen_list(log_dir: &Path) -> error::Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&log_dir)?
        .flat_map(|res| -> error::Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    gen_list.sort_unstable();
    Ok(gen_list)
}

fn load(
    gen: Generation,
    reader: &mut BufReaderWithPos<File>,
    keydir: &mut KeyDir,
) -> error::Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut new_pos = pos;
    let mut uncompacted = 0;

    while let Some(_) = reader.bytes().next() {
        pos = reader.seek(SeekFrom::Start(pos))?;

        let mut prefix_bytes = [0; entry::PREFIX_SIZE];
        let mut prefix_reader = reader.take(entry::PREFIX_SIZE as u64);
        prefix_reader.read_exact(&mut prefix_bytes)?;
        new_pos += entry::PREFIX_SIZE as u64;

        let key_size = u64::from(u32::from_ne_bytes(prefix_bytes[4..8].try_into()?));
        let value_size = u64::from(u32::from_ne_bytes(
            prefix_bytes[8..entry::PREFIX_SIZE].try_into()?,
        ));

        reader.seek(SeekFrom::Start(pos))?;
        let mut entry_reader = reader.take(entry::PREFIX_SIZE as u64 + key_size + value_size);
        let entry = entry::from_reader(&mut entry_reader)?;
        new_pos += key_size + value_size;

        match entry {
            Entry {
                key,
                value: Some(_),
                ..
            } => {
                if let Some(old_entry) = keydir.insert(key, (gen, pos..new_pos).into()) {
                    uncompacted += old_entry.len;
                }
            }
            Entry {
                key, value: None, ..
            } => {
                if let Some(old_entry) = keydir.remove(&key) {
                    uncompacted += old_entry.len;
                }

                uncompacted += new_pos - pos;
            }
        }

        reader.seek(SeekFrom::Start(new_pos))?;
        pos = new_pos;
    }

    Ok(uncompacted)
}

fn log_path(log_dir: &Path, gen: Generation) -> PathBuf {
    log_dir.join(format!("{}.log", gen))
}

struct EntryPos {
    gen: Generation,
    pos: u64,
    len: u64,
}

impl From<(Generation, Range<u64>)> for EntryPos {
    fn from((gen, range): (Generation, Range<u64>)) -> Self {
        Self {
            gen,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

#[derive(Debug)]
struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> error::Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

#[derive(Debug)]
struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> error::Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
