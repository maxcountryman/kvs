use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpStream, ToSocketAddrs};

use crate::error;
use crate::response;

/// Key-value store client.
pub struct KvsClient {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    /// Connects to the given `addr`, returning a new `KvsClient`.
    pub fn connect(addr: impl ToSocketAddrs) -> error::Result<Self> {
        let tcp_reader = TcpStream::connect(addr)?;

        let writer = BufWriter::new(tcp_reader.try_clone()?);
        let reader = BufReader::new(tcp_reader);

        Ok(Self { reader, writer })
    }

    /// Sets a key to a value via the server.
    pub fn set(mut self, key: String, value: String) -> error::Result<()> {
        self.writer
            .write_all(format!("+\r\n{}\r\n{}\r\n", key, value).as_bytes())?;
        self.writer.flush()?;
        response::from_reader(&mut self.reader)?;

        Ok(())
    }

    /// Gets a key via the server.
    pub fn get(mut self, key: String) -> error::Result<Option<String>> {
        self.writer
            .write_all(format!("?\r\n{}\r\n", key).as_bytes())?;
        self.writer.flush()?;

        response::from_reader(&mut self.reader)
    }

    /// Removes a key via the server.
    pub fn remove(mut self, key: String) -> error::Result<()> {
        self.writer
            .write_all(format!("-\r\n{}\r\n", key).as_bytes())?;
        self.writer.flush()?;
        response::from_reader(&mut self.reader)?;

        Ok(())
    }
}
