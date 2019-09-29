use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

use crate::error;
use crate::request::Request;
use crate::KvsEngine;

/// A key-value server.
pub struct KvsServer<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> KvsServer<E> {
    /// Create a new server with the given engine.
    pub fn new(engine: E) -> Self {
        Self { engine }
    }

    /// Run the server.
    pub fn run<A: ToSocketAddrs>(mut self, addr: A) -> error::Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(e) = self.serve(stream) {
                        error!("Error on serving client: {}", e);
                    }
                }
                Err(e) => error!("Connection failed: {}", e),
            }
        }
        Ok(())
    }

    fn serve(&mut self, tcp: TcpStream) -> error::Result<()> {
        let peer_addr = tcp.peer_addr()?;
        let mut reader = BufReader::new(&tcp);
        let mut writer = BufWriter::new(&tcp);

        let req = Request::from_reader(&mut reader)?;
        debug!("Received request from {}: {:?}", peer_addr, req);
        match req {
            Request::Get { key } => match self.engine.get(key.clone()) {
                Ok(Some(value)) => writer.write_all(format!("{}\r\n", value).as_bytes())?,
                Ok(None) => writer.write_all(b"-1\r\n")?,
                Err(e) => writer.write_all(format!("!{}\r\n", e).as_bytes())?,
            },
            Request::Set { key, value } => match self.engine.set(key, value) {
                Ok(_) => writer.write_all(b"OK")?,
                Err(e) => writer.write_all(format!("!{}\r\n", e).as_bytes())?,
            },
            Request::Remove { key } => match self.engine.remove(key) {
                Ok(_) => writer.write_all(b"OK")?,
                Err(e) => writer.write_all(format!("!{}\r\n", e).as_bytes())?,
            },
        }

        Ok(())
    }
}
