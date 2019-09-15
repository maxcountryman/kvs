use std::io::BufRead;

use crate::error;
use crate::KvsError;

#[derive(Debug)]
pub enum Request {
    Get { key: String },
    Set { key: String, value: String },
    Remove { key: String },
}

impl Request {
    pub fn from_reader(reader: &mut dyn BufRead) -> error::Result<Request> {
        match reader.lines().next() {
            Some(req) => {
                let req = req?;
                match req.as_str() {
                    "?" => match reader.lines().next() {
                        Some(key) => Ok(Request::Get { key: key? }),
                        None => Err(KvsError::String(String::from("Malformed get request"))),
                    },
                    "+" => {
                        let key = reader.lines().next();
                        let value = reader.lines().next();
                        match (key, value) {
                            (Some(key), Some(value)) => Ok(Request::Set {
                                key: key?,
                                value: value?,
                            }),
                            _ => Err(KvsError::String(String::from("Malformed set request"))),
                        }
                    }
                    "-" => match reader.lines().next() {
                        Some(key) => Ok(Request::Remove { key: key? }),
                        None => Err(KvsError::String(String::from("Malformed remove request"))),
                    },
                    _ => Err(KvsError::String(String::from("Illegal server command"))),
                }
            }
            None => Err(KvsError::String(String::from("Malformed request"))),
        }
    }
}
