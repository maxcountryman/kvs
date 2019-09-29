use std::io::BufRead;

use crate::error;
use crate::KvsError;

pub fn from_reader(reader: &mut dyn BufRead) -> error::Result<Option<String>> {
    match reader.lines().next() {
        Some(resp) => {
            let resp = resp?;

            if resp.starts_with('!') {
                Err(KvsError::String(resp))
            // TODO: Handle `None` types more robustly.
            } else if resp == "-1" {
                Ok(None)
            } else {
                Ok(Some(resp))
            }
        }
        None => Err(KvsError::String(String::from("Malformed response"))),
    }
}
