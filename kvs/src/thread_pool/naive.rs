use std::thread;

use crate::error;
use crate::thread_pool::ThreadPool;

/// Naive thread pool.
pub struct NaiveThreadPool {}

impl ThreadPool for NaiveThreadPool {
    fn new(_: u32) -> error::Result<Self> {
        Ok(Self {})
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let child = thread::spawn(move || job());
        child.join();
    }
}
