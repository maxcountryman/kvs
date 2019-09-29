use crate::error;
use crate::thread_pool::ThreadPool;

/// Shared queue thread pool.
pub struct SharedQueueThreadPool {}

impl ThreadPool for SharedQueueThreadPool {
    fn new(_: u32) -> error::Result<Self> {
        Ok(Self {})
    }

    fn spawn<F>(&self, _: F)
    where
        F: FnOnce() + Send + 'static,
    {
    }
}
