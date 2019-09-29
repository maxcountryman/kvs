use crate::error;
use crate::thread_pool::ThreadPool;

/// Rayon thread pool.
pub struct RayonThreadPool {}

impl ThreadPool for RayonThreadPool {
    fn new(_: u32) -> error::Result<Self> {
        Ok(Self {})
    }

    fn spawn<F>(&self, _: F)
    where
        F: FnOnce() + Send + 'static,
    {
    }
}
