use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};

use crate::common::exception::Exception;

pub struct Channel<T> {
    queue: Mutex<VecDeque<T>>,
    condvar: Condvar,
}

impl<T> Channel<T> {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
        }
    }
    pub fn put(&self, element: T) -> Result<(), Exception> {
        let mut queue = self.queue.lock()?;
        queue.push_back(element);
        self.condvar.notify_all();
        Ok(())
    }
    pub fn get(&self) -> Result<T, Exception> {
        let mut queue = self.queue.lock().unwrap();
        while queue.is_empty() {
            queue = self.condvar.wait(queue).unwrap();
        }
        Ok(queue.pop_front().unwrap())
    }
}

impl<T> Default for Channel<T> {
    fn default() -> Self {
        Self::new()
    }
}
