use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};

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
    pub fn put(&self, element: T) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(element);
        self.condvar.notify_all();
    }
    pub fn get(&self) -> T {
        let mut queue = self.queue.lock().unwrap();
        while queue.is_empty() {
            queue = self.condvar.wait(queue).unwrap();
        }
        queue.pop_front().unwrap()
    }
}

impl<T> Default for Channel<T> {
    fn default() -> Self {
        Self::new()
    }
}
