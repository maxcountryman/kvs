use std::collections::HashMap;

/// A key-value store.


#[derive(Clone)]
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    /// Creates an empty `KvStore`.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// ```
    pub fn new() -> Self {
        KvStore {
            store: HashMap::new(),
        }
    }

    /// Sets a key-value pair in the store.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    ///
    /// let mut store = KvStore::new();
    /// store.set(String::from("foo"), String::from("bar"));
    /// assert_eq!(store.get(String::from("foo")), Some(String::from("bar")));
    /// ```
    pub fn set(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }

    /// Returns the value corresponding to the key.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    ///
    /// let mut store = KvStore::new();
    /// store.set(String::from("foo"), String::from("bar"));
    /// assert_eq!(store.get(String::from("foo")), Some(String::from("bar")));
    /// ```
    pub fn get(&self, key: String) -> Option<String> {
        if let Some(value) = self.store.get(&key) {
            Some(value.to_owned())
        } else {
            None
        }
    }

    /// Removes a key from the store.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    ///
    /// let mut store = KvStore::new();
    ///
    /// store.set(String::from("foo"), String::from("bar"));
    /// store.remove(String::from("foo"));
    /// assert_eq!(store.get(String::from("foo")), None);
    /// ```
    pub fn remove(&mut self, key: String) {
        self.store.remove(&key);
    }
}
