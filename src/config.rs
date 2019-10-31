use std::path::{Path, PathBuf};

const DEFAULT_ROOT_PATH: &str = "suziQ";

pub struct DBConfig {
    pub cache_capacity: usize,
    pub root_path: PathBuf,
}

impl Default for DBConfig {
    fn default() -> Self {
        Self {
            cache_capacity: 4096,
            root_path: PathBuf::from(DEFAULT_ROOT_PATH),
        }
    }
}

impl DBConfig {
    pub fn new() -> Self {
        DBConfig::default()
    }

    pub fn root_path<P: AsRef<Path>>(mut self, p: P) -> Self {
        self.root_path = p.as_ref().to_path_buf();
        self
    }

    pub fn cache_capacity(mut self, cache_capacity: usize) -> Self {
        self.cache_capacity = cache_capacity;
        self
    }

    pub fn get_storage_path(&self) -> PathBuf {
        let mut path = self.root_path.clone();
        path.push("base");
        path
    }
}
