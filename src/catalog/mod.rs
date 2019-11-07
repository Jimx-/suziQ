mod catalog_cache;

pub use self::catalog_cache::CatalogCache;

#[derive(Default)]
pub struct Schema {}

impl Schema {
    pub fn new() -> Self {
        Self {}
    }
}
