pub use hashbrown::hash_map::Entry as HashMapEntry;
pub use indexmap::IndexMap as OrderMap;
pub use indexmap::map::Entry as OrderMapEntry;

pub type HashMap<K, V> = hashbrown::HashMap<K, V, hashbrown::hash_map::DefaultHashBuilder>;
pub type HashSet<T> = hashbrown::HashSet<T, hashbrown::hash_map::DefaultHashBuilder>;

