use std::fs;
use std::process;

use crate::stora::meta::BucketMeta;

#[derive(Debug, Clone)]
pub struct Bucket {
    pub id: u32,
    pub volume_id: String,
    pub path: String,
    pub cnt_blocks: u64,
    pub active_slots: u64,
    pub initial_size_bytes: u64,
    pub avail_size_bytes: u64,
    pub gc_size_bytes: u64,
}

impl Bucket {
    pub fn new(id: u32, volume_id: &String, bucket_path: &String, initial_size_bytes: u64) -> Bucket {
        Bucket {
            id: id,
            volume_id: volume_id.to_owned(),
            path: bucket_path.to_owned(),
            cnt_blocks: 0,
            active_slots: 0,
            initial_size_bytes: initial_size_bytes,
            avail_size_bytes: initial_size_bytes,
            gc_size_bytes: 0,
        }
    }

    pub fn bootstrap(&mut self) -> Result<bool, String> {
        if !fs::metadata(&self.path).is_ok() {
            fs::create_dir_all(&self.path).expect("can't create bucket directory");
        }
        if let Ok(Some(bucket_meta)) = BucketMeta::get(BucketMeta::db_id(self.id, &self.volume_id)) {
            self.cnt_blocks = bucket_meta.cnt_blocks;
            self.avail_size_bytes = bucket_meta.avail_size_bytes;
            self.initial_size_bytes = bucket_meta.init_size_bytes;
            self.active_slots = bucket_meta.active_slots;
            self.gc_size_bytes = bucket_meta.gc_size_bytes;
        } else {
            let mut bm = BucketMeta::new();
            bm.init_size_bytes = self.initial_size_bytes;
            bm.avail_size_bytes = self.avail_size_bytes;
            if let Err(_) = bm.upsert(self.id, &self.volume_id) {
                error!("can't upsert bucket meta");
                process::exit(1)
            }
        }
        Ok(true)
    }
}