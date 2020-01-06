use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::sync::RwLock;

use uuid::Uuid;
use vm_util::collections::HashMap;

use crate::stora::meta::BlockMeta;
use crate::stora::volume::Volume;

lazy_static! {
    pub static ref DISK: RwLock<Disk> = RwLock::new(Disk::new());
}

pub fn init_volumes(volumes: Vec<Volume>) {
    DISK.write().unwrap().init_volumes(volumes);
}

#[derive(Debug)]
pub struct Disk {
    pub volumes: Vec<Volume>,
    volumes_mapping: HashMap<String, usize>,
}

impl Disk {
    pub fn new() -> Disk {
        Disk {
            volumes: vec![],
            volumes_mapping: HashMap::new(),
        }
    }

    pub fn init_volumes(&mut self, volumes: Vec<Volume>) {
        self.volumes = volumes;
        for (idx, v) in self.volumes.iter().enumerate() {
            self.volumes_mapping.insert(v.id.to_owned(), idx);
        }
    }

    pub fn get_write_slot(&mut self) -> Result<WriteSlot, ()> {
        if self.volumes.len() == 0 {
            return Err(());
        }
        let mut min = self.volumes[0].cnt_objects + self.volumes[0].active_slots;
        for v in &self.volumes {
            min = std::cmp::min(min, v.cnt_objects + v.active_slots)
        }
        for v in &mut self.volumes {
            if v.cnt_objects + v.active_slots <= min {
                v.active_slots += 1;
                let buckets = &mut v.buckets;
                if buckets.len() == 0 {
                    return Err(());
                }
                let mut bmin = buckets[0].cnt_blocks + buckets[0].active_slots;
                for b in buckets.iter() {
                    bmin = std::cmp::min(bmin, b.cnt_blocks + b.active_slots)
                }
                for b in buckets.iter_mut() {
                    if b.cnt_blocks + b.active_slots <= bmin {
                        b.active_slots += 1;
                        return Ok(WriteSlot {
                            volume_id: v.id.clone(),
                            bucket_id: b.id.clone(),
                            file_path: format!("{}/{}", b.path, Uuid::new_v4().to_simple()),
                        });
                    }
                }
            }
        }
        Err(())
    }

    pub fn release_write_slot(&mut self, slot: WriteSlot, written_bytes: u64) -> Result<bool, ()> {
        let vi = self.volumes_mapping.get(&slot.volume_id).unwrap().to_owned();
        let v = self.volumes.get_mut(vi).unwrap();
        v.active_slots -= 1;

        let bi = v.buckets_mapping.get(&slot.bucket_id).unwrap().to_owned();
        let b = v.buckets.get_mut(bi).unwrap();
        b.active_slots -= 1;

        if written_bytes > 0 {
            v.cnt_objects += 1;
            b.cnt_blocks += 1;
            b.avail_size_bytes -= written_bytes;
        }

        Ok(true)
    }

    pub fn delete_object(&mut self, volume_id: &String, bucket_id: u32, deleted_bytes: u64) -> Result<(), ()> {
        let vi = self.volumes_mapping.get(volume_id).unwrap().to_owned();
        let v = self.volumes.get_mut(vi).unwrap();
        v.cnt_objects -= 1;

        let bi = v.buckets_mapping.get(&bucket_id).unwrap().to_owned();
        let b = v.buckets.get_mut(bi).unwrap();
        b.cnt_blocks -= 1;
        b.gc_size_bytes += deleted_bytes;

        Ok(())
    }

    pub fn purge_object(&mut self, volume_id: &String, bucket_id: u32, deleted_bytes: u64) -> Result<(), ()> {
        let vi = self.volumes_mapping.get(volume_id).unwrap().to_owned();
        let v = self.volumes.get_mut(vi).unwrap();

        let bi = v.buckets_mapping.get(&bucket_id).unwrap().to_owned();
        let b = v.buckets.get_mut(bi).unwrap();
        b.gc_size_bytes -= deleted_bytes;
        b.avail_size_bytes += deleted_bytes;

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct WriteSlot {
    pub volume_id: String,
    pub bucket_id: u32,
    pub file_path: String,
}

impl WriteSlot {
    pub fn store(self, payload: Vec<u8>) -> Result<String, String> {
        let path = Path::new(&self.file_path);
        match File::create(&path) {
            Err(why) => Err(why.to_string()),
            Ok(file) => {
                let mut file = file;
                match file.write_all(payload.as_slice()) {
                    Err(why) => Err(why.to_string()),
                    Ok(_) => Ok(self.file_path),
                }
            }
        }
    }
    pub fn release(self, written_bytes: u64) {
        if let Err(_) = DISK.write().unwrap().release_write_slot(self, written_bytes) {
            error!("can't release write slot");
        }
    }

    pub fn commit(self, block_meta: BlockMeta) -> Result<(), ()> {
        let written_bytes = block_meta.size;
        match block_meta.store() {
            Ok(_) => {
                self.release(written_bytes);
            }
            Err(_) => {
                error!("can't store block meta");
                self.release(0);
            }
        };
        Ok(())
    }
}

pub fn read_block(path: &String) -> Result<Vec<u8>, String> {
    let path = Path::new(path);
    match File::open(&path) {
        Err(why) => Err(why.to_string()),
        Ok(file) => {
            let mut file = file;
            let mut payload = Vec::new();
            match file.read_to_end(&mut payload) {
                Err(why) => Err(why.to_string()),
                Ok(_) => Ok(payload),
            }
        }
    }
}

pub fn mark_block_as_deleted(meta: BlockMeta) -> Result<(), ()> {
    let volume_id = meta.volume_id.to_owned();
    let bucket_id = meta.bucket_id.to_owned();
    let object_size = meta.size.to_owned();
    if let Err(_) = meta.delete() {
        error!("can't mark block as deleted");
        return Err(());
    }
    if let Err(_) = DISK.write().unwrap().delete_object(&volume_id, bucket_id, object_size) {
        error!("can't delete object");
        return Err(());
    }
    Ok(())
}

pub fn purge_block(meta: BlockMeta) -> Result<(), ()> {
    let volume_id = meta.volume_id.to_owned();
    let bucket_id = meta.bucket_id.to_owned();
    let object_size = meta.size.to_owned();
    match std::fs::remove_file(&meta.path) {
        Ok(_) => {
            if let Err(_) = meta.purge() {
                error!("can't mark block as deleted");
                return Err(());
            }
            if let Err(_) = DISK.write().unwrap().purge_object(&volume_id, bucket_id, object_size) {
                error!("can't purge object");
                return Err(());
            }
            Ok(())
        }
        Err(e) => {
            error!("can't delete file: {}", e);
            Err(())
        }
    }
}