use crate::stora::volume::Volume;
use std::sync::RwLock;
use vm_util::collections::HashMap;
use uuid::Uuid;
use std::path::Path;
use std::fs::File;
use std::io::prelude::*;
use crate::stora::meta::BlockMeta;

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