use std::fs;
use std::io;
use std::os::linux::fs::MetadataExt;
use std::path::Path;
use std::process;

use crypto::digest::Digest;
use crypto::sha1::Sha1;
use crate::stora::bucket::Bucket;
use super::systemstat::Filesystem;
use crate::stora::meta::VolumeMeta;
use std::collections::HashMap;


#[derive(Debug, Clone)]
pub struct Volume {
    pub id: String,
    pub dev: String,
    pub path: String,
    pub mountpoint: String,
    pub buckets: Vec<Bucket>,
    pub buckets_mapping: HashMap<u32, usize>,
    pub cnt_objects: u64,
    pub active_slots: u64,
}

impl Volume {
    pub fn new(volume_path: &String) -> Volume {
        Volume {
            id: "".to_string(),
            dev: "".to_string(),
            path: volume_path.to_owned(),
            mountpoint: "".to_string(),
            buckets: vec![],
            buckets_mapping: HashMap::new(),
            cnt_objects: 0,
            active_slots: 0,
        }
    }

    pub fn bootstrap(&mut self, mounts: &Vec<Filesystem>, bucket_size_limit: u64) -> Result<bool, &str> {
        if !fs::metadata(&self.path).is_ok() {
            fs::create_dir_all(&self.path).expect("can't create volume");
        }
        self.path = match fs::canonicalize(&self.path) {
            Ok(cpath) => {
                cpath.as_path().to_str().unwrap().to_string()
            }
            Err(_x) => {
                "".to_string()
            }
        };

        self.find_mount_point();

        let mut hasher = Sha1::new();
        hasher.input_str(self.path.as_str());
        self.id = hasher.result_str().to_string();

        let mut dev = "".to_string();
        let mut buckets: Vec<Bucket> = vec![];
        for mnt in mounts.iter() {
            if self.mountpoint.eq(mnt.fs_mounted_on.as_str()) {
                dev = mnt.fs_mounted_from.to_owned();
                let cnt = self.volume_cnt_buckets(mnt.total.as_u64(), bucket_size_limit);
                info!("init {} buckets for volume {}", cnt, self.path);
                for i in 1..cnt + 1 {
                    let bucket_path = format!("{}/{}", self.path, i);
                    let mut bucket = Bucket::new(
                        i, &self.id, &bucket_path, bucket_size_limit,
                    );
                    match bucket.bootstrap() {
                        Ok(_) => {
                            buckets.push(bucket)
                        }
                        Err(x) => {
                            error!("bucket init: {}", x);
                            process::exit(1)
                        }
                    }
                }
            }
        }

        if let Ok(Some(_volume_meta)) = VolumeMeta::get(self.id.to_owned()) {
            self.cnt_objects = 0;
            for b in &buckets {
                self.cnt_objects += b.cnt_blocks;
            }
        }else{
            let mut vm = VolumeMeta::new();
            vm.id = self.id.to_owned();
            vm.path = self.path.to_owned();
            if let Err(_) = vm.upsert() {
                error!("upsert volume")
            }
        }

        self.dev = dev.to_owned();
        self.buckets = buckets;
        for (idx, b) in self.buckets.iter().enumerate() {
            self.buckets_mapping.insert(b.id, idx);
        }
        self.cnt_objects = 0;
        Ok(true)
    }
    fn find_mount_point(&mut self) {
        let mut path = match Path::new(&self.path).parent() {
            Some(parent) => {
                parent.to_str().unwrap().to_string()
            }
            None => {
                "".to_string()
            }
        };
        let orig_stdev = self.st_dev(&path.clone()).unwrap();
        while !&path.eq("/") {
            let dir = Path::new(&path).parent().unwrap().to_str().unwrap().to_string();
            let stdev = self.st_dev(&dir.clone()).unwrap();
            if stdev != orig_stdev {
                break;
            }
            path = dir;
        }
        self.mountpoint = path
    }

    fn st_dev(&mut self, path: &String) -> io::Result<u64> {
        let meta = fs::metadata(path)?;
        Ok(meta.st_dev())
    }

    fn volume_cnt_buckets(&mut self, volume_total_size: u64, bucket_size_limit_bytes: u64) -> u32 {
        (volume_total_size / bucket_size_limit_bytes) as u32 - 1
    }
}