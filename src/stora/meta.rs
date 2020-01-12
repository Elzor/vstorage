extern crate walkdir;

use std::fmt::Error;
use std::sync::RwLock;
use std::time::SystemTime;

use highway::{HighwayBuilder, HighwayHash, Key};
use rmps::Serializer;
use rocksdb::{IteratorMode, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use tokio::time;
use walkdir::WalkDir;

use crate::binutil::setup;
use crate::config::Config;
use crate::metrics::META_DB_SIZE_GAUGE;

#[derive(Debug)]
pub struct Metainfo {}

lazy_static! {
    pub static ref METADB: RwLock<Option<DB>> = RwLock::new(None);
    pub static ref DBSIZE: RwLock<Option<u64>> = RwLock::new(None);
    pub static ref LAST_BACKUP_TS: RwLock<Option<u64>> = RwLock::new(None);
}

pub fn init_db(config: &Config) {
    let db = setup::init_metadb(&config);
    let mut p = METADB.write().unwrap();
    *p = Some(db);
    let db_path = config.db.meta_db_path.to_string();
    let calc_interval = config.db.size_calculation_interval_min as u64;
    tokio::spawn(async move {
        let mut interval = time::interval(std::time::Duration::from_secs(calc_interval * 60));
        interval.tick().await;
        loop {
            info!("calculate meta_db size: start");
            let total_size = WalkDir::new(&db_path)
                .min_depth(1)
                .max_depth(3)
                .into_iter()
                .filter_map(|entry| entry.ok())
                .filter_map(|entry| entry.metadata().ok())
                .filter(|metadata| metadata.is_file())
                .fold(0, |acc, m| acc + m.len());
            {
                let mut p = DBSIZE.write().unwrap();
                META_DB_SIZE_GAUGE.set(total_size as f64);
                *p = Some(total_size);
            }
            info!("calculate meta_db size: done");
            interval.tick().await;
        }
    });
    ()
}

pub fn db_size() -> Option<u64> {
    DBSIZE.read().unwrap().to_owned()
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum HashFun {
    MD5,
    SHA128,
    SHA256,
    HGW128,
    HGW256,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub enum Compression {
    None,
    LZ4,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct BlockMeta {
    pub id: String,
    pub object_id: String,
    pub volume_id: String,
    pub bucket_id: u32,
    pub hash_fun: HashFun,
    pub hash: String,
    pub crc: String,
    pub size: u64,
    pub orig_size: u64,
    pub compressed: bool,
    pub path: String,
    pub created: u64,
    pub last_check_ts: u64,
}

impl BlockMeta {
    pub fn new() -> BlockMeta {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        BlockMeta {
            id: "".to_string(),
            object_id: "".to_string(),
            volume_id: "".to_string(),
            bucket_id: 0,
            hash_fun: HashFun::HGW128,
            hash: "".to_string(),
            crc: "".to_string(),
            size: 0,
            orig_size: 0,
            compressed: false,
            path: "".to_string(),
            created: now,
            last_check_ts: now,
        }
    }

    #[inline]
    pub fn encode(self) -> Result<Vec<u8>, Error> {
        let mut buf: Vec<u8> = Vec::new();
        self.serialize(&mut Serializer::new(&mut buf)).unwrap();
        //        dbg!(buf.len());
        //        dbg!(buf.as_slice().iter().map(|&c| c as char).collect::<String>());
        Ok(buf)
    }

    #[inline]
    pub fn decode(payload: Vec<u8>) -> Result<BlockMeta, Error> {
        let r: BlockMeta = rmps::from_read_ref(&payload).unwrap();
        Ok(r)
    }

    pub fn store(self) -> Result<(), ()> {
        match METADB.write().unwrap().as_ref() {
            Some(db) => {
                let blocks_cf = db.cf_handle("blocks").unwrap();
                let buckets_cf = db.cf_handle("buckets").unwrap();

                let bucket_db_id = BucketMeta::db_id(self.bucket_id, &self.volume_id);
                let mut bucket = match db.get_cf(buckets_cf, bucket_db_id.as_str()) {
                    Ok(None) => return Err(()),
                    Ok(r) => match BucketMeta::decode(r.unwrap()) {
                        Ok(res) => res,
                        Err(e) => {
                            error!("decode bucket meta: {}", e);
                            return Err(());
                        }
                    },
                    _ => return Err(()),
                };
                bucket.cnt_blocks += 1;
                bucket.avail_size_bytes -= self.size;

                let mut batch = WriteBatch::default();
                let _ = batch.put_cf(
                    blocks_cf,
                    &self.id.as_str().to_owned(),
                    self.encode().unwrap(),
                );
                let _ = batch.put_cf(
                    buckets_cf,
                    bucket_db_id.to_owned(),
                    bucket.encode().unwrap(),
                );

                match db.write(batch) {
                    Ok(_) => Ok(()),
                    Err(_) => Err(()),
                }
            }
            None => Err(()),
        }
    }

    pub fn purge(self) -> Result<(), ()> {
        match METADB.write().unwrap().as_ref() {
            Some(db) => {
                let buckets_cf = db.cf_handle("buckets").unwrap();
                let delete_queue_cf = db.cf_handle("delete_queue").unwrap();

                let bucket_db_id = BucketMeta::db_id(self.bucket_id, &self.volume_id);
                let mut bucket = match db.get_cf(buckets_cf, bucket_db_id.as_str()) {
                    Ok(None) => return Err(()),
                    Ok(r) => match BucketMeta::decode(r.unwrap()) {
                        Ok(res) => res,
                        Err(e) => {
                            error!("decode bucket meta: {}", e);
                            return Err(());
                        }
                    },
                    _ => return Err(()),
                };
                bucket.gc_size_bytes -= self.size;
                bucket.avail_size_bytes += self.size;

                let mut batch = WriteBatch::default();
                let _ = batch.delete_cf(delete_queue_cf, &self.id.as_str().to_owned());
                let _ = batch.put_cf(
                    buckets_cf,
                    bucket_db_id.to_owned(),
                    bucket.encode().unwrap(),
                );

                match db.write(batch) {
                    Ok(_) => Ok(()),
                    Err(_) => Err(()),
                }
            }
            None => Err(()),
        }
    }

    pub fn delete(self) -> Result<(), ()> {
        match METADB.write().unwrap().as_ref() {
            Some(db) => {
                let blocks_cf = db.cf_handle("blocks").unwrap();
                let buckets_cf = db.cf_handle("buckets").unwrap();
                let delete_queue_cf = db.cf_handle("delete_queue").unwrap();

                let bucket_db_id = BucketMeta::db_id(self.bucket_id, &self.volume_id);
                let mut bucket = match db.get_cf(buckets_cf, bucket_db_id.as_str()) {
                    Ok(None) => return Err(()),
                    Ok(r) => match BucketMeta::decode(r.unwrap()) {
                        Ok(res) => res,
                        Err(e) => {
                            error!("decode bucket meta: {}", e);
                            return Err(());
                        }
                    },
                    _ => return Err(()),
                };
                bucket.cnt_blocks -= 1;
                bucket.gc_size_bytes += self.size;

                let mut batch = WriteBatch::default();
                let _ = batch.delete_cf(blocks_cf, &self.id.as_str().to_owned());
                let _ = batch.put_cf(
                    buckets_cf,
                    bucket_db_id.to_owned(),
                    bucket.encode().unwrap(),
                );
                let _ = batch.put_cf(
                    delete_queue_cf,
                    &self.id.as_str().to_owned(),
                    self.encode().unwrap(),
                );

                match db.write(batch) {
                    Ok(_) => Ok(()),
                    Err(_) => Err(()),
                }
            }
            None => Err(()),
        }
    }

    pub fn fetch_deleted(limit: u32) -> Result<Vec<BlockMeta>, Error> {
        let mut res: Vec<BlockMeta> = vec![];
        match METADB.read().unwrap().as_ref() {
            Some(db) => {
                let delete_queue_cf = db.cf_handle("delete_queue").unwrap();
                let iterator = db
                    .iterator_cf(delete_queue_cf, IteratorMode::Start)
                    .unwrap();
                let raw = iterator.take(limit as usize).collect::<Vec<_>>();
                for r in raw {
                    match BlockMeta::decode(r.1.to_vec()) {
                        Ok(bm) => {
                            res.push(bm);
                        }
                        Err(e) => {
                            error!("decode block meta: {}", e);
                            return Err(e);
                        }
                    }
                }
                return Ok(res);
            }
            None => {
                dbg!("here");
            }
        }
        Ok(res)
    }

    pub fn get(block_id: String) -> Result<Option<BlockMeta>, Error> {
        match METADB.read().unwrap().as_ref() {
            Some(db) => {
                let cf = db.cf_handle("blocks").unwrap();
                match db.get_cf(cf, block_id.as_str()) {
                    Ok(None) => Ok(None),
                    Ok(r) => match BlockMeta::decode(r.unwrap()) {
                        Ok(res) => Ok(Some(res)),
                        Err(e) => {
                            error!("decode block meta: {}", e);
                            Err(e)
                        }
                    },
                    _ => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    pub fn exists(block_id: String) -> Result<bool, Error> {
        match METADB.read().unwrap().as_ref() {
            Some(db) => {
                let cf = db.cf_handle("blocks").unwrap();
                match db.get_cf(cf, block_id.as_str()) {
                    Ok(None) => Ok(false),
                    Ok(_) => Ok(true),
                    _ => Ok(false),
                }
            }
            None => Ok(false),
        }
    }

    pub fn crc(payload: Vec<u8>) -> String {
        let key = Key([
            0x0706050403020100,
            0x0F0E0D0C0B0A0908,
            0x1716151413121110,
            0x1F1E1D1C1B1A1918,
        ]);
        let mut hasher = HighwayBuilder::new(&key);
        hasher.append(payload.as_slice());
        let res: [u64; 2] = hasher.finalize128();
        format!("{:x}{:x}", res[0], res[1]).to_string()
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct VolumeMeta {
    pub id: String,
    pub path: String,
    pub last_check_ts: u64,
}

impl VolumeMeta {
    pub fn new() -> VolumeMeta {
        VolumeMeta {
            id: "".to_string(),
            path: "".to_string(),
            last_check_ts: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    #[inline]
    pub fn encode(self) -> Result<Vec<u8>, Error> {
        let mut buf: Vec<u8> = Vec::new();
        self.serialize(&mut Serializer::new(&mut buf)).unwrap();
        Ok(buf)
    }

    #[inline]
    pub fn decode(payload: Vec<u8>) -> Result<VolumeMeta, Error> {
        let r: VolumeMeta = rmps::from_read_ref(&payload).unwrap();
        Ok(r)
    }

    pub fn upsert(self) -> Result<(), ()> {
        match METADB.write().unwrap().as_ref() {
            Some(db) => {
                let cf = db.cf_handle("volumes").unwrap();
                match db.put_cf(cf, &self.id.as_str().to_owned(), self.encode().unwrap()) {
                    Ok(_) => Ok(()),
                    Err(_) => Err(()),
                }
            }
            None => Err(()),
        }
    }

    pub fn get(volume_id: String) -> Result<Option<VolumeMeta>, Error> {
        match METADB.read().unwrap().as_ref() {
            Some(db) => {
                let cf = db.cf_handle("volumes").unwrap();
                match db.get_cf(cf, volume_id.as_str()) {
                    Ok(None) => Ok(None),
                    Ok(r) => match VolumeMeta::decode(r.unwrap()) {
                        Ok(res) => Ok(Some(res)),
                        Err(e) => {
                            error!("decode volume meta: {}", e);
                            Err(e)
                        }
                    },
                    _ => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    pub fn exists(volume_id: String) -> Result<bool, Error> {
        match METADB.read().unwrap().as_ref() {
            Some(db) => {
                let cf = db.cf_handle("volumes").unwrap();
                match db.get_cf(cf, volume_id.as_str()) {
                    Ok(None) => Ok(false),
                    Ok(_) => Ok(true),
                    _ => Ok(false),
                }
            }
            None => Ok(false),
        }
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct BucketMeta {
    pub cnt_blocks: u64,
    pub active_slots: u64,
    pub init_size_bytes: u64,
    pub avail_size_bytes: u64,
    pub gc_size_bytes: u64,
    pub ts: u64,
}

impl BucketMeta {
    pub fn new() -> BucketMeta {
        BucketMeta {
            cnt_blocks: 0,
            active_slots: 0,
            init_size_bytes: 0,
            avail_size_bytes: 0,
            gc_size_bytes: 0,
            ts: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    #[inline]
    pub fn db_id(id: u32, volume_id: &String) -> String {
        format!("{:05}-{}", id, &volume_id.to_owned())
    }

    pub fn upsert(self, id: u32, volume_id: &String) -> Result<(), ()> {
        match METADB.write().unwrap().as_ref() {
            Some(db) => {
                let cf = db.cf_handle("buckets").unwrap();
                match db.put_cf(
                    cf,
                    BucketMeta::db_id(id, &volume_id.to_owned()),
                    self.encode().unwrap(),
                ) {
                    Ok(_) => Ok(()),
                    Err(_) => Err(()),
                }
            }
            None => Err(()),
        }
    }

    #[inline]
    pub fn encode(self) -> Result<Vec<u8>, Error> {
        let mut buf: Vec<u8> = Vec::new();
        self.serialize(&mut Serializer::new(&mut buf)).unwrap();
        Ok(buf)
    }

    #[inline]
    pub fn decode(payload: Vec<u8>) -> Result<BucketMeta, Error> {
        let r: BucketMeta = rmps::from_read_ref(&payload).unwrap();
        Ok(r)
    }

    pub fn get(bucket_db_id: String) -> Result<Option<BucketMeta>, Error> {
        match METADB.read().unwrap().as_ref() {
            Some(db) => {
                let cf = db.cf_handle("buckets").unwrap();
                match db.get_cf(cf, bucket_db_id.as_str()) {
                    Ok(None) => Ok(None),
                    Ok(r) => match BucketMeta::decode(r.unwrap()) {
                        Ok(res) => Ok(Some(res)),
                        Err(e) => {
                            error!("decode bucket meta: {}", e);
                            Err(e)
                        }
                    },
                    _ => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    pub fn exists(bucket_id: String) -> Result<bool, Error> {
        match METADB.read().unwrap().as_ref() {
            Some(db) => {
                let cf = db.cf_handle("buckets").unwrap();
                match db.get_cf(cf, bucket_id.as_str()) {
                    Ok(None) => Ok(false),
                    Ok(_) => Ok(true),
                    _ => Ok(false),
                }
            }
            None => Ok(false),
        }
    }
}
