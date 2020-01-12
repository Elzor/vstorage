use std::time::{Duration, Instant, SystemTime};

use rocksdb::IteratorMode;
use tokio::time;

use crate::metrics::CHECK_TIME_GAUGE;
use crate::stora::disk::read_block;
use crate::stora::meta::{BlockMeta, METADB};

pub fn process(check_interval_days: u32, timeout: u32) {
    tokio::spawn(async move {
        info!("start block validator");
        let mut interval = time::interval(Duration::from_secs(timeout as u64));
        interval.tick().await;
        loop {
            let mut check_list: Vec<BlockMeta> = vec![];
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            match METADB.read().unwrap().as_ref() {
                Some(db) => {
                    let blocks_cf = db.cf_handle("blocks").unwrap();
                    let mut iterator = db.iterator_cf(blocks_cf, IteratorMode::Start).unwrap();
                    loop {
                        match iterator.next() {
                            None => break,
                            Some((_k, v)) => match BlockMeta::decode(v.to_vec()) {
                                Ok(bm) => {
                                    if bm.last_check_ts + check_interval_days as u64 * 86400 < now {
                                        check_list.push(bm);
                                    }
                                }
                                Err(e) => {
                                    error!("decode block meta: {}", e);
                                    continue;
                                }
                            },
                        }
                    }
                }
                None => {
                    error!("can't read meta db");
                }
            }
            for b in check_list.iter_mut() {
                let now = Instant::now();
                match read_block(&b.path) {
                    Ok(content) => {
                        if !b.crc.eq(&BlockMeta::crc(content)) {
                            //todo: write in error queue
                            error!("found wrong block content: {}", b.id)
                        }
                    }
                    Err(e) => {
                        //todo: write in error queue
                        error!("can't read the block: {}", e)
                    }
                }
                b.last_check_ts = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let b = b.clone();
                if let Err(_) = b.store() {
                    error!("can't update meta block")
                }
                CHECK_TIME_GAUGE.set(now.elapsed().as_micros() as f64);
            }
            interval.tick().await;
        }
    });
}
