use tokio::time;
use std::time::Duration;
use crate::stora::meta::BlockMeta;
use crate::stora::disk::purge_block;

pub fn process(batch: u32, timeout: u32) {
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(timeout as u64));
        loop{
            match BlockMeta::fetch_deleted(batch) {
                Ok(items) => {
                    for bm in items{
                        let _ = purge_block(bm);
                    }
                }
                Err(e) => {
                    error!("gc: {}", e);
                }
            }
            interval.tick().await;
        }
    });
}