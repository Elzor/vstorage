use crate::metrics::GC_LOOP_TIME_GAUGE;
use crate::stora::disk::purge_block;
use crate::stora::meta::BlockMeta;
use std::time::{Duration, Instant};
use tokio::time;

pub fn process(batch: u32, timeout: u32) {
    tokio::spawn(async move {
        info!("start GC");
        let mut interval = time::interval(Duration::from_secs(timeout as u64));
        loop {
            let now = Instant::now();
            match BlockMeta::fetch_deleted(batch) {
                Ok(items) => {
                    for bm in items {
                        let _ = purge_block(bm);
                    }
                }
                Err(e) => {
                    error!("gc: {}", e);
                }
            }
            GC_LOOP_TIME_GAUGE.set(now.elapsed().as_millis() as f64);
            interval.tick().await;
        }
    });
}
