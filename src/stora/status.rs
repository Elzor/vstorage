use std::sync::RwLock;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use systemstat::{CPULoad, Platform, System};
use tokio::time;

use crate::config::Config;
use crate::stora::disk::DISK;
use crate::stora::meta::db_size;

use crate::metrics::{CPU_GAUGE, LA_GAUGE, MEMORY_GAUGE, NET_GAUGE, STORAGE_GAUGE, UPTIME_GAUGE};

lazy_static! {
    pub static ref CONFIG: RwLock<Option<Config>> = RwLock::new(None);
    pub static ref STATUS: RwLock<String> = RwLock::new(String::from("normal"));
    pub static ref CPU: RwLock<CpuStatus> = RwLock::new(CpuStatus::new());
    pub static ref MEMORY: RwLock<MemoryStatus> = RwLock::new(MemoryStatus::new());
    pub static ref LA: RwLock<LaStatus> = RwLock::new(LaStatus::new());
    pub static ref UPTIME: RwLock<UptimeStatus> = RwLock::new(UptimeStatus::new());
    pub static ref NET: RwLock<NetStatus> = RwLock::new(NetStatus::new());
}

pub fn set_config(config: &Config) {
    let mut p = CONFIG.write().unwrap();
    *p = Some(config.clone())
}

#[cfg(target_os = "linux")]
fn iowait(cpu: CPULoad) -> f32 {
    cpu.platform.iowait
}

#[cfg(not(target_os = "linux"))]
fn iowait(cpu: CPULoad) -> f32 {
    0.0
}

#[derive(Serialize, Deserialize)]
pub struct Status {
    pub node: NodeStatus,
    pub meta: MetaStatus,
    pub storage: StorageStatus,
    pub cpu: CpuStatus,
    pub memory: MemoryStatus,
    pub la: LaStatus,
    pub uptime: UptimeStatus,
    pub net: NetStatus,
}

impl Status {
    pub fn new() -> Status {
        Status {
            node: NodeStatus::get(),
            meta: MetaStatus::get(),
            storage: StorageStatus::get(),
            cpu: CpuStatus::get(),
            memory: MemoryStatus::get(),
            la: LaStatus::get(),
            uptime: UptimeStatus::get(),
            net: NetStatus::get(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct NodeStatus {
    pub nodename: String,
    pub status: String,
    pub zone: String,
    pub rest_internal_endpoint: String,
    pub rest_public_endpoint: String,
    pub grpc_internal_endpoint: String,
    pub grpc_public_endpoint: String,
}

impl NodeStatus {
    pub fn get() -> NodeStatus {
        let cfg = CONFIG.read().unwrap().to_owned().unwrap();
        NodeStatus {
            nodename: cfg.node.nodename,
            status: STATUS.read().unwrap().to_string(),
            zone: cfg.node.zone,
            rest_public_endpoint: cfg.interfaces.rest_public,
            rest_internal_endpoint: cfg.interfaces.rest_internal,
            grpc_public_endpoint: cfg.interfaces.grpc_public,
            grpc_internal_endpoint: cfg.interfaces.grpc_internal,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct MetaStatus {
    pub db_size: u64,
}

impl MetaStatus {
    pub fn get() -> MetaStatus {
        MetaStatus {
            db_size: match db_size() {
                Some(s) => s,
                None => 0,
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StorageStatus {
    pub objects: u64,
    pub gc_bytes: u64,
    pub move_bytes: u64,
    pub init_bytes: u64,
    pub avail_bytes: u64,
    pub active_slots: u64,
}

impl StorageStatus {
    pub fn get() -> StorageStatus {
        let disk = DISK.read().unwrap();
        let mut cnt_blocks: u64 = 0;
        let mut active_slots: u64 = 0;
        let mut initial_size: u64 = 0;
        let mut available_size: u64 = 0;
        let mut gc_size: u64 = 0;
        for v in &disk.volumes {
            for b in v.buckets.iter() {
                cnt_blocks += b.cnt_blocks;
                active_slots += b.active_slots;
                initial_size += b.initial_size_bytes;
                available_size += b.avail_size_bytes;
                gc_size += b.gc_size_bytes;
            }
        }
        StorageStatus {
            objects: cnt_blocks,
            gc_bytes: gc_size,
            move_bytes: 0,
            init_bytes: initial_size,
            avail_bytes: available_size,
            active_slots: active_slots,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuStatus {
    pub user: f32,
    pub nice: f32,
    pub system: f32,
    pub interrupt: f32,
    pub idle: f32,
    pub iowait: f32,
}

impl CpuStatus {
    pub fn new() -> CpuStatus {
        CpuStatus {
            user: 0.0,
            nice: 0.0,
            system: 0.0,
            interrupt: 0.0,
            idle: 0.0,
            iowait: 0.0,
        }
    }
    pub fn get() -> CpuStatus {
        CPU.read().unwrap().to_owned()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryStatus {
    pub free: u64,
    pub total: u64,
}

impl MemoryStatus {
    pub fn new() -> MemoryStatus {
        MemoryStatus { free: 0, total: 0 }
    }
    pub fn get() -> MemoryStatus {
        MEMORY.read().unwrap().to_owned()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaStatus {
    pub one: f32,
    pub five: f32,
    pub fifteen: f32,
}

impl LaStatus {
    pub fn new() -> LaStatus {
        LaStatus {
            one: 0.0,
            five: 0.0,
            fifteen: 0.0,
        }
    }
    pub fn get() -> LaStatus {
        LA.read().unwrap().to_owned()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UptimeStatus {
    pub host: u64,
    pub node: u64,
}

impl UptimeStatus {
    pub fn new() -> UptimeStatus {
        UptimeStatus { host: 0, node: 0 }
    }
    pub fn get() -> UptimeStatus {
        UPTIME.read().unwrap().to_owned()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetStatus {
    pub tcp_in_use: usize,
    pub tcp_orphaned: usize,
    pub udp_in_use: usize,
    pub tcp6_in_use: usize,
    pub udp6_in_use: usize,
}

impl NetStatus {
    pub fn new() -> NetStatus {
        NetStatus {
            tcp_in_use: 0,
            tcp_orphaned: 0,
            udp_in_use: 0,
            tcp6_in_use: 0,
            udp6_in_use: 0,
        }
    }
    pub fn get() -> NetStatus {
        NET.read().unwrap().to_owned()
    }
}

pub struct PhysStats {}

impl PhysStats {
    pub fn new() -> PhysStats {
        PhysStats {}
    }

    pub fn calc(self) {
        // storage
        tokio::spawn(async {
            let mut interval = time::interval(Duration::from_secs(60));
            let mut short_interval = time::interval(Duration::from_millis(80));
            loop {
                let status = StorageStatus::get();

                STORAGE_GAUGE
                    .with_label_values(&["active_slots"])
                    .set(status.active_slots as i64);
                STORAGE_GAUGE
                    .with_label_values(&["avail_bytes"])
                    .set(status.avail_bytes as i64);
                STORAGE_GAUGE
                    .with_label_values(&["init_bytes"])
                    .set(status.init_bytes as i64);
                STORAGE_GAUGE
                    .with_label_values(&["move_bytes"])
                    .set(status.move_bytes as i64);
                STORAGE_GAUGE
                    .with_label_values(&["gc_bytes"])
                    .set(status.gc_bytes as i64);
                STORAGE_GAUGE
                    .with_label_values(&["objects"])
                    .set(status.objects as i64);

                if status.objects == 0 && status.init_bytes == 0 {
                    short_interval.tick().await;
                } else {
                    interval.tick().await;
                }
            }
        });
        // cpu
        tokio::spawn(async {
            let mut interval = time::interval(Duration::from_secs(2));
            let sys = System::new();
            loop {
                match sys.cpu_load_aggregate() {
                    Ok(cpu) => {
                        interval.tick().await;
                        let cpu = cpu.done().unwrap();
                        {
                            CPU_GAUGE.with_label_values(&["user"]).set(cpu.user as f64);
                            CPU_GAUGE.with_label_values(&["nice"]).set(cpu.nice as f64);
                            CPU_GAUGE
                                .with_label_values(&["system"])
                                .set(cpu.system as f64);
                            CPU_GAUGE
                                .with_label_values(&["interrupt"])
                                .set(cpu.interrupt as f64);
                            CPU_GAUGE.with_label_values(&["idle"]).set(cpu.idle as f64);
                            CPU_GAUGE
                                .with_label_values(&["iowait"])
                                .set(iowait(cpu.clone()) as f64);
                            let mut p = CPU.write().unwrap();
                            *p = CpuStatus {
                                user: cpu.user,
                                nice: cpu.nice,
                                system: cpu.system,
                                interrupt: cpu.interrupt,
                                idle: cpu.idle,
                                iowait: iowait(cpu),
                            }
                        };
                    }
                    _ => {
                        info!("can't calc cpu load");
                    }
                }
                interval.tick().await;
            }
        });
        // memory
        tokio::spawn(async {
            let mut interval = time::interval(Duration::from_secs(1));
            let sys = System::new();
            loop {
                match sys.memory() {
                    Ok(mem) => {
                        MEMORY_GAUGE
                            .with_label_values(&["free"])
                            .set(mem.free.as_u64() as i64);
                        MEMORY_GAUGE
                            .with_label_values(&["total"])
                            .set(mem.total.as_u64() as i64);

                        let mut p = MEMORY.write().unwrap();
                        *p = MemoryStatus {
                            free: mem.free.as_u64(),
                            total: mem.total.as_u64(),
                        };
                    }
                    _ => {
                        info!("can't calc memory stat");
                    }
                }
                interval.tick().await;
            }
        });
        //la
        tokio::spawn(async {
            let mut interval = time::interval(Duration::from_secs(1));
            let sys = System::new();
            loop {
                match sys.load_average() {
                    Ok(la) => {
                        LA_GAUGE.with_label_values(&["one"]).set(la.one as f64);
                        LA_GAUGE.with_label_values(&["five"]).set(la.five as f64);
                        LA_GAUGE
                            .with_label_values(&["fifteen"])
                            .set(la.fifteen as f64);

                        let mut p = LA.write().unwrap();
                        *p = LaStatus {
                            one: la.one,
                            five: la.five,
                            fifteen: la.fifteen,
                        };
                    }
                    _ => {
                        info!("can't calc la stat");
                    }
                }
                interval.tick().await;
            }
        });
        //uptime
        tokio::spawn(async {
            let mut interval = time::interval(Duration::from_secs(1));
            let sys = System::new();
            loop {
                match sys.uptime() {
                    Ok(up) => {
                        let mut p = UPTIME.write().unwrap();
                        *p = UptimeStatus {
                            host: up.as_secs(),
                            node: (*p).node + 1,
                        };
                        UPTIME_GAUGE
                            .with_label_values(&["host"])
                            .set(up.as_secs() as i64);
                        UPTIME_GAUGE.with_label_values(&["node"]).set(p.node as i64);
                    }
                    _ => {
                        info!("can't calc la stat");
                    }
                }
                interval.tick().await;
            }
        });
        //net
        tokio::spawn(async {
            let mut interval = time::interval(Duration::from_secs(1));
            let sys = System::new();
            loop {
                match sys.socket_stats() {
                    Ok(sock) => {
                        NET_GAUGE
                            .with_label_values(&["tcp_in_use"])
                            .set(sock.tcp_sockets_in_use as i64);
                        NET_GAUGE
                            .with_label_values(&["tcp_orphaned"])
                            .set(sock.tcp_sockets_orphaned as i64);
                        NET_GAUGE
                            .with_label_values(&["udp_in_use"])
                            .set(sock.udp_sockets_in_use as i64);
                        NET_GAUGE
                            .with_label_values(&["tcp6_in_use"])
                            .set(sock.tcp6_sockets_in_use as i64);
                        NET_GAUGE
                            .with_label_values(&["udp6_in_use"])
                            .set(sock.udp6_sockets_in_use as i64);

                        let mut p = NET.write().unwrap();
                        *p = NetStatus {
                            tcp_in_use: sock.tcp_sockets_in_use,
                            tcp_orphaned: sock.tcp_sockets_orphaned,
                            udp_in_use: sock.udp_sockets_in_use,
                            tcp6_in_use: sock.tcp6_sockets_in_use,
                            udp6_in_use: sock.udp6_sockets_in_use,
                        };
                    }
                    _ => {
                        info!("can't calc la stat");
                    }
                }
                interval.tick().await;
            }
        });
    }
}
