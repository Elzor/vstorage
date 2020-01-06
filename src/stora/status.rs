use std::sync::RwLock;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use systemstat::{Platform, System, CPULoad};
use tokio::time;

use crate::config::Config;
use crate::stora::meta::{db_size};
use crate::stora::disk::DISK;

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
fn iowait(cpu: CPULoad) -> f32 { cpu.platform.iowait }

#[cfg(not(target_os = "linux"))]
fn iowait(cpu: CPULoad) -> f32 { 0.0 }

#[derive(Serialize, Deserialize)]
pub struct Status {
    node: NodeStatus,
    meta: MetaStatus,
    storage: StorageStatus,
    cpu: CpuStatus,
    memory: MemoryStatus,
    la: LaStatus,
    uptime: UptimeStatus,
    net: NetStatus,
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
    nodename: String,
    status: String,
    zone: String,
    internal_endpoint: String,
    public_endpoint: String,
}

impl NodeStatus {
    pub fn get() -> NodeStatus {
        let cfg = CONFIG.read().unwrap().to_owned().unwrap();
        NodeStatus {
            nodename: cfg.node.nodename,
            status: STATUS.read().unwrap().to_string(),
            zone: cfg.node.zone,
            public_endpoint: cfg.interfaces.public,
            internal_endpoint: cfg.interfaces.internal,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct MetaStatus {
    db_size: u64,
}

impl MetaStatus {
    pub fn get() -> MetaStatus {
        MetaStatus {
            db_size: match db_size() {
                Some(s) => s,
                None => 0
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct StorageStatus {
    objects: u64,
    gc_bytes: u64,
    move_bytes: u64,
    init_bytes: u64,
    avail_bytes: u64,
    active_slots: u64,
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
    user: f32,
    nice: f32,
    system: f32,
    interrupt: f32,
    idle: f32,
    iowait: f32,
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
    free: u64,
    total: u64,
}

impl MemoryStatus {
    pub fn new() -> MemoryStatus {
        MemoryStatus {
            free: 0,
            total: 0,
        }
    }
    pub fn get() -> MemoryStatus {
        MEMORY.read().unwrap().to_owned()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaStatus {
    one: f32,
    five: f32,
    fifteen: f32,
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
    host: u64,
    node: u64,
}

impl UptimeStatus {
    pub fn new() -> UptimeStatus {
        UptimeStatus {
            host: 0,
            node: 0,
        }
    }
    pub fn get() -> UptimeStatus {
        UPTIME.read().unwrap().to_owned()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetStatus {
    tcp_in_use: usize,
    tcp_orphaned: usize,
    udp_in_use: usize,
    tcp6_in_use: usize,
    udp6_in_use: usize,
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