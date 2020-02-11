use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::RwLock;

use chrono::prelude::*;
use lz4_compress::{compress, decompress};
use tonic::{Request, Response, Status, transport::Server};
use uuid::Uuid;

use block_api::{IdxReply, IdxRequest};
use block_api::{status_reply, StatusReply, StatusRequest};
use block_api::{ExistsReply, ExistsRequest};
use block_api::{InsertReply, InsertRequest};
use block_api::{UpsertReply, UpsertRequest};
use block_api::{GetReply, GetRequest};
use block_api::{AppendReply, AppendRequest};
use block_api::{DeleteReply, DeleteRequest};
use block_api::block_api_server::{BlockApi, BlockApiServer};

use crate::config::Config;
use crate::metrics::{GRPC_BYTES_IN, GRPC_BYTES_OUT, GRPC_COUNTER, GRPC_REQ_HISTOGRAM};
use crate::stora::disk::{DISK, mark_block_as_deleted, read_block};
use crate::stora::meta::BlockMeta;
use crate::stora::meta::HashFun::{Hgw128, Hgw256, Md5, Other, Sha128, Sha256};
use crate::stora::status::Status as SysStatus;

lazy_static! {
    pub static ref CONFIG: RwLock<Option<Config>> = RwLock::new(None);
}

pub fn set_config(config: &Config) {
    let mut p = CONFIG.write().unwrap();
    *p = Some(config.clone())
}

pub mod block_api {
    tonic::include_proto!("block_api");
}

#[derive(Debug, Default)]
pub struct MyBlockApi {}

#[tonic::async_trait]
impl BlockApi for MyBlockApi {
    // ---------------------------------------------------------------------------------------------
    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteReply>, Status> {
        let timer = GRPC_REQ_HISTOGRAM
            .with_label_values(&["delete"])
            .start_timer();
        GRPC_COUNTER.inc();
        let request = request.into_inner();
        let block_id = match request.block_id.as_str() {
            "" => {
                timer.observe_duration();
                return Err(tonic::Status::invalid_argument("Block id is required"));
            }
            bid => bid.to_string()
        };
        match BlockMeta::get(block_id) {
            Ok(Some(meta)) => {
                let deleted_bid = meta.id.to_owned();
                if let Err(_) = mark_block_as_deleted(meta) {
                    error!("can't mark block as deleted");
                    timer.observe_duration();
                    return Err(tonic::Status::internal("Metadb issue"));
                }
                timer.observe_duration();
                Ok(Response::new(DeleteReply {
                    block_id: deleted_bid,
                }))
            }
            _ => {
                timer.observe_duration();
                Err(tonic::Status::not_found("Block id is not found"))
            }
        }
    }
    // ---------------------------------------------------------------------------------------------
    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<AppendReply>, Status> {
        let timer = GRPC_REQ_HISTOGRAM
            .with_label_values(&["append"])
            .start_timer();
        GRPC_COUNTER.inc();
        let request = request.into_inner();
        let block_id = match request.block_id.as_str() {
            "" => {
                timer.observe_duration();
                return Err(tonic::Status::invalid_argument("Block id is required"));
            }
            bid => bid.to_string()
        };
        let payload = request.payload;

        GRPC_BYTES_IN.inc_by(payload.len() as f64);

        if payload.len() > CONFIG.read().unwrap().clone().unwrap().storage.block_size_limit_bytes as usize
        {
            timer.observe_duration();
            return Err(tonic::Status::resource_exhausted("Payload too large"));
        }
        match BlockMeta::append(block_id, payload) {
            Ok(Some(meta)) => {
                timer.observe_duration();
                Ok(Response::new(AppendReply {
                    block_id: meta.id.clone(),
                    object_id: meta.object_id.clone(),
                    meta: Some(meta.to_grpc()),
                }))
            }
            _ => {
                timer.observe_duration();
                Err(tonic::Status::not_found("Block id is not found"))
            }
        }
    }
    // ---------------------------------------------------------------------------------------------
    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetReply>, Status> {
        let timer = GRPC_REQ_HISTOGRAM
            .with_label_values(&["get"])
            .start_timer();
        GRPC_COUNTER.inc();
        let request = request.into_inner();
        let block_id = match request.block_id.as_str() {
            "" => {
                timer.observe_duration();
                return Err(tonic::Status::invalid_argument("Block id is required"));
            }
            bid => bid.to_string()
        };
        let crc = request.crc.as_str().to_string();
        let lz4_transfer = request.allow_compressed;
        match BlockMeta::get(block_id) {
            Ok(Some(meta)) => {
                if !crc.eq("") && crc.eq(&meta.crc) {
                    timer.observe_duration();
                    return Ok(Response::new(GetReply {
                        block_id: meta.id,
                        object_id: meta.object_id,
                        not_modified: true,
                        payload: vec![],
                        compressed: false,
                        meta: None,
                    }));
                }
                let body = match read_block(&meta.path) {
                    Ok(content) => {
                        if meta.compressed && !lz4_transfer {
                            decompress(content.as_slice()).unwrap()
                        } else {
                            content
                        }
                    }
                    Err(e) => {
                        error!("can't read block: {}", e);
                        timer.observe_duration();
                        return Err(tonic::Status::unavailable("Disk issue on this machine"));
                    }
                };
                GRPC_BYTES_OUT.inc_by(body.len() as f64);
                timer.observe_duration();
                Ok(Response::new(GetReply {
                    block_id: meta.id.clone(),
                    object_id: meta.object_id.clone(),
                    payload: body,
                    not_modified: false,
                    compressed: !(meta.compressed && !lz4_transfer),
                    meta: Some(meta.to_grpc()),
                }))
            }
            _ => {
                timer.observe_duration();
                return Err(tonic::Status::not_found("Block id is not found"));
            }
        }
    }
    // ---------------------------------------------------------------------------------------------
    async fn upsert(
        &self,
        request: Request<UpsertRequest>,
    ) -> Result<Response<UpsertReply>, Status> {
        let timer = GRPC_REQ_HISTOGRAM
            .with_label_values(&["upsert"])
            .start_timer();
        GRPC_COUNTER.inc();
        let request = request.into_inner();
        let block_id = match request.block_id.as_str() {
            "" => format!("{}", Uuid::new_v4().to_simple()),
            bid => bid.to_string()
        };
        let object_id = request.object_id;
        let payload = request.payload;
        if payload.len() > CONFIG.read().unwrap().clone().unwrap().storage.block_size_limit_bytes as usize
        {
            timer.observe_duration();
            return Err(tonic::Status::resource_exhausted("Payload too large"));
        }

        let mut b = BlockMeta::new();
        b.id = block_id.to_owned();
        b.object_id = object_id.to_owned();
        b.size = payload.len() as u64;
        b.orig_size = b.size;
        b.last_check_ts = Utc::now().timestamp() as u64;
        match request.options {
            Some(options) => {
                b.content_type = options.content_type;
                b.compressed = options.compress;
                b.hash = options.hash;
                b.hash_fun = match options.hash_fun {
                    1 => Md5,
                    2 => Sha128,
                    3 => Sha256,
                    4 => Hgw128,
                    5 => Hgw256,
                    _ => Other,
                };
            }
            _ => {
                // without opts => skip
                ()
            }
        }

        GRPC_BYTES_IN.inc_by(b.size as f64);

        let slot = { DISK.write().unwrap().get_write_slot() };
        match slot {
            Ok(slot) => {
                let body = if b.compressed {
                    let compressed_body = compress(&payload);
                    if compressed_body.len() < payload.len() {
                        b.size = compressed_body.len() as u64;
                        b.compressed = true;
                        compressed_body
                    } else {
                        b.compressed = false;
                        payload
                    }
                } else {
                    payload
                };

                match slot.clone().store(body.to_vec()) {
                    Ok(saved_file) => {
                        b.volume_id = slot.volume_id.to_owned();
                        b.bucket_id = slot.bucket_id.to_owned();
                        b.path = saved_file;
                        b.crc = BlockMeta::crc(body.to_vec());
                        let bc = b.clone().to_owned();
                        if let Err(_) = slot.commit(b) {
                            error!("can't commit slot");
                            timer.observe_duration();
                            return Err(tonic::Status::internal("Disk can't write payload"));
                        } else {
                            timer.observe_duration();
                            Ok(Response::new(UpsertReply {
                                block_id: block_id.clone(),
                                object_id: object_id.clone(),
                                meta: Some(bc.to_grpc()),
                            }))
                        }
                    }
                    Err(e) => {
                        slot.release(0);
                        error!("can't write payload {}", e);
                        timer.observe_duration();
                        return Err(tonic::Status::internal("Disk can't write payload"));
                    }
                }
            }
            Err(_) => {
                error!("disk get slot");
                timer.observe_duration();
                return Err(tonic::Status::internal("Disk get slot issue"));
            }
        }
    }
    // ---------------------------------------------------------------------------------------------
    async fn insert(
        &self,
        request: Request<InsertRequest>,
    ) -> Result<Response<InsertReply>, Status> {
        let timer = GRPC_REQ_HISTOGRAM
            .with_label_values(&["insert"])
            .start_timer();
        GRPC_COUNTER.inc();
        let request = request.into_inner();
        let block_id = match request.block_id.as_str() {
            "" => format!("{}", Uuid::new_v4().to_simple()),
            bid => bid.to_string()
        };
        let object_id = request.object_id;
        let payload = request.payload;
        if payload.len() > CONFIG.read().unwrap().clone().unwrap().storage.block_size_limit_bytes as usize
        {
            timer.observe_duration();
            return Err(tonic::Status::resource_exhausted("Payload too large"));
        }
        if let Ok(true) = BlockMeta::exists(block_id.clone()) {
            timer.observe_duration();
            return Err(tonic::Status::already_exists("Object with this id exists"));
        }

        let mut b = BlockMeta::new();
        b.id = block_id.to_owned();
        b.object_id = object_id.to_owned();
        b.size = payload.len() as u64;
        b.orig_size = b.size;
        b.last_check_ts = Utc::now().timestamp() as u64;
        match request.options {
            Some(options) => {
                b.content_type = options.content_type;
                b.compressed = options.compress;
                b.hash = options.hash;
                b.hash_fun = match options.hash_fun {
                    1 => Md5,
                    2 => Sha128,
                    3 => Sha256,
                    4 => Hgw128,
                    5 => Hgw256,
                    _ => Other,
                };
            }
            _ => {
                // without opts => skip
                ()
            }
        }

        GRPC_BYTES_IN.inc_by(b.size as f64);

        let slot = { DISK.write().unwrap().get_write_slot() };
        match slot {
            Ok(slot) => {
                let body = if b.compressed {
                    let compressed_body = compress(&payload);
                    if compressed_body.len() < payload.len() {
                        b.size = compressed_body.len() as u64;
                        b.compressed = true;
                        compressed_body
                    } else {
                        b.compressed = false;
                        payload
                    }
                } else {
                    payload
                };

                match slot.clone().store(body.to_vec()) {
                    Ok(saved_file) => {
                        b.volume_id = slot.volume_id.to_owned();
                        b.bucket_id = slot.bucket_id.to_owned();
                        b.path = saved_file;
                        b.crc = BlockMeta::crc(body.to_vec());
                        let bc = b.clone().to_owned();
                        if let Err(_) = slot.commit(b) {
                            error!("can't commit slot");
                            timer.observe_duration();
                            return Err(tonic::Status::internal("Disk can't write payload"));
                        } else {
                            timer.observe_duration();
                            Ok(Response::new(InsertReply {
                                block_id: block_id.clone(),
                                object_id: object_id.clone(),
                                meta: Some(bc.to_grpc()),
                            }))
                        }
                    }
                    Err(e) => {
                        slot.release(0);
                        error!("can't write payload {}", e);
                        timer.observe_duration();
                        return Err(tonic::Status::internal("Disk can't write payload"));
                    }
                }
            }
            Err(_) => {
                error!("disk get slot");
                timer.observe_duration();
                return Err(tonic::Status::internal("Disk get slot issue"));
            }
        }
    }
    // ---------------------------------------------------------------------------------------------
    async fn exists(
        &self,
        request: Request<ExistsRequest>,
    ) -> Result<Response<ExistsReply>, Status> {
        let timer = GRPC_REQ_HISTOGRAM
            .with_label_values(&["exists"])
            .start_timer();
        GRPC_COUNTER.inc();
        let block_id = request.into_inner().block_id;
        let found = BlockMeta::exists(block_id) == Ok(true);
        timer.observe_duration();
        Ok(Response::new(ExistsReply {
            found: found,
        }))
    }
    // ---------------------------------------------------------------------------------------------
    async fn idx(
        &self,
        _request: Request<IdxRequest>,
    ) -> Result<Response<IdxReply>, Status> {
        let timer = GRPC_REQ_HISTOGRAM
            .with_label_values(&["idx"])
            .start_timer();
        GRPC_COUNTER.inc();
        timer.observe_duration();
        Ok(Response::new(IdxReply {
            message: "The little block engine that could!".into(),
        }))
    }
    // ---------------------------------------------------------------------------------------------
    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusReply>, Status> {
        let timer = GRPC_REQ_HISTOGRAM
            .with_label_values(&["status"])
            .start_timer();
        GRPC_COUNTER.inc();
        let status = SysStatus::new();
        let reply = StatusReply {
            node: Some(status_reply::Node {
                role: "storage".to_string(),
                nodename: status.node.nodename,
                status: status.node.status,
                zone: status.node.zone,
                rest_public_endpoint: status.node.rest_wan_endpoint,
                rest_internal_endpoint: status.node.rest_lan_endpoint,
                grpc_public_endpoint: status.node.grpc_wan_endpoint,
                grpc_internal_endpoint: status.node.grpc_lan_endpoint,
            }),
            meta: Some(status_reply::Meta {
                db_size: status.meta.db_size,
            }),
            storage: Some(status_reply::Storage {
                objects: status.storage.objects,
                gc_bytes: status.storage.gc_bytes,
                move_bytes: status.storage.move_bytes,
                init_bytes: status.storage.init_bytes,
                avail_bytes: status.storage.avail_bytes,
                active_slots: status.storage.active_slots,
            }),
            cpu: Some(status_reply::Cpu {
                user: status.cpu.user,
                nice: status.cpu.nice,
                system: status.cpu.system,
                interrupt: status.cpu.interrupt,
                idle: status.cpu.idle,
                iowait: status.cpu.iowait,
            }),
            memory: Some(status_reply::Memory {
                free: status.memory.free,
                total: status.memory.total,
            }),
            la: Some(status_reply::La {
                one: status.la.one,
                five: status.la.five,
                fifteen: status.la.fifteen,
            }),
            uptime: Some(status_reply::Uptime {
                host: status.uptime.host,
                node: status.uptime.node,
            }),
            net: Some(status_reply::Net {
                tcp_in_use: status.net.tcp_in_use as u64,
                tcp_orphaned: status.net.tcp_orphaned as u64,
                udp_in_use: status.net.udp_in_use as u64,
                tcp6_in_use: status.net.tcp6_in_use as u64,
                udp6_in_use: status.net.udp6_in_use as u64,
            }),
        };
        timer.observe_duration();
        Ok(Response::new(reply))
    }
}

#[derive(Debug)]
pub struct BlockGrpcApi {
    pub endpoint: SocketAddr,
    pub mode: String,
}

impl BlockGrpcApi {
    pub fn new(endpoint: &String, mode: &String) -> BlockGrpcApi {
        let addr = endpoint
            .to_socket_addrs()
            .unwrap()
            .next()
            .expect("could not parse address");
        BlockGrpcApi {
            endpoint: addr,
            mode: mode.to_owned(),
        }
    }

    pub fn serve(self) {
        let endpoint = self.endpoint.to_owned();
        let mode = self.mode;
        tokio::spawn(async move {
            info!("start {} grpc handler: {}", &mode, &endpoint);
            let srv = MyBlockApi::default();
            let _ = Server::builder()
                .add_service(BlockApiServer::new(srv))
                .serve(endpoint)
                .await;
        });
    }
}