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
use crate::stora::disk::{DISK, mark_block_as_deleted, read_block};
use crate::stora::meta::BlockMeta;
use crate::stora::meta::HashFun::{HGW128, HGW256, MD5, OTHER, SHA128, SHA256};
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
        let request = request.into_inner();
        let block_id = match request.block_id.as_str() {
            "" => {
                return Err(tonic::Status::invalid_argument("Block id is required"));
            }
            bid => bid.to_string()
        };
        match BlockMeta::get(block_id) {
            Ok(Some(meta)) => {
                let deleted_bid = meta.id.to_owned();
                if let Err(_) = mark_block_as_deleted(meta) {
                    error!("can't mark block as deleted");
                    return Err(tonic::Status::internal("Metadb issue"));
                }
                Ok(Response::new(DeleteReply {
                    block_id: deleted_bid,
                }))
            }
            _ => {
                Err(tonic::Status::not_found("Block id is not found"))
            }
        }
    }
    // ---------------------------------------------------------------------------------------------
    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<AppendReply>, Status> {
        let request = request.into_inner();
        let block_id = match request.block_id.as_str() {
            "" => {
                return Err(tonic::Status::invalid_argument("Block id is required"));
            }
            bid => bid.to_string()
        };
        let payload = request.payload;
        if payload.len() > CONFIG.read().unwrap().clone().unwrap().storage.block_size_limit_bytes as usize
        {
            return Err(tonic::Status::resource_exhausted("Payload too large"));
        }
        match BlockMeta::append(block_id, payload) {
            Ok(Some(meta)) => {
                Ok(Response::new(AppendReply {
                    block_id: meta.id,
                    object_id: meta.object_id,
                    meta: Some(block_api::Meta {
                        content_type: meta.content_type,
                        crc: meta.crc,
                        created: meta.created,
                        hash: meta.hash,
                        hash_fun: match meta.hash_fun {
                            MD5 => block_api::HashFun::Md5 as i32,
                            SHA128 => block_api::HashFun::Sha128 as i32,
                            SHA256 => block_api::HashFun::Sha256 as i32,
                            HGW128 => block_api::HashFun::Hgw128 as i32,
                            HGW256 => block_api::HashFun::Hgw256 as i32,
                            _ => block_api::HashFun::Other as i32,
                        },
                        last_check: meta.last_check_ts,
                        size: meta.size,
                    }),
                }))
            }
            _ => {
                Err(tonic::Status::not_found("Block id is not found"))
            }
        }
    }
    // ---------------------------------------------------------------------------------------------
    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetReply>, Status> {
        let request = request.into_inner();
        let block_id = match request.block_id.as_str() {
            "" => {
                return Err(tonic::Status::invalid_argument("Block id is required"));
            }
            bid => bid.to_string()
        };
        let crc = request.crc.as_str().to_string();
        let lz4_transfer = request.allow_compressed;
        match BlockMeta::get(block_id) {
            Ok(Some(meta)) => {
                if !crc.eq("") && crc.eq(&meta.crc) {
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
                        return Err(tonic::Status::unavailable("Disk issue on this machine"));
                    }
                };
                Ok(Response::new(GetReply {
                    block_id: meta.id,
                    object_id: meta.object_id,
                    payload: body,
                    not_modified: false,
                    compressed: !(meta.compressed && !lz4_transfer),
                    meta: Some(block_api::Meta {
                        content_type: meta.content_type,
                        crc: meta.crc,
                        created: meta.created,
                        hash: meta.hash,
                        hash_fun: match meta.hash_fun {
                            MD5 => block_api::HashFun::Md5 as i32,
                            SHA128 => block_api::HashFun::Sha128 as i32,
                            SHA256 => block_api::HashFun::Sha256 as i32,
                            HGW128 => block_api::HashFun::Hgw128 as i32,
                            HGW256 => block_api::HashFun::Hgw256 as i32,
                            _ => block_api::HashFun::Other as i32,
                        },
                        last_check: meta.last_check_ts,
                        size: meta.size,
                    }),
                }))
            }
            _ => {
                return Err(tonic::Status::not_found("Block id is not found"));
            }
        }
    }
    // ---------------------------------------------------------------------------------------------
    async fn upsert(
        &self,
        request: Request<UpsertRequest>,
    ) -> Result<Response<UpsertReply>, Status> {
        let request = request.into_inner();
        let block_id = match request.block_id.as_str() {
            "" => format!("{}", Uuid::new_v4().to_simple()),
            bid => bid.to_string()
        };
        let object_id = request.object_id;
        let payload = request.payload;
        if payload.len() > CONFIG.read().unwrap().clone().unwrap().storage.block_size_limit_bytes as usize
        {
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
                    1 => MD5,
                    2 => SHA128,
                    3 => SHA256,
                    4 => HGW128,
                    5 => HGW256,
                    _ => OTHER,
                };
            }
            _ => {
                // without opts => skip
                ()
            }
        }

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
                            return Err(tonic::Status::internal("Disk can't write payload"));
                        } else {
                            Ok(Response::new(UpsertReply {
                                block_id: block_id,
                                object_id: object_id,
                                meta: Some(block_api::Meta {
                                    content_type: bc.content_type,
                                    crc: bc.crc,
                                    created: bc.created,
                                    hash: bc.hash,
                                    hash_fun: match bc.hash_fun {
                                        MD5 => block_api::HashFun::Md5 as i32,
                                        SHA128 => block_api::HashFun::Sha128 as i32,
                                        SHA256 => block_api::HashFun::Sha256 as i32,
                                        HGW128 => block_api::HashFun::Hgw128 as i32,
                                        HGW256 => block_api::HashFun::Hgw256 as i32,
                                        _ => block_api::HashFun::Other as i32,
                                    },
                                    last_check: bc.last_check_ts,
                                    size: bc.size,
                                }),
                            }))
                        }
                    }
                    Err(e) => {
                        slot.release(0);
                        error!("can't write payload {}", e);
                        return Err(tonic::Status::internal("Disk can't write payload"));
                    }
                }
            }
            Err(_) => {
                error!("disk get slot");
                return Err(tonic::Status::internal("Disk get slot issue"));
            }
        }
    }
    // ---------------------------------------------------------------------------------------------
    async fn insert(
        &self,
        request: Request<InsertRequest>,
    ) -> Result<Response<InsertReply>, Status> {
        let request = request.into_inner();
        let block_id = match request.block_id.as_str() {
            "" => format!("{}", Uuid::new_v4().to_simple()),
            bid => bid.to_string()
        };
        let object_id = request.object_id;
        let payload = request.payload;
        if payload.len() > CONFIG.read().unwrap().clone().unwrap().storage.block_size_limit_bytes as usize
        {
            return Err(tonic::Status::resource_exhausted("Payload too large"));
        }
        if let Ok(true) = BlockMeta::exists(block_id.clone()) {
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
                    1 => MD5,
                    2 => SHA128,
                    3 => SHA256,
                    4 => HGW128,
                    5 => HGW256,
                    _ => OTHER,
                };
            }
            _ => {
                // without opts => skip
                ()
            }
        }

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
                            return Err(tonic::Status::internal("Disk can't write payload"));
                        } else {
                            Ok(Response::new(InsertReply {
                                block_id: block_id,
                                object_id: object_id,
                                meta: Some(block_api::Meta {
                                    content_type: bc.content_type,
                                    crc: bc.crc,
                                    created: bc.created,
                                    hash: bc.hash,
                                    hash_fun: match bc.hash_fun {
                                        MD5 => block_api::HashFun::Md5 as i32,
                                        SHA128 => block_api::HashFun::Sha128 as i32,
                                        SHA256 => block_api::HashFun::Sha256 as i32,
                                        HGW128 => block_api::HashFun::Hgw128 as i32,
                                        HGW256 => block_api::HashFun::Hgw256 as i32,
                                        _ => block_api::HashFun::Other as i32,
                                    },
                                    last_check: bc.last_check_ts,
                                    size: bc.size,
                                }),
                            }))
                        }
                    }
                    Err(e) => {
                        slot.release(0);
                        error!("can't write payload {}", e);
                        return Err(tonic::Status::internal("Disk can't write payload"));
                    }
                }
            }
            Err(_) => {
                error!("disk get slot");
                return Err(tonic::Status::internal("Disk get slot issue"));
            }
        }
    }
    // ---------------------------------------------------------------------------------------------
    async fn exists(
        &self,
        request: Request<ExistsRequest>,
    ) -> Result<Response<ExistsReply>, Status> {
        let block_id = request.into_inner().block_id;
        Ok(Response::new(ExistsReply {
            found: BlockMeta::exists(block_id) == Ok(true),
        }))
    }
    // ---------------------------------------------------------------------------------------------
    async fn idx(
        &self,
        _request: Request<IdxRequest>,
    ) -> Result<Response<IdxReply>, Status> {
        Ok(Response::new(IdxReply {
            message: "The little block engine that could!".into(),
        }))
    }
    // ---------------------------------------------------------------------------------------------
    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusReply>, Status> {
        let status = SysStatus::new();
        let reply = StatusReply {
            node: Some(status_reply::Node {
                nodename: status.node.nodename,
                status: status.node.status,
                zone: status.node.zone,
                rest_public_endpoint: status.node.rest_public_endpoint,
                rest_internal_endpoint: status.node.rest_internal_endpoint,
                grpc_public_endpoint: status.node.grpc_public_endpoint,
                grpc_internal_endpoint: status.node.grpc_internal_endpoint,
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