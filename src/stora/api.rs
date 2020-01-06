use std::convert::Infallible;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::RwLock;

use bytes::Bytes;
use chrono::prelude::*;
use hyper::{Body, HeaderMap, Method, Request, Response, Server, StatusCode};
use hyper::service::{make_service_fn, service_fn};
use lz4_compress::{compress, decompress};
use prometheus::{Counter, Encoder, HistogramVec, TextEncoder};
use tokio::sync::mpsc::Sender;

use crate::config::Config;
use crate::stora::disk::{DISK, mark_block_as_deleted, read_block};
use crate::stora::meta::{BlockMeta, Compression, HashFun};
use crate::stora::meta::HashFun::{HGW128, HGW256, MD5, SHA128, SHA256};
use crate::stora::status::Status;
use uuid::Uuid;

lazy_static! {
    pub static ref CONFIG: RwLock<Option<Config>> = RwLock::new(None);

    pub static ref HTTP_COUNTER: Counter = register_counter!(opts!(
        "http_requests_total",
        "Total number of HTTP requests made."
    )).unwrap();

    pub static ref HTTP_BYTES_IN: Counter = register_counter!(opts!(
        "http_request_size_bytes",
        "The HTTP request sizes in bytes."
    )).unwrap();

    pub static ref HTTP_BYTES_OUT: Counter = register_counter!(opts!(
        "http_response_size_bytes",
        "The HTTP response sizes in bytes."
    )).unwrap();

    pub static ref HTTP_REQ_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "http_request_duration_seconds",
        "The HTTP request latencies in seconds.",
        &["method"]
    ).unwrap();
}

pub fn set_config(config: &Config) {
    let mut p = CONFIG.write().unwrap();
    *p = Some(config.clone())
}

#[derive(Debug)]
pub struct BlockApi {
    pub endpoint: SocketAddr,
    pub mode: String,
    pub status_channel: Option<Sender<bool>>,
}

async fn block_api(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let mut path = req.uri().path().to_lowercase();
    if path.ends_with('/') {
        //todo: maybe need redirect
        path.pop();
    }
    let v: Vec<&str> = path.split("/").collect();
    let tokens = v[1..].to_vec();
    let tokens_len = tokens.len();

    let cmd = if tokens_len > 0 { tokens[0] } else { "" };

    HTTP_COUNTER.inc();
    let timer = HTTP_REQ_HISTOGRAM.with_label_values(&[req.method().as_str()]).start_timer();

    let hash_fun = |req: &Request<Body>| -> HashFun {
        let hash_fun_header_name = "v-hash-fun";
        if req.headers().contains_key(hash_fun_header_name) {
            match req.headers().get(hash_fun_header_name).unwrap().as_bytes() {
                b"0" => MD5,
                b"1" => SHA128,
                b"2" => SHA256,
                b"3" => HGW128,
                b"4" => HGW256,
                _ => HGW128
            }
        } else {
            HGW128
        }
    };

    let object_id = |req: &Request<Body>| -> String {
        let object_id_header_name = "v-object-id";
        if req.headers().contains_key(object_id_header_name) {
            String::from_utf8(req.headers().get(object_id_header_name).unwrap().as_bytes().to_vec()).unwrap()
        } else {
            "".to_string()
        }
    };

    let hash = |req: &Request<Body>| -> String {
        let hash_header_name = "v-hash";
        if req.headers().contains_key(hash_header_name) {
            String::from_utf8(req.headers().get(hash_header_name).unwrap().as_bytes().to_vec()).unwrap()
        } else {
            "".to_string()
        }
    };

    let compression = |req: &Request<Body>| -> Compression {
        let complress_header_name = "v-compress";
        if req.headers().contains_key(complress_header_name) {
            match String::from_utf8(req.headers().get(complress_header_name).unwrap().as_bytes().to_vec()).unwrap().to_lowercase().as_str() {
                "lz4" => {
                    Compression::LZ4
                }
                _ => {
                    Compression::None
                }
            }
        } else {
            Compression::None
        }
    };

    let payload_size = |req: &Request<Body>| -> u64 {
        let size_header_name = "content-length";
        if req.headers().contains_key(size_header_name) {
            String::from_utf8(req.headers().get(size_header_name).unwrap().as_bytes().to_vec())
                .unwrap().parse::<u64>().unwrap()
        } else {
            0
        }
    };

    let etag = |req: &Request<Body>| -> String {
        let etag_header_name = "if-none-match";
        if req.headers().contains_key(etag_header_name) {
            String::from_utf8(
                req.headers().get(etag_header_name).unwrap().as_bytes().to_vec()
            ).unwrap().replace("\"", "")
        } else {
            "".to_string()
        }
    };

    let accept_encoding = |req: &Request<Body>| -> String {
        let accept_encoding_header_name = "accept-encoding";
        if req.headers().contains_key(accept_encoding_header_name) {
            String::from_utf8(
                req.headers().get(accept_encoding_header_name).unwrap().as_bytes().to_vec()
            ).unwrap()
        } else {
            "".to_string()
        }
    };

    match (req.method(), (cmd, tokens_len), path.as_str()) {
        // -----------------------------------------------------------------------------------------
        (&Method::GET, _, "/") | (&Method::GET, _, "/index.html") | (&Method::GET, _, "") => {
            let msg = "The little block engine that could!";
            let res = Ok(Response::new(Body::from(msg)));
            HTTP_BYTES_OUT.inc_by(msg.len() as f64);
            timer.observe_duration();
            res
        }
        // -----------------------------------------------------------------------------------------
        (&Method::GET, ("status", 1), _) => {
            match serde_json::to_string(&Status::new()) {
                Ok(status) => {
                    HTTP_BYTES_OUT.inc_by(status.len() as f64);
                    let res = Ok(Response::new(Body::from(status)));
                    timer.observe_duration();
                    res
                }
                Err(e) => {
                    error!("json encoder: {}", e);
                    let mut err = Response::default();
                    *err.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    Ok(err)
                }
            }
        }
        // -----------------------------------------------------------------------------------------
        (&Method::GET, ("metrics", 1), _) => {
            let encoder = TextEncoder::new();
            let metric_families = prometheus::gather();
            let mut buffer = vec![];
            encoder.encode(&metric_families, &mut buffer).unwrap();
            HTTP_BYTES_OUT.inc_by(buffer.len() as f64);
            let res = Ok(Response::new(Body::from(buffer)));
            timer.observe_duration();
            res
        }
        // -----------------------------------------------------------------------------------------
        (&Method::HEAD, ("block", 2), _) => {
            let block_id = tokens[1].to_string();
            let code = match BlockMeta::exists(block_id) {
                Ok(true) => StatusCode::FOUND,
                _ => StatusCode::NOT_FOUND
            };
            let mut res = Response::default();
            *res.status_mut() = code;
            timer.observe_duration();
            Ok(res)
        }
        // -----------------------------------------------------------------------------------------
        (&Method::GET, ("block", 2), _) => {
            let block_id = tokens[1].to_string();
            match BlockMeta::get(block_id) {
                Ok(Some(meta)) => {
                    let etag = etag(&req);
                    if !etag.eq("") && etag.eq(&meta.crc) {
                        let mut res = Response::default();
                        *res.status_mut() = StatusCode::NOT_MODIFIED;
                        timer.observe_duration();
                        return Ok(res);
                    }
                    let lz4_transfer = accept_encoding(&req).find("lz4").is_some();
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
                            let mut res = Response::default();
                            *res.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
                            timer.observe_duration();
                            return Ok(res);
                        }
                    };

                    let mut headers = HeaderMap::new();
                    headers.insert(
                        http::header::CONTENT_LENGTH,
                        http::header::HeaderValue::from(body.len()),
                    );
                    headers.insert(
                        http::header::ETAG,
                        http::header::HeaderValue::from_str(format!("\"{}\"", meta.crc).as_str()).unwrap(),
                    );
                    headers.insert(
                        http::header::SERVER,
                        http::header::HeaderValue::from_str("vbs").unwrap(),
                    );
                    headers.insert(
                        http::header::LAST_MODIFIED,
                        http::header::HeaderValue::from_str(
                            Utc.timestamp(meta.created as i64, 0).format("%a, %d %b %Y %T GMT").to_string().as_str()
                        ).unwrap(),
                    );
                    if meta.compressed && lz4_transfer {
                        headers.insert(
                            http::header::CONTENT_ENCODING,
                            http::header::HeaderValue::from_str("lz4").unwrap(),
                        );
                    }

                    HTTP_BYTES_OUT.inc_by(body.len() as f64);

                    let mut res = Response::default();
                    *res.status_mut() = StatusCode::OK;
                    *res.headers_mut() = headers;
                    *res.body_mut() = Body::from(body);

                    timer.observe_duration();
                    Ok(res)
                }
                _ => {
                    let mut res = Response::default();
                    *res.status_mut() = StatusCode::NOT_FOUND;
                    timer.observe_duration();
                    Ok(res)
                }
            }
        }
        // -----------------------------------------------------------------------------------------
        (&Method::PUT, ("block", argc), _) | (&Method::POST, ("block", argc), _) => {
            let block_id = if argc > 1 {
                // with id
                tokens[1].to_string()
            } else {
                // without id
                format!("{}", Uuid::new_v4().to_simple())
            };

            if payload_size(&req) > CONFIG.read().unwrap().clone().unwrap().storage.block_size_limit_bytes {
                let mut res = Response::default();
                *res.status_mut() = StatusCode::PAYLOAD_TOO_LARGE;

                timer.observe_duration();
                return Ok(res);
            }

            match req.method() {
                &Method::PUT => {
                    // check that block with this id is not exists
                    if let Ok(true) = BlockMeta::exists(block_id.clone()) {
                        let mut res = Response::default();
                        *res.status_mut() = StatusCode::FOUND;

                        timer.observe_duration();
                        return Ok(res);
                    }
                }
                _ => ()
            }

            let mut b = BlockMeta::new();
            b.id = block_id.to_owned();
            b.object_id = object_id(&req);
            b.hash_fun = hash_fun(&req);
            b.hash = hash(&req);
            b.size = payload_size(&req);
            b.compressed = compression(&req) == Compression::LZ4;
            b.orig_size = b.size;
            b.last_check_ts = Utc::now().timestamp() as u64;
            let body = hyper::body::to_bytes(req.into_body()).await.unwrap();

            HTTP_BYTES_IN.inc_by(body.len() as f64);

            if body.len() != b.orig_size as usize {
                let mut res = Response::default();
                *res.status_mut() = StatusCode::LENGTH_REQUIRED;

                timer.observe_duration();
                return Ok(res);
            }

            let slot = {
                DISK.write().unwrap().get_write_slot()
            };
            match slot {
                Ok(slot) => {
                    let body = if b.compressed {
                        let compressed_body = compress(&body.to_vec());
                        if compressed_body.len() < body.len() {
                            b.size = compressed_body.len() as u64;
                            b.compressed = true;
                            Bytes::from(compressed_body)
                        } else {
                            b.compressed = false;
                            body
                        }
                    } else {
                        body
                    };

                    match slot.clone().store(body.to_vec()) {
                        Ok(saved_file) => {
                            b.volume_id = slot.volume_id.to_owned();
                            b.bucket_id = slot.bucket_id.to_owned();
                            b.path = saved_file;
                            b.crc = BlockMeta::crc(body.to_vec());
                            if let Err(_) = slot.commit(b) {
                                error!("can't commit slot");
                            }
                        }
                        Err(e) => {
                            slot.release(0);
                            error!("can't write payload {}", e);
                            let mut res = Response::default();
                            *res.status_mut() = StatusCode::SERVICE_UNAVAILABLE;

                            timer.observe_duration();
                            return Ok(res);
                        }
                    }

                    let mut res = Response::default();
                    if argc > 1 {
                        *res.status_mut()  = StatusCode::NO_CONTENT;
                    }else{
                        *res.status_mut()  = StatusCode::OK;
                        *res.body_mut() = Body::from(block_id);
                    };

                    timer.observe_duration();
                    Ok(res)
                }
                Err(_) => {
                    error!("disk get slot");
                    let mut res = Response::default();
                    *res.status_mut() = StatusCode::SERVICE_UNAVAILABLE;

                    timer.observe_duration();
                    Ok(res)
                }
            }
        }
        // -----------------------------------------------------------------------------------------
        (&Method::DELETE, ("block", 2), _) => {
            let block_id = tokens[1].to_string();

            match BlockMeta::get(block_id) {
                Ok(Some(meta)) => {
                    if let Err(_) = mark_block_as_deleted(meta) {
                        error!("can't mark block as deleted")
                    }
                    let mut res = Response::default();
                    *res.status_mut() = StatusCode::NO_CONTENT;
                    timer.observe_duration();
                    Ok(res)
                }
                _ => {
                    let mut res = Response::default();
                    *res.status_mut() = StatusCode::NOT_FOUND;
                    timer.observe_duration();
                    Ok(res)
                }
            }
        }
        // -----------------------------------------------------------------------------------------
        _ => {
            let mut not_found = Response::default();
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
        // -----------------------------------------------------------------------------------------
    }
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
}

impl BlockApi {
    pub fn new(endpoint: &String, mode: &String) -> BlockApi {
        let addr = endpoint.to_socket_addrs().unwrap().next().expect("could not parse address");
        BlockApi {
            endpoint: addr,
            mode: mode.to_owned(),
            status_channel: None,
        }
    }

    pub fn set_status_channel(mut self, ch: Sender<bool>) -> Self {
        self.status_channel = Some(ch);
        self
    }

    pub fn serve(self) {
        let endpoint = self.endpoint.to_owned();
        let ready_ch = self.status_channel;
        let mode = self.mode;
        tokio::spawn(async move {
            let make_svc = make_service_fn(|_conn| async {
                Ok::<_, Infallible>(service_fn(block_api))
            });
            let server = Server::bind(&endpoint).serve(make_svc);
            let graceful = server.with_graceful_shutdown(shutdown_signal());
            info!("start {} http handler: {}", &mode, &endpoint);
            if let Err(e) = graceful.await {
                error!("server error: {}", e);
            }
            // inform about stop
            if ready_ch.is_some() {
                let mut ch = ready_ch.unwrap();
                let _ = ch.send(true).await;
                ()
            }
        });
    }
}