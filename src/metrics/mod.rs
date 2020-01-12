use prometheus::{Counter, Gauge, GaugeVec, HistogramVec, IntGaugeVec};

lazy_static! {
    // ---------------------------------------------------------------------------------------------
    // storage
    // ---------------------------------------------------------------------------------------------
    pub static ref STORAGE_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "storage",
        "Storage metrics",
        &["item"]
    ).unwrap();

    // ---------------------------------------------------------------------------------------------
    // http api
    // ---------------------------------------------------------------------------------------------
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

    // ---------------------------------------------------------------------------------------------
    // gc
    // ---------------------------------------------------------------------------------------------
    pub static ref GC_LOOP_TIME_GAUGE: Gauge = register_gauge!(opts!(
        "gc_batch_purging_time",
        "GC time for deleting batch of blocks from disk (ms)."
    )).unwrap();

    // ---------------------------------------------------------------------------------------------
    // validator
    // ---------------------------------------------------------------------------------------------
    pub static ref CHECK_TIME_GAUGE: Gauge = register_gauge!(opts!(
        "block_checking_time",
        "CRC checking time (mcs)."
    )).unwrap();

    // ---------------------------------------------------------------------------------------------
    // hw
    // ---------------------------------------------------------------------------------------------
    pub static ref META_DB_SIZE_GAUGE: Gauge = register_gauge!(opts!(
        "meta_db_size_bytes",
        "Meta RocksDB size on disk"
    )).unwrap();

    pub static ref CPU_GAUGE: GaugeVec = register_gauge_vec!(
        "cpu",
        "CPU usage",
        &["item"]
    ).unwrap();

    pub static ref NET_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "net",
        "Network sockets and connections usage",
        &["item"]
    ).unwrap();

    pub static ref MEMORY_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "memory",
        "System memory usage (bytes)",
        &["item"]
    ).unwrap();

    pub static ref LA_GAUGE: GaugeVec = register_gauge_vec!(
        "la",
        "System LA",
        &["item"]
    ).unwrap();

    pub static ref UPTIME_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "uptime",
        "Host and node uptime (seconds)",
        &["item"]
    ).unwrap();

}
