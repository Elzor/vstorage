use std::process;

use clap::{crate_authors, crate_version, App, Arg};
use log::{info, error};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc::channel;

use vstorage::api::rest::BlockRestApi;
use vstorage::api::rpc::BlockGrpcApi;
use vstorage::binutil::{self, cli_opts, setup};
use vstorage::config::Config;
use vstorage::stora::status::PhysStats;
use vstorage::cluster::CLUSTER;
use vstorage::cluster::coordinator_api;

#[tokio::main]
async fn main() {
    let matches = App::new("block-server")
        .about("Block storage blocks handler. Powered by Vonmo")
        .author(crate_authors!())
        .version(crate_version!())
        .long_version(binutil::vm_version_info().as_ref())
        .arg(
            Arg::with_name(cli_opts::CONFIG)
                .short("C")
                .long("config")
                .value_name("FILE")
                .help("Set the configuration file")
                //                .default_value("vm.yml")
                .takes_value(true),
        )
        .arg(
            Arg::with_name(cli_opts::PRINT_SAMPLE_CONFIG)
                .long("print-sample-config")
                .help("Print a sample config to stdout"),
        )
        .get_matches();

    if matches.is_present(cli_opts::PRINT_SAMPLE_CONFIG) {
        let config = Config::default();
        println!("{}", serde_yaml::to_string(&config).unwrap());
        process::exit(0);
    }
    let mut config = matches
        .value_of(cli_opts::CONFIG)
        .map_or_else(Config::default, |path| Config::from_file(&path));

    setup::overwrite_config_with_cmd_args(&mut config, &matches);

    if let Err(e) = config.check_critical_params() {
        panic!("config: critical params check failed: {}", e.to_string());
    }

    setup::init_logger(&config);
    setup::write_pidfile(&config);

    vstorage::stora::status::set_config(&config);
    PhysStats::new().calc();

    vstorage::api::rest::set_config(&config);
    vstorage::api::rpc::set_config(&config);
    vstorage::stora::meta::init_db(&config);

    let volumes = setup::bootstrap_volumes(&config);
    vstorage::stora::disk::init_volumes(volumes);
    vstorage::stora::gc::process(config.storage.gc_batch, config.storage.gc_timeout_sec);
    vstorage::stora::validator::process(config.storage.block_check_interval_days, 300);

    //init cluster
    if config.cluster.enabled {
        if config.cluster.coordinators.len() == 0 {
            error!("cluster mode enabled but coordinators weren't set");
            process::exit(1);
        }
        for e in config.cluster.coordinators {
            let _ = CLUSTER.write().unwrap().add_coordinator(e);
        }
        let _ = vstorage::cluster::health_check();
        loop {
            match CLUSTER.read().unwrap().get_coordinator() {
                Ok(mut coordinator) => {
                    let pong = coordinator.ping().await.unwrap_or(false);
                    if pong {
                        let registred = coordinator.register(coordinator_api::Server{
                            endpoint: config.interfaces.rest.lan.clone(),
                            nodename: config.node.nodename.clone(),
                            zone: config.node.zone.clone(),
                            rack: config.node.rack.clone(),
                        }).await.unwrap_or(false);
                        if registred {
                            break;
                        }else{
                            error!("cluster: can't register");
                            std::thread::sleep(std::time::Duration::from_secs(1));
                        }
                    }
                }
                _ => {
                    error!("cluster: can't register. No coordinators");
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        }
    }

    // internal handlers
    let rest_lan_endpoint = config.interfaces.rest.lan;
    let (txi, mut rxi) = channel(1);
    if !rest_lan_endpoint.eq("") {
        BlockRestApi::new(&rest_lan_endpoint, &"internal".to_string())
            .set_status_channel(txi)
            .serve();
    }

    let grpc_lan_endpoint = config.interfaces.grpc.lan;
    if !grpc_lan_endpoint.eq("") {
        BlockGrpcApi::new(&grpc_lan_endpoint, &"internal".to_string())
            .serve();
    }

    // public handlers
    let rest_wan_endpoint = config.interfaces.rest.wan;
    let (txp, mut rxp) = channel(1);
    if !rest_wan_endpoint.eq("") {
        BlockRestApi::new(&rest_wan_endpoint, &"public".to_string())
            .set_status_channel(txp)
            .serve();
    }

    let grpc_wan_endpoint = config.interfaces.grpc.wan;
    if !grpc_wan_endpoint.eq("") {
        BlockGrpcApi::new(&grpc_wan_endpoint, &"public".to_string())
            .serve();
    }

    // init os signals handler
    tokio::spawn(async move {
        let mut stream_hup = signal(SignalKind::hangup()).unwrap();
        loop {
            stream_hup.recv().await;
            info!("pid {} got HUP signal", process::id());
        }
    });
    tokio::spawn(async {
        let mut stream_term = signal(SignalKind::terminate()).unwrap();
        loop {
            stream_term.recv().await;
            info!("pid {} got TERM signal. exit with 0", process::id());
            process::exit(0);
        }
    });
    let mut stream_interrupt = signal(SignalKind::interrupt()).unwrap();
    loop {
        stream_interrupt.recv().await;
        info!("pid {} got INT signal", process::id());
        info!("wait to http graceful shutdown");
        if !rest_lan_endpoint.eq("") {
            rxi.recv().await;
        }
        if !rest_wan_endpoint.eq("") {
            rxp.recv().await;
        }
        info!("exit with 130");
        process::exit(130)
    }
}
