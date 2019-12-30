use std::process;

use clap::{App, Arg, crate_authors, crate_version};
use log::{info};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc::channel;

use vblock::binutil::{self, cli_opts, setup};
use vblock::config::Config;
use vblock::stora::api::{BlockApi};
use vblock::stora::status::{PhysStats};

#[tokio::main]
async fn main() {
    let matches = App::new("vm")
        .about("Block storage chunk-handler. Powered by Vonmo")
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

    vblock::stora::status::set_config(&config);
    PhysStats::new().calc();

    vblock::stora::api::set_config(&config);
    vblock::stora::meta::init_db(&config);

    let volumes = setup::bootstrap_volumes(&config);
    vblock::stora::disk::init_volumes(volumes);
    
    // internal handler
    let internal_endpoint = config.interfaces.internal.to_string();
    let (txi, mut rxi) = channel(1);
    BlockApi::new(&internal_endpoint, &"internal".to_string())
        .set_status_channel(txi)
        .serve();

    // public handler
    let public_endpoint = config.interfaces.public.to_string();
    let (txp, mut rxp) = channel(1);
    BlockApi::new(&public_endpoint, &"public".to_string())
        .set_status_channel(txp)
        .serve();
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
        rxi.recv().await;
        rxp.recv().await;
        info!("exit with 130");
        process::exit(130)
    }
}