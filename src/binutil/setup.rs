use clap::ArgMatches;
use vm_util::collections::HashMap;

use crate::config::Config;
use crate::stora::volume::Volume;
use rocksdb::{Options, DB};
use std::fs;
use std::io::Write;
use std::process;
use systemstat::{Platform, System};

pub fn init_logger(config: &Config) {
    let path = config.node.logger_config.to_owned();
    match log4rs::init_file(&path, Default::default()) {
        Ok(_) => info!("logger is ready. config file: {}", &path),
        Err(_x) => println!("[error] logger config file not found: {}", &path),
    }
}

pub fn init_metadb(config: &Config) -> DB {
    if !fs::metadata(&config.db.meta_db_path).is_ok() {
        fs::create_dir_all(&config.db.meta_db_path).expect("can't metadb path");
    }
    if !fs::metadata(&config.db.meta_db_backup_path).is_ok() {
        fs::create_dir_all(&config.db.meta_db_backup_path).expect("can't metadb backup path");
    }

    let mut opts = Options::default();
    opts.set_keep_log_file_num(10);

    let mut db = match DB::list_cf(&opts, &config.db.meta_db_path) {
        Ok(cfs) => {
            info!("open metadb: {:?}", cfs);
            DB::open_cf(&opts, &config.db.meta_db_path, cfs)
        }
        Err(e) => {
            warn!("metadb: {}", e);
            DB::open_default(&config.db.meta_db_path)
        }
    }
    .expect("meta db issue");
    let _ = db.create_cf("volumes", &opts);
    let _ = db.create_cf("buckets", &opts);
    let _ = db.create_cf("blocks", &opts);
    let _ = db.create_cf("delete_queue", &opts);
    let _ = db.create_cf("move_queue", &opts);
    db
}

pub fn write_pidfile(config: &Config) {
    delete_pidfile(&config);
    let pid_file = config.node.pid_file.clone();
    let mut f = fs::File::create(&pid_file).expect("can't open pid file");
    let _ = f.write_all((process::id() as u32).to_string().as_bytes().clone());
    info!(
        "start server. pid {}, pidfile: {}",
        process::id(),
        &pid_file
    );
}

pub fn delete_pidfile(config: &Config) {
    let pid_file = config.node.pid_file.to_owned();
    if pid_file.ne("") {
        let _ = fs::remove_file(&pid_file);
    }
}

pub fn bootstrap_volumes(config: &Config) -> Vec<Volume> {
    let sys = System::new();
    let mounts = match sys.mounts() {
        Ok(mounts) => mounts,
        Err(x) => {
            error!("fs mounts error: {}", x);
            process::exit(1)
        }
    };

    info!("init volumes");

    let mut volumes: Vec<Volume> = vec![];
    for volume_path in config.storage.volumes.iter() {
        let mut volume = Volume::new(volume_path);
        match volume.bootstrap(&mounts, config.storage.bucket_size_limit_bytes) {
            Ok(_v) => volumes.push(volume),
            Err(x) => {
                error!("volume init: {}", x);
                process::exit(1)
            }
        }
    }
    for volume in volumes.iter() {
        match validate_volumes(volume, &volumes) {
            Ok(_) => (),
            Err(x) => {
                error!("wrong volume {}: {}", volume.path, x);
                process::exit(1)
            }
        }
    }
    volumes
}

fn validate_volumes(volume: &Volume, volumes: &Vec<Volume>) -> Result<bool, String> {
    for v in volumes.iter() {
        if v.path != volume.path && v.dev == volume.dev {
            return Err(format!(
                "duplicated dev: {} in {} and {}",
                v.dev, v.path, volume.path
            ));
        }
    }
    Ok(true)
}

pub fn overwrite_config_with_cmd_args(_config: &mut Config, matches: &ArgMatches<'_>) {
    //    if let Some(addr) = matches.value_of("addr") {
    //        config.server.addr = addr.to_owned();
    //    }
    //
    //    if let Some(data_dir) = matches.value_of("data-dir") {
    //        config.storage.data_dir = data_dir.to_owned();
    //    }
    //
    if let Some(tags_vec) = matches.values_of("tags") {
        let mut tags = HashMap::default();
        tags_vec
            .map(|s| {
                let mut parts = s.split('=');
                let key = parts.next().unwrap().to_owned();
                let value = match parts.next() {
                    None => panic!("invalid tag: {}", s),
                    Some(v) => v.to_owned(),
                };
                if parts.next().is_some() {
                    panic!("invalid tag: {}", s);
                }
                tags.insert(key, value);
            })
            .count();
        //        config.tags = tags;
    }
    info!("cli args applied")
}
