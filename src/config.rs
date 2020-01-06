use std::error::Error;
use std::fs;
use std::path::Path;
use std::io::Error as IoError;
use std::io::Write;
use serde_json::{Value, Map};
use serde::{Serialize, Deserialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub node: Node,
    pub interfaces: Interfaces,
    pub db: Db,
    pub storage: Storage,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct Storage {
    pub block_size_limit_bytes: u64,
    pub bucket_size_limit_bytes: u64,
    pub volumes: Vec<String>,
    pub gc_timeout_sec: u32,
    pub gc_batch:u32,
}

fn default_storage() -> Storage {
    Storage{
        block_size_limit_bytes: 10*1024*1024,
        volumes: default_volumes(),
        bucket_size_limit_bytes: 1073741824,
        gc_timeout_sec: 1,
        gc_batch: 1000,
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct Node {
    pub nodename: String,
    pub zone: String,
    pub work_dir: String,
    pub pid_file: String,
    pub ctl_socket_file: String,
    pub logger_config: String,
    pub opts: Map<String, Value>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct Interfaces {
    pub internal: String,
    pub public: String,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct Db {
    pub meta_db_path: String,
    pub meta_db_backup_path: String,
    pub size_calculation_interval_min: i32,
}

fn default_node() -> Node {
    Node {
        nodename: "dev1".to_string(),
        zone: "default".to_string(),
        work_dir: "./info/temp".to_string(),
        pid_file: "/tmp/sblock_server.pid".to_string(),
        ctl_socket_file: "/tmp/sblock_ctl.sock".to_string(),
        logger_config: "sblock_logger.yml".to_string(),
        opts: default_node_opts(),
    }
}

fn default_node_opts() -> Map<String, Value> {
    let mut opts = Map::new();
    opts.insert("mode".to_string(), Value::String("default".to_string()));
    opts
}

fn default_interfaces() -> Interfaces {
    Interfaces {
        public: "127.0.0.1:33088".to_string(),
        internal: "127.0.0.1:33087".to_string(),
    }
}

fn default_volumes() -> Vec<String> {
    vec!["./info/data".to_string()]
}

fn default_db() -> Db {
    Db {
        meta_db_path: "./info/meta".to_string(),
        meta_db_backup_path: "./info/meta_backup".to_string(),
        size_calculation_interval_min: 60,
    }
}

impl Default for Config {
    fn default() -> Config {
        Config {
            node: default_node(),
            interfaces: default_interfaces(),
            db: default_db(),
            storage: default_storage(),
        }
    }
}

impl Config {
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> Self {
        (|| -> Result<Self, Box<dyn Error>> {
            let s = fs::read_to_string(&path)?;
            Ok(serde_yaml::from_str(&s)?)
        })().unwrap_or_else(|e| {
            panic!("invalid auto generated configuration file {}, err {}", path.as_ref().display(), e);
        })
    }

    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), IoError> {
        let content = serde_yaml::to_string(&self).unwrap();
        let mut f = fs::File::create(&path)?;
        f.write_all(content.as_bytes())?;
        f.sync_all()?;

        Ok(())
    }

    /// Prevents launching with an incompatible configuration
    pub fn check_critical_params(&self) -> Result<(), Box<dyn Error>> {
//        if self.logger_config == "" {
//            return Err(format!(
//                "logger_config can't be empty"
//            ).into());
//        }
//        if self.server_script == "" {
//            return Err(format!(
//                "server_script can't be empty"
//            ).into());
//        }
        Ok(())
    }
}