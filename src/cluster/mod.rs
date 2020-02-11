use std::sync::RwLock;
use std::{error};
use tokio::time;
use vm_util::collections::HashMap;
use crate::cluster::coordinator::Coordinator;

pub mod coordinator;
pub mod coordinator_api;

lazy_static! {
    pub static ref CLUSTER: RwLock<Cluster> = RwLock::new(Cluster::new());
}

type Result<T> = std::result::Result<T, Box<dyn error::Error>>;

pub struct Cluster {
    pub coordinators: HashMap<String, coordinator::Coordinator>,
    pub coordinator: Option<String>,
}

impl Cluster {
    pub fn new() -> Cluster {
        Cluster {
            coordinators: HashMap::new(),
            coordinator: None,
        }
    }

    pub fn add_coordinator(&mut self, endpoint: String) -> Result<bool> {
        let new_coordinator = coordinator::Coordinator::new(endpoint.to_owned());
        self.coordinators.insert(
            new_coordinator.endpoint.to_lowercase(),
            new_coordinator,
        );
        if self.coordinators.len() == 1 {
            self.coordinator = Some(endpoint)
        }
        Ok(true)
    }

    pub fn get_coordinator(&self) -> Result<Coordinator> {
        let current_coordinator = self.coordinator.clone().unwrap_or("".to_string());
        match self.coordinators.get(&current_coordinator) {
            Some(c) => {
                Ok(c.to_owned())
            }
            _ => {
                for (_, c) in self.coordinators.iter() {
                    if c.active {
                        return Ok(c.to_owned());
                    }
                }
                Err(Box::new(coordinator::NoCoordinators))
            }
        }
    }

    pub fn get_coordinators(&self) -> Result<Vec<coordinator::Coordinator>> {
        let mut v = Vec::new();
        for (_, c) in self.coordinators.iter() {
            v.push(c.to_owned());
        }
        Ok(v)
    }

    pub fn coordinator_active(&mut self, endpoint: String, active: bool) -> Result<bool> {
        match active {
            true => {
                match self.coordinator {
                    None => { self.coordinator = Some(endpoint.clone()); }
                    _ => {}
                }
                match self.coordinators.get_mut(&endpoint.to_lowercase()) {
                    Some(c) => {
                        c.active = true;
                    }
                    _ => {
                        dbg!(&self.coordinators);
                        error!("coordinator not found: {}", endpoint);
                    }
                };
            }
            false => {
                match self.coordinators.get_mut(&endpoint.to_lowercase()) {
                    Some(c) => {
                        c.active = false;
                        if self.coordinator.is_some() && self.coordinator.as_ref().unwrap().eq(&c.endpoint) {
                            self.coordinator = None;
                            for (_, coord) in self.coordinators.iter() {
                                if coord.active {
                                    self.coordinator = Some(coord.endpoint.clone());
                                    break;
                                }
                            }
                        };
                    }
                    _ => {
                        error!("coordinator not found: {}", endpoint);
                    }
                };
            }
        }
        Ok(true)
    }
}

pub fn health_check() -> Result<bool> {
    let checker = async {
        let mut interval = time::interval(std::time::Duration::from_secs(1));
        interval.tick().await;
        loop {
            let coords = {
                CLUSTER.read().unwrap().get_coordinators().unwrap().clone()
            };
            for mut c in coords {
                let coord_endpoint = c.endpoint.clone();
                match c.ping().await {
                    Ok(_) => {
                        let _ = CLUSTER.write().unwrap().coordinator_active(coord_endpoint, true);
                    }
                    Err(e) => {
                        let _ = CLUSTER.write().unwrap().coordinator_active(coord_endpoint.clone(), false);
                        error!("coordinator {}: {}", coord_endpoint, e);
                    }
                };
            }
            interval.tick().await;
        };
    };
    tokio::spawn(checker);
    Ok(true)
}