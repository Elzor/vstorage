use serde_derive::{Deserialize, Serialize};
use std::{error};

type Result<T> = std::result::Result<T, Box<dyn error::Error>>;

pub trait RestPath<U> {
    fn get_path(param: U) -> Result<String>;
}

#[derive(Deserialize, Clone, Debug)]
pub struct Pong {
    pub pong: bool,
}
impl RestPath<bool> for Pong {
    fn get_path(_ping: bool) -> Result<String> {
        Ok("/ping".to_string())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Server {
    pub nodename: String,
    pub zone: String,
    pub rack: String,
    pub endpoint: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerRegistrationRequest {
    pub server: Server,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerRegistrationResponse {
    pub success: bool,
    pub code: u32,
}
impl RestPath<ServerRegistrationRequest> for ServerRegistrationResponse {
    fn get_path(_param: ServerRegistrationRequest) -> Result<String> {
        Ok("/v1/server/register".to_string())
    }
}