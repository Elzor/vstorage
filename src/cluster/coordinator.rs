use std::{error, fmt, process};

use bytes::buf::ext::BufExt;
use crate::cluster::coordinator_api;
use hyper::{Body, Client, Request};

type Result<T> = std::result::Result<T, Box<dyn error::Error>>;

#[derive(Debug, Clone)]
pub struct NoCoordinators;

impl fmt::Display for NoCoordinators {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "no active coordinator")
    }
}

impl error::Error for NoCoordinators {
    fn description(&self) -> &str {
        "no active coordinator"
    }
    fn cause(&self) -> Option<&error::Error> {
        None
    }
}


#[derive(Debug, Clone, PartialEq)]
pub struct Coordinator {
    pub endpoint: String,
    pub active: bool,
}

impl Coordinator {
    pub fn new(endpoint: String) -> Coordinator {
        match endpoint.parse() {
            Ok(enp) => {
                let enp: hyper::Uri = enp;
                let scheme = match enp.scheme() {
                    Some(s) => s.as_str(),
                    _ => "http",
                };
                Coordinator {
                    endpoint: format!("{}://{}", scheme, enp.to_string()),
                    active: true,
                }
            }
            Err(e) => {
                error!("can't parse coordinator's endpoint: {}", e);
                process::exit(1);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    pub async fn ping(&mut self) -> Result<bool> {
        let pong: coordinator_api::Pong = self.get(true).await?;
        Ok(pong.pong)
    }

    pub async fn register(&mut self, server: coordinator_api::Server) -> Result<bool> {
        let req = coordinator_api::ServerRegistrationRequest {server};
        let registred: coordinator_api::ServerRegistrationResponse =
            self.post(&req).await?;
        dbg!(registred);
        Ok(false)
    }

    // ---------------------------------------------------------------------------------------------
    async fn get<U, T>(&mut self, params: U) -> Result<T> where
        T: serde::de::DeserializeOwned + coordinator_api::RestPath<U> {
        let client = Client::new();
        let t_path = T::get_path(params)?;
        let uri = format!("{}{}", self.endpoint, t_path);
        let res = client.get(uri.parse().unwrap()).await?;
        let body = hyper::body::aggregate(res).await?;
        let res = serde_json::from_reader(body.reader())?;
        Ok(res)
    }

    async fn post<U, T>(&mut self, params: &U) -> Result<T> where
        T: serde::de::DeserializeOwned + coordinator_api::RestPath<U>, U: serde::Serialize + Copy {
        let client = Client::new();
        let t_path = T::get_path(*params)?;
        let uri = format!("{}{}", self.endpoint, t_path);
        let req = Request::builder()
            .method("POST")
            .uri(uri)
            .body(Body::from(serde_json::to_string(&params)?))
            .expect("request built");
        let res = client.request(req).await?;
        let body = hyper::body::aggregate(res).await?;
        let res = serde_json::from_reader(body.reader())?;
        Ok(res)
    }

    // ---------------------------------------------------------------------------------------------
}