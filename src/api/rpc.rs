
use tonic::{transport::Server, Request, Response, Status};

use hello_world::greeter_server::{Greeter, GreeterServer};
use hello_world::{HelloReply, HelloRequest};
use std::net::{SocketAddr, ToSocketAddrs};

pub mod hello_world {
    tonic::include_proto!("helloworld"); // The string specified here must match the proto package name
}

#[derive(Debug, Default)]
pub struct MyGreeter {}

#[tonic::async_trait]
impl Greeter for MyGreeter {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        println!("Got a request: {:?}", request);

        let reply = hello_world::HelloReply {
            message: format!("Hello {}!", request.into_inner().name).into(),
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
            let greeter = MyGreeter::default();

            let _ = Server::builder()
                .add_service(GreeterServer::new(greeter))
                .serve(endpoint)
                .await;
        });
    }
}