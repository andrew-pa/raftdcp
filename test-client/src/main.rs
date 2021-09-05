
use anyhow::{Result};

use raft_proto::*;

#[tokio::main]
async fn main() {
    env_logger::init();
    let addr: std::net::SocketAddr = "127.0.0.1:5551".parse().unwrap();
    let client = tarpc::serde_transport::tcp::connect(addr, tokio_serde::formats::Json::default).await
        .map(|transport| RaftServiceClient::new(tarpc::client::Config::default(), transport).spawn()).unwrap();
    client.append_log_entry(tarpc::context::current(), LogItem(3)).await.unwrap();
}
