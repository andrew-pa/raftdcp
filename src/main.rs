use anyhow::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use futures::prelude::*;
use tarpc::{server::Channel, context::Context as RpcContext};

mod common;
use common::*;

#[derive(Clone)]
struct RaftServer(Arc<RwLock<RaftState>>);

#[tarpc::server]
impl RaftService for RaftServer {
    async fn append_entries(self, _: RpcContext, term: Term, leader_id: NodeId, 
        prev_log_index: usize, prev_log_term: Term,
        entries: Vec<LogEntry>, leader_commit: usize) -> (Term, bool)
    {
        (0, false)
    }

    async fn request_vote(self, _: RpcContext, term: Term, candidate_id: NodeId,
        last_log_index: usize, last_log_term: Term) -> (Term, bool)
    {
        (0, false)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    log::trace!("starting process");

    let server_address = std::env::args().nth(1).expect("provide address to listen on")
        .parse::<std::net::SocketAddr>().unwrap();
    
    let state = Arc::new(RwLock::new(RaftState::default()));

    let mut listener = tarpc::serde_transport::tcp::listen(
        server_address, tokio_serde::formats::Json::default).await?;
    listener.config_mut().max_frame_length(usize::MAX);

    log::info!("listening on {}!", server_address);
    listener
        .filter_map(|r| {
            if let Some(e) = r.as_ref().err() {
                log::error!("error listening for requests: {}", e);
            }
            future::ready(r.ok())
        })
        .map(tarpc::server::BaseChannel::with_defaults)
        .map(|channel| {
            log::trace!("creating server instance");
            let server = RaftServer(state.clone());
            channel.execute(server.serve())
        })
        .buffer_unordered(32)
        .for_each(|_| async {})
        .await;

    Ok(())
}
