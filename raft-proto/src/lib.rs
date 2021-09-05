pub use uuid::Uuid;

pub type Term = usize;

//#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub type NodeId = Uuid;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct LogItem(pub u32);

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
    pub term: Term,
    pub item: LogItem
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ServerDebugReport {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub last_log_index: usize,
    pub last_log_term : Option<Term>,
    pub log: Vec<LogEntry>,

    pub commit_index: usize,
    pub last_applied: usize,

    pub role: String,

    pub election_ticks_before_timeout: u16,

    pub last_leader_id: Option<NodeId>
}

#[tarpc::service]
pub trait RaftService {
    async fn append_entries(term: Term, leader_id: NodeId, 
        prev_log_index: usize, prev_log_term: Term,
        entries: Vec<LogEntry>, leader_commit: usize) -> (Term, bool);

    async fn request_vote(term: Term, candidate_id: NodeId,
        last_log_index: usize, last_log_term: Term) -> (Term, bool);

    async fn append_log_entry(item: LogItem);

    async fn debug_report() -> ServerDebugReport;
    async fn shutdown();
}

use std::{collections::HashMap, net::SocketAddr};
use tokio::sync::RwLock;
use anyhow::Context;

pub struct ClusterConfig {
    pub self_id: NodeId,
    pub addresses: HashMap<NodeId, SocketAddr>,

    clients: RwLock<HashMap<NodeId, RaftServiceClient>>
}

impl ClusterConfig {
    pub async fn from_disk(self_id: NodeId) -> anyhow::Result<ClusterConfig> {
        let addrs: HashMap<NodeId, SocketAddr>
            = serde_json::from_reader(
                std::fs::File::open("cluster.json")
                    .context("open cluster config file")?
                ).context("parse cluster config file")?;
        let mut clients = HashMap::new();
        for (id, addr) in addrs.iter() {
            match tarpc::serde_transport::tcp::connect(addr, tokio_serde::formats::Json::default).await {
                Ok(transport) => { clients.insert(*id, RaftServiceClient::new(tarpc::client::Config::default(), transport).spawn()); }
                Err(e) => log::error!("tried to connect to {}@{} on startup but failed: {}", id, addr, e)
            }
        }
        Ok(ClusterConfig {
            self_id, addresses: addrs, clients: RwLock::new(clients)
        })
    }

    pub async fn get_client(&self, id: &NodeId) -> anyhow::Result<RaftServiceClient> {
        let clients = self.clients.read().await;
        if let Some(client) = clients.get(id) {
            Ok(client.clone())
        } else {
            std::mem::drop(clients);
            let mut clients = self.clients.write().await;
            let addr = self.addresses.get(id).ok_or_else(|| anyhow::anyhow!("don't have address for node {}", id))?;
            let mut retries = 0;
            while retries < 5 {
                match tarpc::serde_transport::tcp::connect(addr, tokio_serde::formats::Json::default).await {
                    Ok(transport) => {
                        let client = RaftServiceClient::new(tarpc::client::Config::default(), transport).spawn();
                        clients.insert(*id, client.clone());
                        return Ok(client);
                    }
                    Err(e) => log::error!("tried to connect to {}@{} on startup but failed, retrying (attempt {}): {}", id, addr, retries, e)
                }
                retries+=1;
                tokio::time::sleep(std::time::Duration::from_millis(retries*2)).await;
            }
            Err(anyhow::anyhow!("could not connect to {}@{}, retries exhausted", id, addr))
        }
    }

    pub async fn reset_client(&self, id: &NodeId) {
        let mut clients = self.clients.write().await;
        clients.remove(id);
    }
}


