use anyhow::{Result, Context};
use std::{collections::HashMap, net::SocketAddr};
use tokio::sync::RwLock;
pub use uuid::Uuid;

pub type Term = usize;

//#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub type NodeId = Uuid;

pub struct ClusterConfig {
    pub self_id: NodeId,
    pub addresses: HashMap<NodeId, SocketAddr>,

    clients: RwLock<HashMap<NodeId, RaftServiceClient>>
}

impl ClusterConfig {
    pub async fn from_disk(self_id: NodeId) -> Result<ClusterConfig> {
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

    pub async fn get_client(&self, id: &NodeId) -> Result<RaftServiceClient> {
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


#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct LogItem(u32);

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
    pub term: Term,
    pub item: LogItem
}

#[derive(Debug)]
pub enum ProtocolRole {
    Follower,
    Candidate(Option<tokio::task::JoinHandle<()>>),
    Leader {
        follower_indices: HashMap<NodeId, (usize, usize)>
    }
}

impl Default for ProtocolRole {
    fn default() -> ProtocolRole {
        ProtocolRole::Follower
    }
}

pub fn default_election_timeout() -> u16 {
    use rand::Rng;
    rand::thread_rng().gen_range(10..30)
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct State {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,

    #[serde(skip)]
    pub commit_index: usize,
    #[serde(skip)]
    pub last_applied: usize,

    #[serde(skip)]
    pub role: ProtocolRole,

    #[serde(skip)]
    #[serde(default = "default_election_timeout")]
    pub election_ticks_before_timeout: u16,

    #[serde(skip)]
    pub last_leader_id: Option<NodeId>
}

impl Default for State {
    fn default() -> Self {
        State {
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            role: ProtocolRole::default(),
            election_ticks_before_timeout: default_election_timeout(),
            last_leader_id: None
        }
    }
}

impl State {
    /// read the persisted node state from disk
    pub fn from_disk() -> Result<State> {
        match std::fs::File::open("node-state.json") {
            Ok(f) => serde_json::from_reader(f).context("deserialize node state from disk"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(State::default()),
            Err(e) => Err(anyhow::anyhow!(e))
        }
    }

    /// write the persisted node state to disk
    /// if the write fails, panic
    pub fn persist(&self) {
        std::fs::File::create("node-state.json")
            .map_err(Into::into)
            .and_then(|f| serde_json::to_writer(f, self).context("serialize node state to disk"))
            .expect("persist node state to disk")
    }

    pub fn start_election(&mut self, self_id: NodeId) {
        assert_eq!(self.election_ticks_before_timeout, 0);
        log::debug!("election timeout reached, starting election");
        self.current_term += 1;
        self.voted_for = Some(self_id);
        self.election_ticks_before_timeout = default_election_timeout();
    }

    pub fn clone_follower_indices(&self) -> Option<HashMap<NodeId, (usize, usize)>> {
        match &self.role {
            ProtocolRole::Leader { follower_indices } => Some(follower_indices.clone()),
            _ => None
        }
    }

    pub fn set_follower_indices(&mut self, fi: HashMap<NodeId, (usize,usize)>) {
        match &mut self.role {
            ProtocolRole::Leader { follower_indices } => *follower_indices = fi,
            _ => panic!()
        }
    }

    /* Indices in Raft start at 1! */
    pub fn log_entry(&self, index: usize) -> Option<&LogEntry> {
        if index == 0 { return None; }
        self.log.get(index - 1)
    }

    pub fn last_log_index(&self) -> usize {
        self.log.len()
    }

    pub fn last_log_term(&self) -> Option<Term> {
        self.log.last().map(|e| e.term)
    }
}

#[tarpc::service]
pub trait RaftService {
    async fn append_entries(term: Term, leader_id: NodeId, 
        prev_log_index: usize, prev_log_term: Term,
        entries: Vec<LogEntry>, leader_commit: usize) -> (Term, bool);

    async fn request_vote(term: Term, candidate_id: NodeId,
        last_log_index: usize, last_log_term: Term) -> (Term, bool);

    async fn append_log_entry(item: LogItem);
}
