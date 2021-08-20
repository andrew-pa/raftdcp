use anyhow::{Result, Context};
use std::{collections::HashMap, net::SocketAddr};
pub use uuid::Uuid;

pub type Term = usize;

//#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub type NodeId = Uuid;

pub struct ClusterConfig {
    pub self_id: NodeId,
    pub addresses: HashMap<NodeId, SocketAddr>,

    pub clients: HashMap<NodeId, RaftServiceClient>
}

impl ClusterConfig {
    pub async fn from_disk(self_id: NodeId) -> Result<ClusterConfig> {
        let addrs: HashMap<NodeId, SocketAddr>
            = serde_json::from_reader(std::fs::File::open("cluster.json")?)?;
        let mut clients = HashMap::new();
        for (id, addr) in addrs.iter() {
            let transport = tarpc::serde_transport::tcp::connect(addr, tokio_serde::formats::Json::default).await?;
            clients.insert(*id, RaftServiceClient::new(tarpc::client::Config::default(), transport).spawn());
        }
        Ok(ClusterConfig {
            self_id, addresses: addrs, clients
        })
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
    pub term: Term
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

pub fn default_election_timeout() -> u16 { 50 }

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
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

    pub fn follower_indices(&mut self) -> Option<&mut HashMap<NodeId, (usize, usize)>> {
        match &mut self.role {
            ProtocolRole::Leader { follower_indices } => Some(follower_indices),
            _ => None
        }
    }

    /*pub async fn hold_election(&mut self, cluster: &ClusterConfig) {
        use futures::stream::*;
        self.election_ticks_before_timeout = default_election_timeout();
        self.current_term += 1;
        self.voted_for = Some(cluster.self_id);
        let mut votes = 0;
        let mut futures: futures::stream::FuturesUnordered<_> =
            cluster.clients.iter().map(|(_, cl)| async move {
                cl.request_vote(tarpc::context::current(), self.current_term,
                    cluster.self_id, self.log.len(), self.log.last().map(|e| e.term).unwrap_or(0))
            }).collect();
        /*{
            match res {
                Ok((term, vote_granted)) => {
                    if term > self.current_term {
                        self.current_term = term;
                        self.role = ProtocolRole::Follower;
                        return;
                    }
                    if vote_granted { votes += 1; }
                },
                Err(e) => log::error!("error recieving vote from node: {}", e)
            }
        }*/
    }*/
}

#[tarpc::service]
pub trait RaftService {
    async fn append_entries(term: Term, leader_id: NodeId, 
        prev_log_index: usize, prev_log_term: Term,
        entries: Vec<LogEntry>, leader_commit: usize) -> (Term, bool);

    async fn request_vote(term: Term, candidate_id: NodeId,
        last_log_index: usize, last_log_term: Term) -> (Term, bool);
}

