use anyhow::{Result, Context};
use std::{collections::HashMap, net::SocketAddr};
use tokio::sync::RwLock;

pub use raft_proto::*;

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
