use anyhow::{Result, Context};
use std::collections::HashMap;

pub type Term = usize;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct NodeId(usize);

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
    term: Term
}

#[derive(Debug)]
pub enum ProtocolRole {
    Follower,
    Candidate,
    Leader {
        follower_indices: HashMap<NodeId, (usize, usize)>
    }
}

impl Default for ProtocolRole {
    fn default() -> ProtocolRole {
        ProtocolRole::Follower
    }
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct State {
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,

    #[serde(skip)]
    commit_index: usize,
    #[serde(skip)]
    last_applied: usize,

    #[serde(skip)]
    role: ProtocolRole
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
}

#[tarpc::service]
pub trait RaftService {
    async fn append_entries(term: Term, leader_id: NodeId, 
        prev_log_index: usize, prev_log_term: Term,
        entries: Vec<LogEntry>, leader_commit: usize) -> (Term, bool);

    async fn request_vote(term: Term, candidate_id: NodeId,
        last_log_index: usize, last_log_term: Term) -> (Term, bool);
}

