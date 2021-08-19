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
pub struct RaftState {
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

#[tarpc::service]
pub trait RaftService {
    async fn append_entries(term: Term, leader_id: NodeId, 
        prev_log_index: usize, prev_log_term: Term,
        entries: Vec<LogEntry>, leader_commit: usize) -> (Term, bool);

    async fn request_vote(term: Term, candidate_id: NodeId,
        last_log_index: usize, last_log_term: Term) -> (Term, bool);
}

