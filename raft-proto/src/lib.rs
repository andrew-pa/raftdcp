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

#[tarpc::service]
pub trait RaftService {
    async fn append_entries(term: Term, leader_id: NodeId, 
        prev_log_index: usize, prev_log_term: Term,
        entries: Vec<LogEntry>, leader_commit: usize) -> (Term, bool);

    async fn request_vote(term: Term, candidate_id: NodeId,
        last_log_index: usize, last_log_term: Term) -> (Term, bool);

    async fn append_log_entry(item: LogItem);
}
