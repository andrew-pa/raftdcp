use anyhow::Result;
use futures::prelude::*;
use std::{sync::Arc, time::Duration};
use tarpc::{context::Context as RpcContext, server::Channel};
use tokio::sync::{Mutex, RwLock};

mod common;
use common::*;
mod resetable_interval;

#[derive(Clone)]
struct RaftServer(Arc<RwLock<State>>, Arc<ClusterConfig>);

#[tarpc::server]
impl RaftService for RaftServer {
    async fn append_entries(
        self,
        _: RpcContext,
        term: Term,
        leader_id: NodeId,
        prev_log_index: usize,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: usize,
    ) -> (Term, bool) {
        let state = self.0.read().await;
        state.persist();
        if term < state.current_term {
            return (state.current_term, false);
        }
        if state.log.get(prev_log_index).map(|e| e.term != prev_log_term).unwrap_or(true)
        {
            return (state.current_term, false);
        }
        std::mem::drop(state);
        let mut state = self.0.write().await;
        if term > state.current_term {
            state.current_term = term;
            state.role = ProtocolRole::Follower;
        }
        state.election_ticks_before_timeout = default_election_timeout();
        state.last_leader_id = Some(leader_id);
        match &mut state.role {
            ProtocolRole::Follower => { }
            ProtocolRole::Candidate(election_task) => {
                election_task.take().unwrap().abort();
                state.role = ProtocolRole::Follower;
            }
            ProtocolRole::Leader { .. } => {
                log::warn!("got append_entries RPC from a node as leader. current term = {}; sending node id = {}, their term index = {}", state.current_term, leader_id, term);
                return (state.current_term, false);
            }
        }
        let next_log_index = prev_log_index+1;
        while next_log_index < state.log.len() {
            if state.log[next_log_index].term != entries[next_log_index-prev_log_index-1].term {
                state.log.truncate(next_log_index);
            }
        }
        state.log.extend_from_slice(&entries[next_log_index-prev_log_index-1..]);
        if leader_commit > state.commit_index {
            state.commit_index = leader_commit.min(/* index of the last new log entry ... from entries[], or from log[]?*/0);
        }
        (state.current_term, true)
    }

    async fn request_vote(
        self,
        _: RpcContext,
        term: Term,
        candidate_id: NodeId,
        last_log_index: usize,
        last_log_term: Term,
    ) -> (Term, bool) {
        let mut state = self.0.write().await;
        state.persist();
        if term < state.current_term {
            return (state.current_term, false);
        }
        state.election_ticks_before_timeout = default_election_timeout();
        let logs_match = todo!();
        if state.voted_for.map(|id| id == candidate_id).unwrap_or(true) && logs_match {
            state.current_term = term;
            (state.current_term, true)
        } else {
            (state.current_term, false)
        }
    }
}

fn apply_to_state_machine(e: &LogEntry) {
    log::debug!("applying entry to state machine: {:?}", e);
}

async fn hold_election(state: Arc<RwLock<State>>, cluster: Arc<ClusterConfig>) {
    let mut votes_recieved = 0;
    let (term, last_log_index, last_log_term) = {
        let state = state.read().await;
        (state.current_term, state.log.len(), state.log.last().map_or(0, |e| e.term))
    };

    let mut futures: Vec<_> = cluster.clients.iter()
        .map(|(id, cl)|
            cl.request_vote(tarpc::context::current(), term, cluster.self_id, last_log_index, last_log_term)
                .then(move |res| future::ready((id, res))).boxed())
        .collect();
    loop {
        let mut should_become_follower = None;
        for (node_id, res) in future::join_all(std::mem::replace(&mut futures, Vec::new())).await {
            match res {
                Ok((nterm, vote_granted)) => {
                    if nterm > term {
                        should_become_follower = Some(nterm);
                    }
                    if vote_granted {
                        votes_recieved += 1;
                    }
                },
                Err(e) => {
                    log::error!("requesting vote from node {} failed: {}, retrying", node_id, e);
                    futures.push(cluster.clients[node_id].request_vote(
                            tarpc::context::current(), term, cluster.self_id, last_log_index, last_log_term)
                        .then(move |res| future::ready((node_id, res))).boxed());
                }
            }
        }
        if votes_recieved >= cluster.addresses.len()/2 {
            become_leader(state.clone(), cluster.clone());
            break;
        }
        {
            let mut state = state.write().await;
            if let Some(term) = should_become_follower {
                state.current_term = term;
                state.role = ProtocolRole::Follower;
            }
            if let ProtocolRole::Follower = state.role {
                break;
            }
        }
        if futures.len() == 0 { break; }
    }
}

async fn become_leader(state: Arc<RwLock<State>>, cluster: Arc<ClusterConfig>) {
    {
        let mut state = state.write().await;
        log::info!("becoming leader, term = {}", state.current_term);
        state.role = ProtocolRole::Leader {
            follower_indices: cluster.addresses.iter().map(|(id, _)| (*id, (state.log.len(), 0usize))).collect()
        };
    }
    leader_update(state, cluster).await
}

async fn leader_update(state: Arc<RwLock<State>>, cluster: Arc<ClusterConfig>) {
    log::trace!("leader update");
    let mut state = state.write().await;
    for (id, (next_index, match_index)) in state.follower_indices().expect("leader update called on leader nodes").iter_mut() {
        if state.log.len()-1 >= *next_index {
            match cluster.clients[id].append_entries(tarpc::context::current(), state.current_term, cluster.self_id,
                prev_log_index, prev_log_term, entries, state.commit_index).await 
            {
                Ok((term, success)) => {
                    if term > state.current_term {
                        state.current_term = term;
                        state.role = ProtocolRole::Follower;
                        return;
                    }
                    if success {
                        // update indices
                    } else {
                        *next_index -= 1;
                        // we'll get this node on the next update tick
                    }
                },
                Err(e) => {
                    log::error!("failed to send append entries RPC to {}: {}", id, e);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    log::trace!("starting process");

    let self_id = std::env::args()
        .nth(1)
        .expect("node id")
        .parse::<Uuid>()
        .unwrap();
    log::trace!("connecting to cluster");
    let cluster = Arc::new(ClusterConfig::from_disk(self_id).await?);

    log::trace!("loading persistent state");
    let state = Arc::new(RwLock::new(State::from_disk()?));

    let et_state = state.clone();
    let et_clu = cluster.clone();
    log::trace!("spawning tick stream");
    let tick_task = tokio::task::spawn(async move {
        tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(Duration::from_millis(35)))
        .for_each(|t| {
            let et_state = et_state.clone();
            let et_clu = et_clu.clone();
            async move {
                log::trace!("tick {:?}", t);
                let mut state = et_state.write().await;
                if state.commit_index < state.last_applied {
                    state.last_applied += 1;
                    apply_to_state_machine(&state.log[state.last_applied]);
                }
                match &mut state.role {
                    ProtocolRole::Follower => {
                        state.election_ticks_before_timeout -= 1;
                        if state.election_ticks_before_timeout == 0 {
                            state.start_election(et_clu.self_id);
                            state.role = ProtocolRole::Candidate(Some(tokio::task::spawn(hold_election(et_state.clone(), et_clu))));
                        }
                    },
                    ProtocolRole::Candidate(election_task) => {
                        election_task.take().unwrap().abort();
                        state.election_ticks_before_timeout -= 1;
                        if state.election_ticks_before_timeout == 0 {
                            state.start_election(et_clu.self_id);
                            state.role = ProtocolRole::Candidate(Some(tokio::task::spawn(hold_election(et_state.clone(), et_clu))));
                        }
                    }
                    ProtocolRole::Leader { .. } => { std::mem::drop(state); leader_update(et_state.clone(), et_clu).await }
                }
            }
        }).await
    });

    let mut listener = tarpc::serde_transport::tcp::listen(
        cluster.addresses[&self_id],
        tokio_serde::formats::Json::default,
    ).await?;
    listener.config_mut().max_frame_length(usize::MAX);

    log::info!(
        "listening on {} as {}!",
        cluster.addresses[&self_id],
        self_id
    );
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
            let server = RaftServer(state.clone(), cluster.clone());
            channel.execute(server.serve())
        })
        .buffer_unordered(32)
        .for_each(|_| async {})
        .await;

    tick_task.abort();
    tick_task.await;

    Ok(())
}
