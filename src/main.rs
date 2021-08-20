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
        log::trace!("append_entries(term: {}, leader_id: {}, prev_log_index: {}, prev_log_term: {}, entries: {:?}, leader_commit: {}",
            term, leader_id, prev_log_index, prev_log_term, entries, leader_commit);
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
            state.commit_index = leader_commit.min(state.log.len()-1 /* last new entry index */);
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
        log::trace!("request_vote(term: {}, candidate_id: {}, last_log_index: {}, last_log_term: {})",
            term, candidate_id, last_log_index, last_log_term);
        if term < state.current_term {
            return (state.current_term, false);
        }
        state.election_ticks_before_timeout = default_election_timeout();
        let logs_match = state.log.last().map_or(true /*how do empty logs match?*/, |e| {
            if e.term == last_log_term { 
                state.log.len()-1 > last_log_index
            } else {
                e.term > last_log_term
            }
        });
        log::trace!("logs_match = {}", logs_match);
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
    let self_id = cluster.self_id;

    let mut futures: Vec<_> = cluster.addresses.iter()
        .map(|(id, _)| cluster.get_client(id).then(move |c| async move {
            (id, match c {
                Ok(cl) => cl.request_vote(tarpc::context::current(), term, self_id, last_log_index, last_log_term)
                                .await.map_err(Into::into),
                Err(e) => Err(e)
            })
        }).boxed())
        .collect();
    loop {
        let mut should_become_follower = None;
        log::trace!("requesting votes {}", futures.len());
        for (node_id, res) in future::join_all(std::mem::replace(&mut futures, Vec::new())).await {
            match res {
                Ok((nterm, vote_granted)) => {
                    log::trace!("got vote result: {}", vote_granted);
                    if nterm > term {
                        should_become_follower = Some(nterm);
                    }
                    if vote_granted {
                        votes_recieved += 1;
                    }
                },
                Err(e) => {
                    log::error!("requesting vote from node {} failed: {}, retrying", node_id, e);
                    futures.push(cluster.get_client(node_id).then(move |c| async move {
                        (node_id, match c {
                            Ok(cl) => cl.request_vote(tarpc::context::current(), term, self_id, last_log_index, last_log_term)
                                .await.map_err(Into::into),
                            Err(e) => Err(e)
                        })
                    }).boxed());
                }
            }
        }
        log::trace!("recieved {} votes so far", votes_recieved);
        if votes_recieved >= cluster.addresses.len()/2 {
            become_leader(state.clone(), cluster.clone()).await;
            return;
        }
        {
            let mut state = state.write().await;
            if let Some(term) = should_become_follower {
                state.current_term = term;
                state.role = ProtocolRole::Follower;
            }
            if let ProtocolRole::Follower = state.role {
                return;
            }
        }
        if futures.len() == 0 {
            break;
        }
    }
}

async fn become_leader(state: Arc<RwLock<State>>, cluster: Arc<ClusterConfig>) {
    {
        let mut state = state.write().await;
        log::info!("becoming leader, term = {}", state.current_term);
        state.role = ProtocolRole::Leader {
            follower_indices: cluster.addresses.iter().map(|(id, _)| (*id, (state.log.len()+1, 0usize))).collect()
        };
    }
    leader_update(state, cluster).await
}

async fn leader_update(state: Arc<RwLock<State>>, cluster: Arc<ClusterConfig>) {
    log::trace!("leader update");
    let mut state = state.write().await;
    let mut fi = state.clone_follower_indices().expect("leader update called on leader nodes");
    for (id, (next_index, match_index)) in fi.iter_mut() {
        //if state.log.len()-1 >= *next_index {
            let entries: Vec<_> = state.log[*next_index..].into();
            let prev_log_index = *next_index-1;
            let prev_log_term = state.log.get(*next_index-1).map_or(0, |e| e.term);
            match cluster.get_client(id).then(|c| async {
                match c {
                    Ok(cl) => cl.append_entries(tarpc::context::current(), state.current_term, cluster.self_id,
                                prev_log_index, prev_log_term, entries, state.commit_index).await.map_err(Into::into),
                    Err(e) => Err(e)
                }
            }).await
            {
                Ok((term, success)) => {
                    if term > state.current_term {
                        state.current_term = term;
                        state.role = ProtocolRole::Follower;
                        return;
                    }
                    if success {
                        // update indices
                        *next_index = state.log.len()+1;
                        *match_index = state.log.len();
                    } else {
                        *next_index -= 1;
                        // we'll get this node on the next update tick
                    }
                },
                Err(e) => {
                    log::error!("failed to send append entries RPC to {}: {}", id, e);
                }
            }
        //}
    }
    let mut n = state.commit_index+1;
    while n < state.log.len() {
        if state.log[n].term == state.current_term {
            let num_followers_match = fi.iter().fold(0,
                |count, (_, (_, match_index))| if *match_index >= n { count+1 } else { count });
            if num_followers_match >= cluster.addresses.len()/2 {
                state.commit_index = n;
                break;
            }
        }
        n+=1;
    }
    state.set_follower_indices(fi);
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    log::trace!("starting process");
    // use rand::Rng;
    // tokio::time::sleep(Duration::from_millis(rand::thread_rng().gen_range(0..100))).await;

    let self_id = std::env::args()
        .nth(1)
        .expect("node id")
        .parse::<Uuid>()
        .unwrap();
    log::trace!("connecting to cluster");
    let cluster = Arc::new(ClusterConfig::from_disk(self_id).await?);

    log::debug!("{:?}", std::fs::create_dir(self_id.to_string()));
    std::env::set_current_dir(self_id.to_string())?;

    log::trace!("loading persistent state");
    let state = Arc::new(RwLock::new(State::from_disk()?));

    let et_state = state.clone();
    let et_clu = cluster.clone();
    log::trace!("spawning tick stream");
    let tick_task = tokio::task::spawn(async move {
        tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(Duration::from_millis(1000)))
        .for_each(|t| {
            let et_state = et_state.clone();
            let et_clu = et_clu.clone();
            async move {
                let mut state = et_state.write().await;
                log::trace!("tick {:?}: current term: {} voted_for: {:?} et: {}",
                    state.role, state.current_term, state.voted_for, state.election_ticks_before_timeout);
                if state.commit_index < state.last_applied {
                    state.last_applied += 1;
                    apply_to_state_machine(&state.log[state.last_applied]);
                }
                match &mut state.role {
                    ProtocolRole::Follower => {
                        state.election_ticks_before_timeout = state.election_ticks_before_timeout.saturating_sub(1);
                        if state.election_ticks_before_timeout == 0 {
                            state.start_election(et_clu.self_id);
                            state.role = ProtocolRole::Candidate(Some(tokio::task::spawn(hold_election(et_state.clone(), et_clu))));
                        }
                    },
                    ProtocolRole::Candidate(election_task) => {
                        election_task.take().map(|t| t.abort());
                        state.election_ticks_before_timeout = state.election_ticks_before_timeout.saturating_sub(1);
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
    match tick_task.await {
        Ok(()) => {},
        Err(e) if e.is_cancelled() => { },
        Err(e) => return Err(anyhow::anyhow!(e))
    }

    Ok(())
}
