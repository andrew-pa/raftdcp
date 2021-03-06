use anyhow::Result;
use futures::prelude::*;
use rand::{Rng, thread_rng};
use std::{sync::Arc, time::Duration};
use tarpc::{context::Context as RpcContext, server::Channel};
use tokio::sync::RwLock;

mod common;
use common::*;

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
        log::trace!("recv: append_entries(term: {}, leader_id: {}, prev_log_index: {}, prev_log_term: {}, entries: {:?}, leader_commit: {}",
            term, leader_id, prev_log_index, prev_log_term, entries, leader_commit);
        if term < state.current_term {
            log::info!("rejecting append_entries because sender's term is before the current term");
            return (state.current_term, false);
        }
        // making the assumption here that if prev_log_index == 0, then the log must
        // be brand new and so there is nothing to match
        if state.log_entry(prev_log_index)
            .map(|e| e.term != prev_log_term)
            .unwrap_or(false)
        {
            log::info!("rejecting append_entries because sender's log does not match terms (prev_log_index = {} [{:?} != {}])",
                prev_log_index, state.log_entry(prev_log_index), prev_log_term);
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
        // log::trace!("adding new entries to log: {:?}", &entries[next_log_index-prev_log_index-1..]);
        // log::trace!("log already had: {:?}", state.log);
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
        log::trace!("recv: request_vote(term: {}, candidate_id: {}, last_log_index: {}, last_log_term: {})",
            term, candidate_id, last_log_index, last_log_term);
        if term < state.current_term {
            return (state.current_term, false);
        }
        state.election_ticks_before_timeout = default_election_timeout();
        let logs_match = state.last_log_term().map_or(true /*how do empty logs match?*/, |term| {
            if term == last_log_term { 
                state.last_log_index() > last_log_index
            } else {
                term > last_log_term
            }
        });
        log::trace!("logs_match = {}", logs_match);
        if state.voted_for.map(|id| id == candidate_id).unwrap_or(true) && logs_match {
            state.current_term = term;
            log::debug!("voting for {}", candidate_id);
            (state.current_term, true)
        } else {
            log::debug!("rejecting vote for {}", candidate_id);
            (state.current_term, false)
        }
    }

    async fn append_log_entry(self, cx: RpcContext, item: LogItem) {
        let mut state = self.0.write().await;
        state.persist();
        match state.role {
            ProtocolRole::Leader { .. } => {
                let term = state.current_term;
                state.log.push(LogEntry { term, item });
            },
            _ => {
                if let Some(leader) = state.last_leader_id {
                    let leader = self.1.get_client(&leader).await.unwrap();
                    leader.append_log_entry(cx, item).await.unwrap();
                }
            }
        }
    }

    async fn debug_report(self, _cx: RpcContext) -> ServerDebugReport {
        let state = self.0.read().await;
        ServerDebugReport {
            current_term: state.current_term,
            voted_for: state.voted_for,
            last_log_index: state.last_log_index(),
            last_log_term: state.last_log_term(),
            log: state.log.clone(),

            commit_index: state.commit_index,
            last_applied: state.last_applied,

            role: (match state.role {
                ProtocolRole::Follower => "Follower",
                ProtocolRole::Leader {..} => "Leader",
                ProtocolRole::Candidate(_) => "Candidate"
            }).to_string(),

            election_ticks_before_timeout: state.election_ticks_before_timeout,

            last_leader_id: state.last_leader_id
        }
    }

    async fn shutdown(self, _cx: RpcContext) {
        log::warn!("shutting down");
        std::process::exit(0);
    }
}

fn apply_to_state_machine(e: &LogEntry) {
    log::debug!("applying entry to state machine: {:?}", e);
}

async fn hold_election(state: Arc<RwLock<State>>, cluster: Arc<ClusterConfig>) {
    // TODO: if you kill the current leader, right now the remaining servers just reject each
    // other's election vote requests
    let mut votes_recieved = 1;
    let (term, last_log_index, last_log_term) = {
        let state = state.read().await;
        (state.current_term, state.log.len(), state.log.last().map_or(0, |e| e.term))
    };
    let self_id = cluster.self_id;

    let mut futures: Vec<_> = cluster.addresses.iter()
        .filter(|(id, _)| cluster.self_id != **id)
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
                    cluster.reset_client(&node_id).await;
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
        if votes_recieved > cluster.addresses.len()/2 {
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
            follower_indices: cluster.addresses.iter()
                .filter(|(id, _)| cluster.self_id != **id)
                .map(|(id, _)| (*id, (state.last_log_index() + 1, 0usize))).collect()
        };
    }
    leader_update(state, cluster, true).await
}

async fn leader_update(state: Arc<RwLock<State>>, cluster: Arc<ClusterConfig>, first_update: bool) {
    log::trace!("leader update");
    let mut state = state.write().await;
    let mut fi = state.clone_follower_indices().expect("leader update called on leader nodes");
    for (id, (next_index, match_index)) in fi.iter_mut() {
        //if first_update || state.last_log_index() >= *next_index {
            let entries: Vec<_> = state.log[(*next_index - 1)..].into();
            let prev_log_index = *next_index-1;
            let prev_log_term = state.log_entry(prev_log_index).map_or(0, |e| e.term);
            if entries.len() == 0 {
                log::trace!("sending heartbeat to {} [n: {}, m: {}]", id, next_index, match_index);
            } else {
                log::trace!("sending append entries to {} [next: {}, match: {}]: entries: {:?}, prev_log_index: {}, prev_log_term: {}, commit_index: {}",
                    id, next_index, match_index, &entries,
                    prev_log_index, prev_log_term, state.commit_index);
            }
            match cluster.get_client(id).then(|c| async {
                match c {
                    Ok(cl) => cl.append_entries(tarpc::context::current(), state.current_term, cluster.self_id,
                                prev_log_index, prev_log_term, entries, state.commit_index).await.map_err(Into::into),
                    Err(e) => Err(e)
                }
            }).await {
                Ok((term, success)) => {
                    if term > state.current_term {
                        state.current_term = term;
                        state.role = ProtocolRole::Follower;
                        return;
                    }
                    if success {
                        // update indices
                        *next_index = state.last_log_index() + 1;
                        *match_index = state.last_log_index();
                        log::trace!("succeded sending entries to {}, next: {}, match: {}",
                            id, next_index, match_index);
                    } else {
                        *next_index -= 1;
                        // we'll get this node on the next update tick
                        log::trace!("failed sending entries to {}, next: {}, match: {}",
                            id, next_index, match_index);
                    }
                },
                Err(e) => {
                    log::error!("append entries RPC to {} failed: {}", id, e);
                    cluster.reset_client(&id).await;
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
                log::trace!("increasing commit index to {}", n);
                state.commit_index = n;
                break;
            }
        }
        n += 1;
    }
    state.set_follower_indices(fi);
}

use std::io::Write;

#[tokio::main]
async fn main() -> Result<()> {
    let self_id = std::env::args()
        .nth(1)
        .expect("node id")
        .parse::<Uuid>()
        .unwrap();

    env_logger::builder()
        .format(move |buf, record| {
            use log::Level;
            writeln!(buf, "[\x1b[{}m{}\x1b[0m {} ({})] {}",
                match record.level() {
                    Level::Error => "31",
                    Level::Warn => "33",
                    Level::Info => "32",
                    Level::Trace => "36",
                    Level::Debug => "34"
                },
                record.level(),
                self_id,
                record.module_path().unwrap_or(record.target()),
                record.args())
        })
    .init();
    log::trace!("starting process");
    // use rand::Rng;
    tokio::time::sleep(Duration::from_millis(thread_rng().gen_range(0..100))).await;

    let config_path = std::env::args()
        .nth(2)
        .unwrap_or("./cluster.json".into());
    log::info!("connecting to cluster, self_id = {} from config at: {}", self_id, config_path);
    let cluster = Arc::new(ClusterConfig::from_disk(config_path, self_id).await?);

    log::debug!("creating directory for persistent state: {:?}", std::fs::create_dir(self_id.to_string()));
    std::env::set_current_dir(self_id.to_string())?;

    log::trace!("loading persistent state");
    let state = Arc::new(RwLock::new(State::/*from_disk()?*/default()));

    let et_state = state.clone();
    let et_clu = cluster.clone();
    log::trace!("spawning tick stream");
    let tick_task = tokio::task::spawn(async move {
        tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(Duration::from_millis(100)))
        .for_each(|_| {
            let et_state = et_state.clone();
            let et_clu = et_clu.clone();
            async move {
                let mut state = et_state.write().await;
                // log::trace!("tick {:?}: current term: {} voted_for: {:?} et: {}",
                //     state.role, state.current_term, state.voted_for, state.election_ticks_before_timeout);
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
                    ProtocolRole::Leader { .. } => { std::mem::drop(state); leader_update(et_state.clone(), et_clu, false).await }
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
