#[cfg(test)]
mod tests {
    use std::{collections::HashMap, net::SocketAddr, path::PathBuf, thread::sleep, time::Duration};
    use futures::{prelude::*, task::LocalSpawnExt};
    use rand::prelude::*;
    use uuid::Uuid;
    use raft_proto::*;
    use anyhow::{Result, anyhow, Context};

    static NEXT_NODE_PORT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(1025);

    struct Cluster {
        config_path: std::path::PathBuf,
        cfg: ClusterConfig,
        children: HashMap<NodeId, tokio::process::Child>,
        shutdown_already: bool
    }

    fn make_node_proc(id: &NodeId, config_path: &std::path::PathBuf) -> anyhow::Result<tokio::process::Child> {
        tokio::process::Command::new("/home/andrew/Source/raft/target/debug/raft-server")
            .current_dir("/tmp")
            .arg(id.to_string())
            .arg(config_path.to_str().unwrap())
            .env("RUST_LOG", "raft=debug")
            .spawn()
            .context("spawn node process")
    }

    impl Cluster {
        async fn create(num_nodes: usize) -> Cluster {
            let mut config_path = PathBuf::from("/tmp");
            let cluster_id = Uuid::new_v4();
            config_path.push(cluster_id.to_string() + ".json");

            let mut nodes = HashMap::new();
            let localhost: SocketAddr = SocketAddr::V4(
                std::net::SocketAddrV4::new(std::net::Ipv4Addr::new(127,0,0,1), 0));
            for _ in 0..num_nodes {
                let node_id = Uuid::new_v4();
                let mut addr = localhost.clone();
                loop {
                    addr.set_port(NEXT_NODE_PORT.fetch_add(1, std::sync::atomic::Ordering::AcqRel));
                    match std::net::TcpListener::bind(addr) {
                        Ok(_) => { break; },
                        Err(_) => { continue; }
                    }
                }
                nodes.insert(node_id, addr);
            }

            serde_json::to_writer(std::fs::File::create(&config_path).unwrap(), &nodes).expect("write cluster config");
            println!("wrote cluster config with {} nodes to {}", num_nodes, config_path.display());

            let children = nodes.iter().map(|(id, _)| {
                (id.clone(), make_node_proc(id, &config_path).expect("start node"))
            }).collect();

            let cfg = ClusterConfig::from_disk(&config_path, Uuid::new_v4()).await.unwrap();
            Cluster {config_path, cfg, children, shutdown_already: false}
        }

        async fn crash_and_restart(&mut self, node: &NodeId) -> Result<()> {
            let mut proc = self.children.remove(node).ok_or(anyhow::anyhow!("node process not found"))?;
            proc.kill().await.context("kill node process")?;
            self.children.insert(*node, make_node_proc(node, &self.config_path)?);
            Ok(())
        }

        async fn shutdown(&mut self) {
            println!("!! shutting down cluster");
            for (node_id, _) in self.cfg.addresses.iter() {
                match self.cfg.get_client(&node_id).await {
                    Ok(cl) => {
                        match cl.shutdown(tarpc::context::current()).await {
                            Ok(_) => {},
                            Err(e) => println!("failed to send shutdown to node {}: {}", node_id, e)
                        }
                    },
                    Err(e) => println!("failed to get client for node {} to shutdown: {}", node_id, e)
                }
            }

            println!("!! killing cluster nodes");
            let mut proc_waits = Vec::new();
            for (id, ch) in self.children.iter_mut() {
                println!("\t killing {}", id);
                match ch.start_kill() {
                    Ok(_) => {
                        proc_waits.push(ch.wait().boxed());
                    },
                    Err(e) => println!("failed to send kill signal to {}: {}", id, e)
                }
            }

            println!("!! waiting for nodes to exit");
            futures::future::join_all(proc_waits).await;

            println!("!! deleting cluster configuration");
            std::fs::remove_file(&self.config_path).unwrap();
            for (node_id, _) in self.cfg.addresses.iter() {
                std::fs::remove_dir_all("/tmp/".to_owned() + &node_id.to_string()).unwrap();
            }
            self.shutdown_already = true;
        }

        async fn read_cluster_state(&self) -> HashMap<NodeId, anyhow::Result<ServerDebugReport>> {
            let mut result = HashMap::new();
            for (node_id, _) in self.cfg.addresses.iter() {
                match self.cfg.get_client(node_id).await {
                    Ok(cl) => {
                        result.insert(node_id.clone(),
                        cl.debug_report(tarpc::context::current()).await.map_err(Into::into));
                    },
                    Err(e) => {
                        println!("failed to get client for {}, resetting: {}", node_id, e);
                        self.cfg.reset_client(node_id).await;
                    }
                }
            }
            result
        }
    }

    impl Drop for Cluster {
        fn drop(&mut self) {
            // !!! this is a big hack because there is no way to have async drop right now !!!
            if self.shutdown_already { return };

            println!("!! killing cluster nodes");
            for (id, ch) in self.children.iter_mut() {
                println!("\t killing {}", id);
                match ch.start_kill() {
                    Ok(_) => { },
                    Err(e) => println!("failed to send kill signal to {}: {}", id, e)
                }
            }

            sleep(Duration::from_secs(10)); //wait for cluster to stop without async

            println!("!! deleting cluster configuration");
            std::fs::remove_file(&self.config_path).unwrap();
            for (node_id, _) in self.cfg.addresses.iter() {
                std::fs::remove_dir_all("/tmp/".to_owned() + &node_id.to_string()).unwrap();
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn up_down_3_nodes() {
        let mut clu = Cluster::create(3).await;
        sleep(Duration::from_secs(4));
        clu.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn up_down_5_nodes() {
        let mut clu = Cluster::create(5).await;
        sleep(Duration::from_secs(4));
        clu.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn successful_leader_election() {
        let mut clu = Cluster::create(5).await;
        let mut tries = 0;
        loop {
            sleep(Duration::from_secs(1));
            println!("polling cluster state");
            let state = clu.read_cluster_state().await;
            let leader = state.iter()
                .find(|(_, report)| report.as_ref().map_or(false, |rep| rep.role == "Leader"));
            if leader.is_some() { break; }
            tries += 1;
            if tries > 20 {
                for (node_id, report) in state { println!("report from {}: {:?}", node_id, report); }
                panic!("waited for 20 seconds but a leader was never elected!");
            }
        }
        clu.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn successful_log_replication() {
        let mut clu = Cluster::create(5).await;
        sleep(Duration::from_secs(8));
        let mut succceded_to_send_log_entry = false;
        for (node_id, _) in clu.cfg.addresses.iter() {
            match clu.cfg.get_client(node_id).await {
                Ok(cl) => {
                    match cl.append_log_entry(tarpc::context::current(), LogItem(42)).await {
                        Ok(_) => { succceded_to_send_log_entry = true; break; },
                        Err(e) => {
                            println!("failed to send log entry to cluster using node {}: {}", node_id, e);
                            continue;
                        }
                    }
                },
                Err(e) => {
                    clu.cfg.reset_client(node_id).await;
                    println!("failed to send log entry to cluster using node {}: {}", node_id, e);
                }
            }
        }
        if !succceded_to_send_log_entry {
            panic!("failed to send log entry to cluter, tried all nodes");
        }
        let mut tries = 0;
        loop {
            sleep(Duration::from_secs(4));
            let state = clu.read_cluster_state().await;
            if state.iter()
                .all(|(node_id, report)|
                    report.as_ref().map_or(false,
                        |rep| rep.log.last().map_or(false, |entry| entry.item.0 == 42)))
            {
                println!("log entry sucessfully replicated!");
                break;
            }
            tries += 1;
            if tries > 5 {
                for (node_id, report) in state { println!("report from {}: {:?}", node_id, report); }
                panic!("waited for 20 seconds but the log entry was never replicated");
            }
        }
        clu.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn follower_restart_and_replicate() {
        let mut clu = Cluster::create(5).await;
        sleep(Duration::from_secs(8));
        let mut succceded_to_send_log_entry = false;
        for (node_id, _) in clu.cfg.addresses.iter() {
            match clu.cfg.get_client(node_id).await {
                Ok(cl) => {
                    match cl.append_log_entry(tarpc::context::current(), LogItem(42)).await {
                        Ok(_) => { succceded_to_send_log_entry = true; break; },
                        Err(e) => {
                            println!("failed to send log entry to cluster using node {}: {}", node_id, e);
                            continue;
                        }
                    }
                },
                Err(e) => {
                    clu.cfg.reset_client(node_id).await;
                    println!("failed to send log entry to cluster using node {}: {}", node_id, e);
                }
            }
        }
        if !succceded_to_send_log_entry {
            panic!("failed to send log entry to cluter, tried all nodes");
        }

        // restart a follower
        {
            let state = clu.read_cluster_state().await;
            let (follower_id, _) = state.iter().find(|(_, rep)|
                    rep.as_ref().map_or(false, |rep| rep.role == "Follower"))
                .expect("there is at least one active follower node");
            clu.crash_and_restart(follower_id).await.expect("crash & restart follower node");
        }

        let mut tries = 0;
        loop {
            sleep(Duration::from_secs(4));
            let state = clu.read_cluster_state().await;
            if state.iter()
                .all(|(_, report)|
                    report.as_ref().map_or(false,
                        |rep| rep.log.last().map_or(false, |entry| entry.item.0 == 42)))
            {
                break;
            }
            tries += 1;
            if tries > 5 {
                for (node_id, report) in state { println!("report from {}: {:?}", node_id, report); }
                panic!("waited for 20 seconds but the log entry was never replicated");
            }
        }
        clu.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn successful_new_election_after_leader_fail() {
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn successful_log_replication_after_new_election() {
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn successful_election_after_first_candidate_fail() {
    }
}
