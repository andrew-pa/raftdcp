#[cfg(test)]
mod tests {
    use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

    use uuid::Uuid;
    use raft_proto::*;

    static NEXT_NODE_PORT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(1025);

    async fn create_cluster(num_nodes: usize) -> (std::path::PathBuf, ClusterConfig, Vec<tokio::process::Child>) {
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
            tokio::process::Command::new("/home/andrew/Source/raft/target/debug/raft-server")
                .current_dir("/tmp")
                .arg(id.to_string())
                .arg(config_path.to_str().unwrap())
                .env("RUST_LOG", "raft")
                .spawn().expect("start node")
        }).collect();

        let cfg = ClusterConfig::from_disk(&config_path, Uuid::new_v4()).await.unwrap();
        (config_path, cfg, children)
    }

    async fn shutdown_cluster(cfg_path: std::path::PathBuf, cfg: ClusterConfig, children: Vec<tokio::process::Child>) {
        for (node_id, _) in cfg.addresses.iter() {
            match cfg.get_client(&node_id).await {
                Ok(cl) => {
                    match cl.shutdown(tarpc::context::current()).await {
                        Ok(_) => {},
                        Err(e) => println!("failed to send shutdown to node {}: {}", node_id, e)
                    }
                },
                Err(e) => println!("failed to get client for node {} to shutdown: {}", node_id, e)
            }
        }
        for mut ch in children {
            match ch.wait().await {
                Ok(_) => {},
                Err(e) => println!("failed to wait for nodes to exit: {}", e)
            }
        }

        std::fs::remove_file(cfg_path).unwrap();
        for (node_id, _) in cfg.addresses.iter() {
            std::fs::remove_dir_all("/tmp/".to_owned() + &node_id.to_string()).unwrap();
        }
    }

    #[tokio::test]
    async fn up_down_3_nodes() {
        let (cfg_path, cfg, children) = create_cluster(3).await;
        std::thread::sleep(std::time::Duration::from_secs(8));
        shutdown_cluster(cfg_path, cfg, children).await;
    }

    #[tokio::test]
    async fn up_down_5_nodes() {
        let (cfg_path, cfg, children) = create_cluster(5).await;
        std::thread::sleep(std::time::Duration::from_secs(8));
        shutdown_cluster(cfg_path, cfg, children).await;
    }

}
