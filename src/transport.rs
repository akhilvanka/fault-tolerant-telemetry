/// In-process async message bus simulating unreliable network transport.
/// Supports configurable packet loss, latency, and node partitioning —
/// used to inject faults and validate the Raft consensus layer.
use crate::consensus::{NodeId, RaftMsg};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use rand::Rng;

pub type NodeTx = mpsc::UnboundedSender<(NodeId, RaftMsg)>;
pub type NodeRx = mpsc::UnboundedReceiver<(NodeId, RaftMsg)>;

#[derive(Clone, Debug)]
pub struct NetworkConfig {
    pub packet_loss_pct: f64,   // 0.0–1.0
    pub latency_min_ms:  u64,
    pub latency_max_ms:  u64,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self { packet_loss_pct: 0.0, latency_min_ms: 1, latency_max_ms: 5 }
    }
}

pub struct InProcessNetwork {
    senders:    HashMap<NodeId, NodeTx>,
    config:     Arc<RwLock<NetworkConfig>>,
    partitions: Arc<RwLock<Vec<(NodeId, NodeId)>>>, // partitioned pairs
}

impl InProcessNetwork {
    pub fn new() -> Self {
        Self {
            senders:    HashMap::new(),
            config:     Arc::new(RwLock::new(NetworkConfig::default())),
            partitions: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Register a node and return its receive channel
    pub fn register(&mut self, id: NodeId) -> NodeRx {
        let (tx, rx) = mpsc::unbounded_channel();
        self.senders.insert(id, tx);
        rx
    }

    /// Returns a NetworkHandle that can be cloned and used by each node
    pub fn handle(&self) -> NetworkHandle {
        NetworkHandle {
            senders:    self.senders.clone(),
            config:     Arc::clone(&self.config),
            partitions: Arc::clone(&self.partitions),
        }
    }

    pub fn config_handle(&self) -> Arc<RwLock<NetworkConfig>> {
        Arc::clone(&self.config)
    }

    pub fn partition_handle(&self) -> Arc<RwLock<Vec<(NodeId, NodeId)>>> {
        Arc::clone(&self.partitions)
    }
}

impl Default for InProcessNetwork {
    fn default() -> Self { Self::new() }
}

#[derive(Clone)]
pub struct NetworkHandle {
    senders:    HashMap<NodeId, NodeTx>,
    config:     Arc<RwLock<NetworkConfig>>,
    partitions: Arc<RwLock<Vec<(NodeId, NodeId)>>>,
}

impl NetworkHandle {
    pub async fn send(&self, from: NodeId, to: NodeId, msg: RaftMsg) {
        let config     = self.config.read().await;
        let partitions = self.partitions.read().await;

        // Drop if partitioned
        if partitions.iter().any(|&(a, b)| (a == from && b == to) || (a == to && b == from)) {
            return;
        }

        // Simulated packet loss
        if config.packet_loss_pct > 0.0 {
            let r: f64 = rand::thread_rng().gen();
            if r < config.packet_loss_pct { return; }
        }

        // Simulated latency
        let latency_ms = rand::thread_rng()
            .gen_range(config.latency_min_ms..=config.latency_max_ms);
        drop(config);
        drop(partitions);

        if let Some(tx) = self.senders.get(&to) {
            let tx = tx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(latency_ms)).await;
                let _ = tx.send((from, msg));
            });
        }
    }

    pub async fn broadcast(&self, from: NodeId, msgs: Vec<(NodeId, RaftMsg)>) {
        for (to, msg) in msgs {
            self.send(from, to, msg).await;
        }
    }

    pub async fn add_partition(&self, a: NodeId, b: NodeId) {
        let mut p = self.partitions.write().await;
        p.push((a, b));
        tracing::warn!("Network partition: {} <-> {}", a, b);
    }

    pub async fn remove_partition(&self, a: NodeId, b: NodeId) {
        let mut p = self.partitions.write().await;
        p.retain(|&(x, y)| !((x == a && y == b) || (x == b && y == a)));
        tracing::warn!("Partition healed: {} <-> {}", a, b);
    }

    pub async fn set_packet_loss(&self, pct: f64) {
        self.config.write().await.packet_loss_pct = pct.clamp(0.0, 1.0);
    }
}
