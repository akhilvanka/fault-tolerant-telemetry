/// Async telemetry node — wraps a RaftNode with tokio event loop.
/// Each node runs an independent async task processing messages and timers.
use crate::consensus::{NodeId, RaftMsg, RaftNode};
use crate::metrics::Metrics;
use crate::telemetry::{TelemetryBuffer, TelemetryFrame};
use crate::transport::{NetworkHandle, NodeRx};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};

const TICK_MS: u64 = 10; // Raft tick interval

pub struct TelemetryNode {
    id:      NodeId,
    raft:    RaftNode,
    rx:      NodeRx,
    net:     NetworkHandle,
    buffer:  TelemetryBuffer,
    metrics: Arc<Metrics>,
}

impl TelemetryNode {
    pub fn new(id: NodeId, peers: Vec<NodeId>, rx: NodeRx,
               net: NetworkHandle, metrics: Arc<Metrics>) -> Self {
        Self {
            id,
            raft: RaftNode::new(id, peers),
            rx,
            net,
            buffer: TelemetryBuffer::new(10_000),
            metrics,
        }
    }

    pub fn id(&self) -> NodeId { self.id }
    pub fn is_leader(&self) -> bool { self.raft.is_leader() }
    pub fn commit_index(&self) -> u64 { self.raft.commit_index }

    /// Main event loop — runs until the task is cancelled
    pub async fn run(mut self) {
        let mut ticker = interval(Duration::from_millis(TICK_MS));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let was_candidate = matches!(self.raft.role, crate::consensus::Role::Candidate);
                    let msgs = self.raft.tick();
                    // Count new elections: node just transitioned FROM non-candidate TO candidate
                    let now_candidate = matches!(self.raft.role, crate::consensus::Role::Candidate);
                    if !was_candidate && now_candidate {
                        self.metrics.increment("elections_started");
                    }
                    if !msgs.is_empty() {
                        self.net.broadcast(self.id, msgs).await;
                    }
                    self.drain_committed();
                }

                Some((from, msg)) = self.rx.recv() => {
                    self.handle_message(from, msg).await;
                    self.drain_committed();
                }
            }
        }
    }

    async fn handle_message(&mut self, from: NodeId, msg: RaftMsg) {
        let was_leader = self.raft.is_leader();
        let out = self.raft.handle(from, msg);
        if was_leader != self.raft.is_leader() {
            self.metrics.increment("leader_changes");
        }
        self.net.broadcast(self.id, out).await;
    }

    fn drain_committed(&mut self) {
        for result in self.raft.drain_committed() {
            if !result.frame.verify_checksum() {
                self.metrics.increment("checksum_failures");
                tracing::warn!("Node {}: checksum failure on frame {}", self.id, result.frame.id);
                continue;
            }
            self.buffer.push(result.frame.clone());
            self.metrics.increment("frames_replicated");
            tracing::debug!("Node {} committed log[{}]: {:?} = {:.2}",
                self.id, result.index,
                result.frame.channel, result.frame.value);
        }
    }
}

/// Standalone task for injecting telemetry into the cluster.
/// Finds the leader and sends frames; handles leader-not-found gracefully.
pub struct TelemetryIngester {
    source_id:  String,
    seq:        u64,
    net:        NetworkHandle,
    node_ids:   Vec<NodeId>,
    metrics:    Arc<Metrics>,
}

impl TelemetryIngester {
    pub fn new(source_id: &str, node_ids: Vec<NodeId>,
               net: NetworkHandle, metrics: Arc<Metrics>) -> Self {
        Self { source_id: source_id.to_owned(), seq: 0, net, node_ids, metrics }
    }

    /// Submit a frame to a node (round-robin, the node will forward if not leader)
    pub async fn submit(&mut self, frame: TelemetryFrame) {
        if self.node_ids.is_empty() { return; }
        let target = self.node_ids[self.seq as usize % self.node_ids.len()];
        self.net.send(0, target, RaftMsg::ClientWrite(frame)).await;
        self.seq += 1;
        self.metrics.increment("frames_ingested");
    }
}
