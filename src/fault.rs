/// Fault injection scenarios for validating system resilience.
use crate::consensus::NodeId;
use crate::transport::NetworkHandle;
use std::time::Duration;
use tokio::time::sleep;

pub struct FaultInjector {
    net: NetworkHandle,
}

impl FaultInjector {
    pub fn new(net: NetworkHandle) -> Self { Self { net } }

    /// Partition a node from all others for `duration`
    pub async fn node_isolation(&self, victim: NodeId, peers: &[NodeId], duration: Duration) {
        tracing::warn!("[FAULT] Isolating node {} for {:?}", victim, duration);
        for &peer in peers {
            self.net.add_partition(victim, peer).await;
        }
        sleep(duration).await;
        for &peer in peers {
            self.net.remove_partition(victim, peer).await;
        }
        tracing::warn!("[FAULT] Node {} reconnected", victim);
    }

    /// Split cluster into two halves that cannot communicate
    pub async fn split_brain(&self, group_a: &[NodeId], group_b: &[NodeId], duration: Duration) {
        tracing::warn!("[FAULT] Split-brain: {:?} vs {:?} for {:?}", group_a, group_b, duration);
        for &a in group_a {
            for &b in group_b {
                self.net.add_partition(a, b).await;
            }
        }
        sleep(duration).await;
        for &a in group_a {
            for &b in group_b {
                self.net.remove_partition(a, b).await;
            }
        }
        tracing::warn!("[FAULT] Split-brain healed");
    }

    /// Introduce packet loss for `duration` then restore
    pub async fn packet_loss(&self, pct: f64, duration: Duration) {
        tracing::warn!("[FAULT] Packet loss {:.0}% for {:?}", pct * 100.0, duration);
        self.net.set_packet_loss(pct).await;
        sleep(duration).await;
        self.net.set_packet_loss(0.0).await;
        tracing::warn!("[FAULT] Packet loss cleared");
    }
}
