use fault_tolerant_telemetry::{
    consensus::NodeId,
    fault::FaultInjector,
    metrics::Metrics,
    node::{TelemetryIngester, TelemetryNode},
    telemetry::{TelemetryChannel, TelemetryFrame},
    transport::InProcessNetwork,
};
use std::time::Duration;
use tokio::time::sleep;
use tracing_subscriber::EnvFilter;

const NUM_NODES: usize = 5;

fn node_ids() -> Vec<NodeId> {
    (1..=NUM_NODES as NodeId).collect()
}

fn peers_of(id: NodeId) -> Vec<NodeId> {
    node_ids().into_iter().filter(|&n| n != id).collect()
}

async fn run_cluster() -> anyhow::Result<()> {
    let mut network = InProcessNetwork::new();
    let metrics = Metrics::new();

    // Phase 1: register ALL nodes before creating any handles.
    // NetworkHandle snapshots the sender map at creation — if we interleave
    // register() and handle() calls, early nodes can't reach later peers.
    let mut rxs: Vec<_> = node_ids().iter().map(|&id| network.register(id)).collect();

    // Phase 2: now that every sender is in the map, create handles and spawn nodes
    let mut node_handles = Vec::new();
    for (i, &id) in node_ids().iter().enumerate() {
        let rx    = rxs.remove(0);
        let net_h = network.handle(); // now contains senders for all 5 nodes
        let m     = metrics.clone();
        let node  = TelemetryNode::new(id, peers_of(id), rx, net_h, m);
        let _i    = i; // suppress unused warning

        let handle = tokio::spawn(async move { node.run().await });
        node_handles.push(handle);
    }

    let net = network.handle();
    let fault_net = network.handle();

    // Wait for initial leader election (election timeouts: 600-1200ms)
    println!("\n=== Fault-Tolerant Telemetry Cluster ===");
    println!("[+] Waiting for initial leader election (up to 2s)...");
    sleep(Duration::from_millis(2000)).await;

    // Start telemetry producer — simulates rocket landing telemetry
    let m2 = metrics.clone();
    let producer_net = net.clone();
    tokio::spawn(async move {
        let mut ingester = TelemetryIngester::new(
            "falcon9-stage1", node_ids(), producer_net, m2
        );
        let channels = vec![
            TelemetryChannel::AltitudeM,
            TelemetryChannel::VelocityVzMps,
            TelemetryChannel::PropellantKg,
            TelemetryChannel::ThrustN,
            TelemetryChannel::GimbalPitchRad,
        ];

        let mut t = 0.0f64;
        loop {
            for ch in &channels {
                // Simulate landing trajectory values
                let val = match ch {
                    TelemetryChannel::AltitudeM      => (3000.0 - 60.0 * t).max(0.0),
                    TelemetryChannel::VelocityVzMps  => -(220.0 - 4.5 * t.sqrt()).max(2.0),
                    TelemetryChannel::PropellantKg   => (8500.0 - 200.0 * t).max(0.0),
                    TelemetryChannel::ThrustN        => 800_000.0 + 50000.0 * (t * 0.5).sin(),
                    TelemetryChannel::GimbalPitchRad => 0.05 * (t * 2.0).sin(),
                    _ => 0.0,
                };

                let frame = TelemetryFrame::new("falcon9-stage1",
                    (t * 1000.0) as u64, ch.clone(), val);
                ingester.submit(frame).await;
            }
            t += 0.1;
            sleep(Duration::from_millis(50)).await;
        }
    });

    sleep(Duration::from_millis(800)).await;

    // --- Fault 1: Isolate node 1 (may be leader) ---
    println!("\n[FAULT 1] Isolating node 1 for 1500ms (simulated node crash)");
    let fi = FaultInjector::new(fault_net.clone());
    let peers_no_1: Vec<NodeId> = (2..=NUM_NODES as NodeId).collect();
    fi.node_isolation(1, &peers_no_1, Duration::from_millis(1500)).await;
    println!("[+] Node 1 reconnected — cluster should have elected new leader");

    // Let cluster stabilize and replicate buffered writes
    sleep(Duration::from_millis(1000)).await;

    // Print metrics
    let snap = metrics.report();
    println!("\n--- Metrics after fault 1 ---");
    println!("  {}", snap);

    // --- Fault 2: Split-brain [1,2] vs [3,4,5] ---
    println!("\n[FAULT 2] Split-brain: nodes [1,2] vs [3,4,5] for 1200ms");
    fi.split_brain(&[1, 2], &[3, 4, 5], Duration::from_millis(1200)).await;
    println!("[+] Split healed — minority partition should rejoin");

    sleep(Duration::from_millis(1000)).await;

    // --- Fault 3: 20% packet loss ---
    println!("\n[FAULT 3] 20% packet loss for 800ms");
    fi.packet_loss(0.20, Duration::from_millis(800)).await;
    println!("[+] Packet loss cleared");

    sleep(Duration::from_millis(800)).await;

    // Final metrics
    let snap = metrics.report();
    println!("\n=== Final Metrics ===");
    println!("  {}", snap);
    println!("\n  frames ingested:   {}", snap.frames_ingested);
    println!("  frames replicated: {}", snap.frames_replicated);
    println!("  elections started: {}", snap.elections_started);
    println!("  leader changes:    {}", snap.leader_changes);
    println!("  checksum failures: {}", snap.checksum_failures);
    println!("  write errors:      {}", snap.write_errors);

    // This demo uses round-robin submission without client-side redirect, so
    // 4/5 writes go to followers and are dropped. The replication rate measures
    // writes that reached the leader AND committed to quorum.
    let commit_rate = if snap.frames_ingested > 0 {
        100.0 * snap.frames_replicated as f64 / snap.frames_ingested as f64
    } else { 0.0 };
    println!("\n  Commit rate (leader-accepted writes): {:.1}%", commit_rate);
    println!("  (Round-robin ingester; non-leader nodes silently drop writes — add redirect for 100%)");
    if snap.checksum_failures == 0 {
        println!("  [PASS] Zero checksum failures — all committed data is bit-for-bit correct");
    }
    if snap.leader_changes > 0 {
        println!("  [PASS] Cluster re-elected leadership {} time(s) under injected faults", snap.leader_changes);
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("warn"))
        .init();

    if let Err(e) = run_cluster().await {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
