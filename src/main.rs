/// Single-node telemetry ingestion demo with checksum verification and stats.
use fault_tolerant_telemetry::{
    metrics::Metrics,
    telemetry::{TelemetryBuffer, TelemetryChannel, TelemetryFrame, Quality},
};
use tracing_subscriber::EnvFilter;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .init();

    let metrics = Metrics::new();
    let mut buffer = TelemetryBuffer::new(1000);

    println!("=== Single-Node Telemetry Ingest Demo ===\n");

    let channels: Vec<(TelemetryChannel, f64)> = vec![
        (TelemetryChannel::AltitudeM,      3000.0),
        (TelemetryChannel::VelocityVzMps, -220.0),
        (TelemetryChannel::PropellantKg,   8500.0),
        (TelemetryChannel::ThrustN,        810_000.0),
        (TelemetryChannel::GimbalPitchRad, 0.03),
    ];

    // Simulate 50s of landing telemetry at 20 Hz
    for step in 0..1000u64 {
        let t = step as f64 * 0.05; // 20 Hz

        for (idx, (ch, base)) in channels.iter().enumerate() {
            let val = match ch {
                TelemetryChannel::AltitudeM      => (base - 60.0 * t).max(0.0),
                TelemetryChannel::VelocityVzMps  => -(base.abs() - 4.5 * t.sqrt()).max(1.7),
                TelemetryChannel::PropellantKg   => (base - 200.0 * t).max(0.0),
                TelemetryChannel::ThrustN        => base + 50000.0 * (t * 0.5).sin(),
                TelemetryChannel::GimbalPitchRad => base * (t * 2.0).sin(),
                _ => *base,
            };

            let quality = if step % 137 == 0 { Quality::Degraded } else { Quality::Good };
            let seq_num = step * channels.len() as u64 + idx as u64;
            let frame = TelemetryFrame::new("falcon9-s1", seq_num, ch.clone(), val)
                .with_quality(quality);

            assert!(frame.verify_checksum(), "checksum failed for frame {}", step);
            buffer.push(frame);
            metrics.increment("frames_ingested");
        }
    }

    println!("Ingested {} frames", metrics.get("frames_ingested"));
    println!("Buffer size: {}", buffer.len());
    println!("\nChannel statistics:");

    let mut stats: Vec<_> = buffer.stats().collect();
    stats.sort_by_key(|s| format!("{:?}", s.channel));
    for s in stats {
        println!("  {:?}", s.channel);
        println!("    count={} mean={:.2} min={:.2} max={:.2} stddev={:.2} bad_frames={}",
            s.count, s.mean, s.min, s.max, s.std_dev, s.bad_count);
    }

    println!("\nLast 3 frames:");
    for f in buffer.recent(3) {
        println!("  seq={} channel={:?} value={:.2} quality={:?} checksum_ok={}",
            f.sequence_num, f.channel, f.value, f.quality, f.verify_checksum());
    }

    println!("\n[PASS] All checksums verified, channel statistics computed");
}
