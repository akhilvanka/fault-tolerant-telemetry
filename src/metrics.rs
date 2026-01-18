use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Lock-free metrics registry for high-throughput telemetry pipeline monitoring.
/// All counter/gauge operations are atomic — no mutexes on the hot path.
#[derive(Default)]
pub struct Metrics {
    counters:   DashMap<&'static str, AtomicU64>,
    start_time: Option<Instant>,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            counters:   DashMap::new(),
            start_time: Some(Instant::now()),
        })
    }

    pub fn increment(&self, key: &'static str) {
        self.counters
            .entry(key)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(&self, key: &'static str, n: u64) {
        self.counters
            .entry(key)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(n, Ordering::Relaxed);
    }

    pub fn set(&self, key: &'static str, val: u64) {
        self.counters
            .entry(key)
            .or_insert_with(|| AtomicU64::new(0))
            .store(val, Ordering::Relaxed);
    }

    pub fn get(&self, key: &'static str) -> u64 {
        self.counters
            .get(key)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub fn uptime_secs(&self) -> u64 {
        self.start_time.map(|t| t.elapsed().as_secs()).unwrap_or(0)
    }

    pub fn report(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            frames_ingested:    self.get("frames_ingested"),
            frames_replicated:  self.get("frames_replicated"),
            elections_started:  self.get("elections_started"),
            leader_changes:     self.get("leader_changes"),
            write_errors:       self.get("write_errors"),
            checksum_failures:  self.get("checksum_failures"),
            network_partitions: self.get("network_partitions"),
            uptime_secs:        self.uptime_secs(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub frames_ingested:    u64,
    pub frames_replicated:  u64,
    pub elections_started:  u64,
    pub leader_changes:     u64,
    pub write_errors:       u64,
    pub checksum_failures:  u64,
    pub network_partitions: u64,
    pub uptime_secs:        u64,
}

impl std::fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f,
            "uptime={}s ingested={} replicated={} elections={} leaders={} \
             write_err={} checksum_fail={} partitions={}",
            self.uptime_secs, self.frames_ingested, self.frames_replicated,
            self.elections_started, self.leader_changes,
            self.write_errors, self.checksum_failures, self.network_partitions)
    }
}
