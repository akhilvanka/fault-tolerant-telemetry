use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// A single telemetry sample from the spacecraft.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TelemetryFrame {
    pub id:          Uuid,
    pub source_id:   String,
    pub timestamp_us: u64,   // microseconds since UNIX epoch
    pub sequence_num: u64,
    pub channel:     TelemetryChannel,
    pub value:       f64,
    pub quality:     Quality,
    pub checksum:    u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum TelemetryChannel {
    AltitudeM,
    VelocityVzMps,
    VelocityVxMps,
    PropellantKg,
    ThrustN,
    GimbalPitchRad,
    AttitudePitchRad,
    BatteryVoltage,
    EngineTemp,
    CpuLoad,
    Custom(String),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Quality {
    Good,
    Degraded,  // sensor noise or stale
    Bad,       // hardware fault
}

impl TelemetryFrame {
    pub fn new(source_id: &str, sequence_num: u64, channel: TelemetryChannel, value: f64) -> Self {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let mut frame = Self {
            id: Uuid::new_v4(),
            source_id: source_id.to_string(),
            timestamp_us: ts,
            sequence_num,
            channel,
            value,
            quality: Quality::Good,
            checksum: 0,
        };
        frame.checksum = frame.compute_checksum();
        frame
    }

    pub fn with_quality(mut self, q: Quality) -> Self {
        self.quality = q;
        self
    }

    pub fn compute_checksum(&self) -> u32 {
        // FNV-1a over the serializable fields
        let mut hash: u32 = 2166136261;
        let data = format!("{}{}{}{}",
            self.source_id, self.timestamp_us,
            self.sequence_num, self.value as u64);
        for byte in data.bytes() {
            hash ^= byte as u32;
            hash = hash.wrapping_mul(16777619);
        }
        hash
    }

    pub fn verify_checksum(&self) -> bool {
        self.checksum == self.compute_checksum()
    }
}

/// Aggregated statistics over a rolling window
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelStats {
    pub channel:    TelemetryChannel,
    pub count:      u64,
    pub mean:       f64,
    pub min:        f64,
    pub max:        f64,
    pub std_dev:    f64,
    pub last_value: f64,
    pub last_ts_us: u64,
    pub bad_count:  u64,
}

impl ChannelStats {
    pub fn new(channel: TelemetryChannel) -> Self {
        Self {
            channel,
            count: 0,
            mean: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            std_dev: 0.0,
            last_value: 0.0,
            last_ts_us: 0,
            bad_count: 0,
        }
    }

    /// Welford online variance update
    pub fn update(&mut self, frame: &TelemetryFrame) {
        self.count += 1;
        self.last_value = frame.value;
        self.last_ts_us = frame.timestamp_us;
        if frame.quality == Quality::Bad { self.bad_count += 1; }

        let delta = frame.value - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = frame.value - self.mean;

        // Running sum of squared deviations — store in std_dev field temporarily
        let m2 = self.std_dev * self.std_dev * (self.count - 1) as f64;
        let new_m2 = m2 + delta * delta2;
        self.std_dev = if self.count > 1 {
            (new_m2 / (self.count - 1) as f64).sqrt()
        } else { 0.0 };

        if frame.value < self.min { self.min = frame.value; }
        if frame.value > self.max { self.max = frame.value; }
    }
}

/// Ring buffer for last N frames per channel
pub struct TelemetryBuffer {
    cap:    usize,
    frames: std::collections::VecDeque<TelemetryFrame>,
    stats:  std::collections::HashMap<String, ChannelStats>,
}

impl TelemetryBuffer {
    pub fn new(capacity: usize) -> Self {
        Self { cap: capacity, frames: Default::default(), stats: Default::default() }
    }

    pub fn push(&mut self, frame: TelemetryFrame) {
        let key = format!("{:?}", frame.channel);
        self.stats
            .entry(key)
            .or_insert_with(|| ChannelStats::new(frame.channel.clone()))
            .update(&frame);

        if self.frames.len() >= self.cap {
            self.frames.pop_front();
        }
        self.frames.push_back(frame);
    }

    pub fn recent(&self, n: usize) -> impl Iterator<Item = &TelemetryFrame> {
        let skip = self.frames.len().saturating_sub(n);
        self.frames.iter().skip(skip)
    }

    pub fn stats(&self) -> impl Iterator<Item = &ChannelStats> {
        self.stats.values()
    }

    pub fn len(&self) -> usize { self.frames.len() }
    pub fn is_empty(&self) -> bool { self.frames.is_empty() }
}
