/// Raft-based consensus for distributed telemetry log replication.
///
/// Each node maintains a replicated log of TelemetryFrames. The leader
/// accepts writes; followers replicate. On leader failure, an election
/// selects a new leader within one election timeout.
use crate::telemetry::TelemetryFrame;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;
use rand::Rng;

pub type NodeId  = u64;
pub type Term    = u64;
pub type LogIdx  = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role { Follower, Candidate, Leader }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term:  Term,
    pub index: LogIdx,
    pub frame: TelemetryFrame,
}

// ---- RPC message types ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntries {
    pub term:          Term,
    pub leader_id:     NodeId,
    pub prev_log_idx:  LogIdx,
    pub prev_log_term: Term,
    pub entries:       Vec<LogEntry>,
    pub leader_commit: LogIdx,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    pub term:    Term,
    pub success: bool,
    pub node_id: NodeId,
    pub match_idx: LogIdx,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVote {
    pub term:           Term,
    pub candidate_id:   NodeId,
    pub last_log_idx:   LogIdx,
    pub last_log_term:  Term,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteReply {
    pub term:         Term,
    pub vote_granted: bool,
    pub voter_id:     NodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMsg {
    AppendEntries(AppendEntries),
    AppendEntriesReply(AppendEntriesReply),
    RequestVote(RequestVote),
    RequestVoteReply(RequestVoteReply),
    ClientWrite(TelemetryFrame),
    ClientWriteAck { index: LogIdx, success: bool },
}

/// State machine result from committing a log entry
#[derive(Debug)]
pub struct CommitResult {
    pub index:  LogIdx,
    pub frame:  TelemetryFrame,
}

/// Core Raft state — no I/O, only pure state transitions.
/// The transport layer drives it with messages and timers.
pub struct RaftNode {
    pub id:            NodeId,
    pub peers:         Vec<NodeId>,
    pub role:          Role,
    pub current_term:  Term,
    pub voted_for:     Option<NodeId>,
    pub log:           Vec<LogEntry>,
    pub commit_index:  LogIdx,
    pub last_applied:  LogIdx,
    pub next_index:    HashMap<NodeId, LogIdx>,
    pub match_index:   HashMap<NodeId, LogIdx>,
    pub votes_received: std::collections::HashSet<NodeId>,
    pub election_deadline: Instant,
    pub heartbeat_deadline: Instant,

    // Committed frames ready for the application layer
    committed: std::collections::VecDeque<CommitResult>,
}

const HEARTBEAT_MS: u64  = 50;
const ELECTION_MIN_MS: u64 = 600;
const ELECTION_MAX_MS: u64 = 1200;

impl RaftNode {
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        let mut node = Self {
            id,
            peers,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            votes_received: Default::default(),
            election_deadline: Instant::now(),
            heartbeat_deadline: Instant::now(),
            committed: Default::default(),
        };
        node.reset_election_timer();
        node