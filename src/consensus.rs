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