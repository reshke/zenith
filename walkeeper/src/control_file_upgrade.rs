//! Code to deal with safekeeper control file upgrades
use crate::safekeeper::{
    AcceptorState, PgUuid, SafeKeeperState, ServerInfo, Term, TermHistory, TermSwitchEntry,
};
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use tracing::*;
use zenith_utils::{
    bin_ser::LeSer,
    lsn::Lsn,
    pq_proto::SystemId,
    zid::{ZTenantId, ZTimelineId},
};

/// Persistent consensus state of the acceptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AcceptorStateV1 {
    /// acceptor's last term it voted for (advanced in 1 phase)
    term: Term,
    /// acceptor's epoch (advanced, i.e. bumped to 'term' when VCL is reached).
    epoch: Term,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SafeKeeperStateV1 {
    /// persistent acceptor state
    acceptor_state: AcceptorStateV1,
    /// information about server
    server: ServerInfo,
    /// Unique id of the last *elected* proposer we dealed with. Not needed
    /// for correctness, exists for monitoring purposes.
    proposer_uuid: PgUuid,
    /// part of WAL acknowledged by quorum and available locally
    commit_lsn: Lsn,
    /// minimal LSN which may be needed for recovery of some safekeeper (end_lsn
    /// of last record streamed to everyone)
    truncate_lsn: Lsn,
    // Safekeeper starts receiving WAL from this LSN, zeros before it ought to
    // be skipped during decoding.
    wal_start_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServerInfoV2 {
    /// Postgres server version
    pub pg_version: u32,
    pub system_id: SystemId,
    pub tenant_id: ZTenantId,
    /// Zenith timelineid
    pub ztli: ZTimelineId,
    pub wal_seg_size: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafeKeeperStateV2 {
    /// persistent acceptor state
    pub acceptor_state: AcceptorState,
    /// information about server
    pub server: ServerInfoV2,
    /// Unique id of the last *elected* proposer we dealed with. Not needed
    /// for correctness, exists for monitoring purposes.
    pub proposer_uuid: PgUuid,
    /// part of WAL acknowledged by quorum and available locally
    pub commit_lsn: Lsn,
    /// minimal LSN which may be needed for recovery of some safekeeper (end_lsn
    /// of last record streamed to everyone)
    pub truncate_lsn: Lsn,
    // Safekeeper starts receiving WAL from this LSN, zeros before it ought to
    // be skipped during decoding.
    pub wal_start_lsn: Lsn,
}

pub fn upgrade_control_file(buf: &[u8], version: u32) -> Result<SafeKeeperState> {
    // migrate to storing full term history
    if version == 1 {
        info!("reading safekeeper control file version {}", version);
        let oldstate = SafeKeeperStateV1::des(&buf[..buf.len()])?;
        let ac = AcceptorState {
            term: oldstate.acceptor_state.term,
            term_history: TermHistory(vec![TermSwitchEntry {
                term: oldstate.acceptor_state.epoch,
                lsn: Lsn(0),
            }]),
        };
        return Ok(SafeKeeperState {
            acceptor_state: ac,
            server: oldstate.server.clone(),
            proposer_uuid: oldstate.proposer_uuid,
            commit_lsn: oldstate.commit_lsn,
            truncate_lsn: oldstate.truncate_lsn,
            wal_start_lsn: oldstate.wal_start_lsn,
        });
    // migrate to hexing some zids
    } else if version == 2 {
        info!("reading safekeeper control file version {}", version);
        let oldstate = SafeKeeperStateV2::des(&buf[..buf.len()])?;
        let server = ServerInfo {
            pg_version: oldstate.server.pg_version,
            system_id: oldstate.server.system_id,
            tenant_id: oldstate.server.tenant_id,
            timeline_id: oldstate.server.ztli,
            wal_seg_size: oldstate.server.wal_seg_size,
        };
        return Ok(SafeKeeperState {
            acceptor_state: oldstate.acceptor_state,
            server,
            proposer_uuid: oldstate.proposer_uuid,
            commit_lsn: oldstate.commit_lsn,
            truncate_lsn: oldstate.truncate_lsn,
            wal_start_lsn: oldstate.wal_start_lsn,
        });
    }
    bail!("unsupported safekeeper control file version {}", version)
}
