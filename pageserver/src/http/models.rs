use serde::{Deserialize, Serialize};
use zenith_utils::{lsn::Lsn, zid::ZTimelineId};

use crate::ZTenantId;

#[derive(Serialize, Deserialize)]
pub struct TimelineCreateRequest {
    #[serde(with = "hex")]
    pub tenant_id: ZTenantId,
    #[serde(with = "hex")]
    pub timeline_id: ZTimelineId,
    pub start_lsn: Option<Lsn>,
}

#[derive(Serialize, Deserialize)]
pub struct TenantCreateRequest {
    #[serde(with = "hex")]
    pub tenant_id: ZTenantId,
}
