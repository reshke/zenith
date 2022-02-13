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
    // TODO kb
    // #[serde(with = "hex")]
    pub ancestor_timeline_id: Option<ZTimelineId>,
}

#[derive(Serialize, Deserialize)]
pub struct TenantCreateRequest {
    #[serde(with = "hex")]
    pub tenant_id: ZTenantId,
}
