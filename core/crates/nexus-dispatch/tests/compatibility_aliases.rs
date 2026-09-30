use nexus_contracts::{DispatchPort, RealtimePort};
use nexus_dispatch::{DispatchService, Realtime};

fn current_port_accepts_legacy(port: &dyn RealtimePort) -> &dyn DispatchPort {
    port
}

fn current_service_is_legacy(service: DispatchService) -> Realtime {
    service
}

#[test]
fn pre_v01_internal_type_names_remain_source_compatible() {
    let _ = current_port_accepts_legacy;
    let _ = current_service_is_legacy;
}
