use crate::bridge::CapabilityBridge as _;
use futures_util::StreamExt as _;

#[test]
fn capability_descriptor_tracks_supported_operations() {
    let capability = crate::bridge::CapabilityDescriptor::new(
        crate::bridge::UNIX_NAMESPACE,
        crate::bridge::BridgeBackendKind::LocalDaemon,
        [crate::bridge::UNIX_EXEC_OPERATION, "process.cancel"],
    );

    assert!(capability.supports(crate::bridge::UNIX_EXEC_OPERATION));
    assert!(!capability.supports("computer.observe"));
}

#[test]
fn bridge_scope_preserves_thread_coordinates() {
    let coordinates = verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
    let scope = crate::bridge::BridgeScope::from_thread(&coordinates);

    assert_eq!(scope.tenant_id, "tenant");
    assert_eq!(scope.user_id, "user");
    assert_eq!(scope.session_id, "session");
    assert_eq!(scope.thread_id, Some(coordinates.thread_id));
}

#[tokio::test]
async fn rejecting_bridge_returns_terminal_failure_event() {
    let bridge =
        crate::bridge::RejectingCapabilityBridge::new(crate::bridge::BridgeCapabilities::new(
            "rejecting",
            crate::bridge::BridgeBackendKind::InProcess,
        ));
    let scope = crate::bridge::BridgeScope::new("tenant", "user", "session", None);
    let session = bridge
        .open_session(crate::bridge::OpenBridgeSessionRequest {
            scope: scope.clone(),
            requested_capabilities: vec![crate::bridge::CapabilityGrant::new(
                crate::bridge::UNIX_NAMESPACE,
                [crate::bridge::UNIX_EXEC_OPERATION],
            )],
            metadata: std::collections::BTreeMap::new(),
        })
        .await
        .unwrap();
    let request = crate::bridge::OperationRequest::unix_exec(
        session.session_id,
        scope,
        crate::bridge::UnixExecPayload::new("echo hi", "/workspace"),
    );
    let operation_id = request.operation_id;

    let mut events = bridge.invoke(request).await.unwrap();
    let event = events.next().await.unwrap().unwrap();

    assert_eq!(event.operation_id(), operation_id);
    assert!(event.is_terminal());
    assert!(matches!(
        event,
        crate::bridge::OperationEvent::Failed {
            code,
            ..
        } if code == "capability_unavailable"
    ));
    assert!(events.next().await.is_none());
}
