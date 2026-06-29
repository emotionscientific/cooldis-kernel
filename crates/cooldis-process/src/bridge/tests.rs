use super::*;
use futures_util::StreamExt;

#[test]
fn capability_descriptor_tracks_supported_operations() {
    let capability = CapabilityDescriptor::new(
        UNIX_NAMESPACE,
        BridgeBackendKind::LocalDaemon,
        [UNIX_EXEC_OPERATION, "process.cancel"],
    );

    assert!(capability.supports(UNIX_EXEC_OPERATION));
    assert!(!capability.supports("computer.observe"));
}

#[test]
fn bridge_scope_preserves_thread_coordinates() {
    let coordinates = ThreadCoordinates::new("tenant", "user", "session");
    let scope = BridgeScope::from_thread(&coordinates);

    assert_eq!(scope.tenant_id, "tenant");
    assert_eq!(scope.user_id, "user");
    assert_eq!(scope.session_id, "session");
    assert_eq!(scope.thread_id, Some(coordinates.thread_id));
}

#[tokio::test]
async fn rejecting_bridge_returns_terminal_failure_event() {
    let bridge = RejectingCapabilityBridge::new(BridgeCapabilities::new(
        "rejecting",
        BridgeBackendKind::InProcess,
    ));
    let scope = BridgeScope::new("tenant", "user", "session", None);
    let session = bridge
        .open_session(OpenBridgeSessionRequest {
            scope: scope.clone(),
            requested_capabilities: vec![CapabilityGrant::new(
                UNIX_NAMESPACE,
                [UNIX_EXEC_OPERATION],
            )],
            metadata: BTreeMap::new(),
        })
        .await
        .unwrap();
    let request = OperationRequest::unix_exec(
        session.session_id,
        scope,
        UnixExecPayload::new("echo hi", "/workspace"),
    );
    let operation_id = request.operation_id;

    let mut events = bridge.invoke(request).await.unwrap();
    let event = events.next().await.unwrap().unwrap();

    assert_eq!(event.operation_id(), operation_id);
    assert!(event.is_terminal());
    assert!(matches!(
        event,
        OperationEvent::Failed {
            code,
            ..
        } if code == "capability_unavailable"
    ));
    assert!(events.next().await.is_none());
}
