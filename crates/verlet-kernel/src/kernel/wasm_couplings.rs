const WASM_COUPLING_CACHE_CAPACITY: usize = 32;

#[derive(Clone)]
pub struct WasmCouplingExecutor {
    operation_registry_root: std::path::PathBuf,
    cache: std::sync::Arc<std::sync::Mutex<WasmCouplingCache>>,
}

impl std::fmt::Debug for WasmCouplingExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WasmCouplingExecutor")
            .field("operation_registry_root", &self.operation_registry_root)
            .finish_non_exhaustive()
    }
}

impl WasmCouplingExecutor {
    pub fn new(operation_registry_root: impl Into<std::path::PathBuf>) -> Self {
        Self {
            operation_registry_root: operation_registry_root.into(),
            cache: std::sync::Arc::new(std::sync::Mutex::new(WasmCouplingCache::new(
                WASM_COUPLING_CACHE_CAPACITY,
            ))),
        }
    }

    pub fn supports_coupling_id(id: &str) -> bool {
        !id.trim().is_empty() && !id.starts_with("std::")
    }

    pub fn operation_registry_root(&self) -> &std::path::Path {
        &self.operation_registry_root
    }

    #[cfg(test)]
    fn cache_stats(&self) -> WasmCouplingCacheStats {
        self.cache.lock().unwrap().stats()
    }
}

#[async_trait::async_trait]
impl crate::CouplingExecutor for WasmCouplingExecutor {
    async fn invoke(
        &self,
        request: crate::CouplingInvocation,
    ) -> crate::VerletResult<crate::CouplingExecutionResult> {
        let operation_name = request
            .coupling
            .function
            .operation_name
            .clone()
            .ok_or_else(|| {
                crate::VerletError::RuntimeFactory(format!(
                    "wasm coupling {:?} must bind exactly one operation",
                    request.coupling.id
                ))
            })?;
        let key = WasmCouplingCacheKey::new(
            request.coupling.function.name.clone(),
            request.coupling.function.artifact_hash.clone(),
        );
        let cached = { self.cache.lock().unwrap().get(&key) };
        let cached = match cached {
            Some(cached) => cached,
            None => {
                let filled = self
                    .fill_cache_entry(&request, &operation_name, &key)
                    .await?;
                self.cache.lock().unwrap().insert(key.clone(), filled)
            }
        };
        ensure_operation_exposed(
            &request.coupling.id,
            &key.operation_name,
            &cached.manifest,
            &operation_name,
        )?;
        let input = encode_invocation(&request)?;
        let output = match request.coupling.budget.max_ms {
            Some(max_ms) => {
                match tokio::time::timeout(
                    std::time::Duration::from_millis(max_ms),
                    cached
                        .runtime
                        .invoke_operation_bytes(&operation_name, input),
                )
                .await
                {
                    Ok(Ok(output)) => output,
                    Ok(Err(err)) if wasm_error_is_timeout(&err.to_string()) => {
                        return Err(crate::VerletError::RuntimeExecution(format!(
                            "timeout: wasm coupling {:?} exceeded max_ms budget",
                            request.coupling.id
                        )));
                    }
                    Ok(Err(err)) => {
                        return Err(crate::VerletError::RuntimeExecution(format!(
                            "trap: wasm coupling {:?} failed: {err}",
                            request.coupling.id
                        )));
                    }
                    Err(_) => {
                        return Err(crate::VerletError::RuntimeExecution(format!(
                            "timeout: wasm coupling {:?} exceeded max_ms budget",
                            request.coupling.id
                        )));
                    }
                }
            }
            None => cached
                .runtime
                .invoke_operation_bytes(&operation_name, input)
                .await
                .map_err(|err| {
                    crate::VerletError::RuntimeExecution(format!(
                        "trap: wasm coupling {:?} failed: {err}",
                        request.coupling.id
                    ))
                })?,
        };
        decode_discharge(&request.coupling.id, &output.output)
    }
}

impl WasmCouplingExecutor {
    async fn fill_cache_entry(
        &self,
        request: &crate::CouplingInvocation,
        operation_name: &str,
        key: &WasmCouplingCacheKey,
    ) -> crate::VerletResult<CachedWasmCoupling> {
        let registry = crate::LocalOperationRegistry::new(&self.operation_registry_root);
        let record = registry
            .load_version_record(&key.operation_name, &key.artifact_hash)
            .map_err(|err| {
                crate::VerletError::RuntimeFactory(format!(
                    "wasm coupling {:?} operation {}@sha256:{} was not found: {err}",
                    request.coupling.id, key.operation_name, key.artifact_hash
                ))
            })?;
        ensure_operation_exposed(
            &request.coupling.id,
            &record.name,
            &record.manifest,
            operation_name,
        )?;
        let mut config = registry.load_runtime_config_for_published_record(&record)?;
        config.operation_name = operation_name.to_string();
        config.capability_grants.clear();
        config.invocation_context = verlet_abi::InvocationContext::anonymous();
        config.secrets.clear();
        config.vfs = None;
        config.host_import_policy = verlet_wasm::WasmHostImportPolicy::PureCompute;

        let factory = verlet_wasm::WasmRuntimeFactory::new(config)?;
        let runtime = factory
            .build_validated_operation_runtime()
            .await
            .map_err(|err| {
                crate::VerletError::RuntimeExecution(format!(
                    "trap: wasm coupling {:?} violates pure-compute import policy: {err}",
                    request.coupling.id
                ))
            })?;
        Ok(CachedWasmCoupling {
            runtime: std::sync::Arc::new(runtime),
            manifest: record.manifest,
        })
    }
}

/// Immutable cache key for a published Wasm operation artifact. The artifact
/// hash is content-addressed, so entries never need invalidation; the bounded
/// map only evicts to cap memory across many distinct artifacts.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct WasmCouplingCacheKey {
    operation_name: String,
    artifact_hash: String,
}

impl WasmCouplingCacheKey {
    fn new(operation_name: String, artifact_hash: String) -> Self {
        Self {
            operation_name,
            artifact_hash,
        }
    }
}

struct CachedWasmCoupling {
    runtime: std::sync::Arc<verlet_wasm::WasmModuleRuntime>,
    manifest: verlet_abi::WasmOperationManifest,
}

struct WasmCouplingCache {
    capacity: usize,
    entries: std::collections::HashMap<WasmCouplingCacheKey, std::sync::Arc<CachedWasmCoupling>>,
    lru: std::collections::VecDeque<WasmCouplingCacheKey>,
    #[cfg(test)]
    stats: WasmCouplingCacheStats,
}

impl WasmCouplingCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: std::collections::HashMap::new(),
            lru: std::collections::VecDeque::new(),
            #[cfg(test)]
            stats: WasmCouplingCacheStats::default(),
        }
    }

    fn get(&mut self, key: &WasmCouplingCacheKey) -> Option<std::sync::Arc<CachedWasmCoupling>> {
        let cached = self.entries.get(key).cloned()?;
        self.touch(key);
        #[cfg(test)]
        {
            self.stats.hits += 1;
        }
        Some(cached)
    }

    fn insert(
        &mut self,
        key: WasmCouplingCacheKey,
        entry: CachedWasmCoupling,
    ) -> std::sync::Arc<CachedWasmCoupling> {
        if let Some(cached) = self.entries.get(&key).cloned() {
            self.touch(&key);
            return cached;
        }
        self.evict_until_room();
        let cached = std::sync::Arc::new(entry);
        self.entries
            .insert(key.clone(), std::sync::Arc::clone(&cached));
        self.lru.push_back(key);
        #[cfg(test)]
        {
            self.stats.fills += 1;
        }
        cached
    }

    fn touch(&mut self, key: &WasmCouplingCacheKey) {
        if let Some(position) = self.lru.iter().position(|candidate| candidate == key) {
            self.lru.remove(position);
        }
        self.lru.push_back(key.clone());
    }

    fn evict_until_room(&mut self) {
        while self.entries.len() >= self.capacity {
            let Some(evicted) = self.lru.pop_front() else {
                break;
            };
            if self.entries.remove(&evicted).is_some() {
                #[cfg(test)]
                {
                    self.stats.evictions += 1;
                }
            }
        }
    }

    #[cfg(test)]
    fn stats(&self) -> WasmCouplingCacheStats {
        WasmCouplingCacheStats {
            entries: self.entries.len(),
            ..self.stats
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct WasmCouplingCacheStats {
    entries: usize,
    fills: usize,
    hits: usize,
    evictions: usize,
}

fn ensure_operation_exposed(
    coupling_id: &str,
    record_name: &str,
    manifest: &verlet_abi::WasmOperationManifest,
    operation_name: &str,
) -> crate::VerletResult<()> {
    if manifest.operation(operation_name).is_none() {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "wasm coupling {coupling_id:?} operation record {record_name:?} does not expose {operation_name:?}"
        )));
    }
    Ok(())
}

fn encode_invocation(request: &crate::CouplingInvocation) -> crate::VerletResult<Vec<u8>> {
    let invocation = verlet_abi::CouplingInvocation::new(
        invocation_event(&request.trigger_event),
        request
            .source_events
            .iter()
            .map(invocation_event)
            .collect::<Vec<_>>(),
        request.coupling.config.clone(),
        verlet_abi::CouplingInvocationMeta {
            coupling_id: request.coupling.id.clone(),
            thread_id: request.trigger_event.coordinates.thread_id.to_string(),
            depth: request.activation.depth,
        },
    );
    serde_json::to_vec(&invocation).map_err(|err| {
        crate::VerletError::RuntimeExecution(format!(
            "trap: failed to encode {COUPLING_INVOCATION_ABI} payload: {err}",
            COUPLING_INVOCATION_ABI = verlet_abi::COUPLING_INVOCATION_ABI
        ))
    })
}

fn invocation_event(event: &crate::EventRecord) -> verlet_abi::CouplingInvocationEvent {
    verlet_abi::CouplingInvocationEvent {
        id: event.id.to_string(),
        stream_id: event.stream_id.to_string(),
        sequence: event.sequence.get(),
        kind: event.kind.to_string(),
        origin: event.origin.as_str().to_string(),
        payload: event.payload.clone(),
    }
}

fn decode_discharge(
    coupling_id: &str,
    bytes: &[u8],
) -> crate::VerletResult<crate::CouplingExecutionResult> {
    let discharge =
        serde_json::from_slice::<verlet_abi::CouplingDischarge>(bytes).map_err(|err| {
            crate::VerletError::RuntimeExecution(format!(
                "trap: wasm coupling {coupling_id:?} emitted invalid discharge JSON: {err}"
            ))
        })?;
    if discharge.abi != verlet_abi::COUPLING_DISCHARGE_ABI {
        return Err(crate::VerletError::RuntimeExecution(format!(
            "trap: wasm coupling {coupling_id:?} emitted unsupported discharge ABI {:?}",
            discharge.abi
        )));
    }
    let discharges = discharge
        .events
        .into_iter()
        .map(|event| {
            let kind = event.kind.parse::<crate::EventKind>().map_err(|err| {
                crate::VerletError::RuntimeExecution(format!(
                    "trap: wasm coupling {coupling_id:?} emitted unknown event kind {:?}: {err}",
                    event.kind
                ))
            })?;
            Ok(crate::CouplingDischarge {
                event_id: None,
                stream: event.stream,
                kind,
                payload: event.payload,
            })
        })
        .collect::<crate::VerletResult<Vec<_>>>()?;
    Ok(crate::CouplingExecutionResult { discharges })
}

fn wasm_error_is_timeout(message: &str) -> bool {
    message.contains("all fuel consumed") || message.contains("wasm trap: interrupt")
}

#[cfg(test)]
mod tests {
    use crate::kernel::history::EventStore as _;

    #[tokio::test]
    async fn runtime_services_dispatch_custom_wasm_coupling_from_registry() {
        let root = temp_dir("wasm-coupling-runtime-services");
        let output = serde_json::json!({
            "abi": verlet_abi::COUPLING_DISCHARGE_ABI,
            "events": [{
                "stream": "derived:counter",
                "kind": "placement.decision",
                "payload": {"count": 1}
            }]
        });
        let operation = publish_coupling_operation(&root, "counter", "run", &output).await;
        let store = std::sync::Arc::new(crate::InMemorySessionStore::default());
        let coordinates = crate::ThreadCoordinates::new("tenant", "user", "session");
        let coupling = test_coupling(
            "org.example.counter",
            "counter",
            "run",
            &operation,
            "derived:counter",
            vec![crate::EventKind::PlacementDecision],
        );
        let services =
            crate::RuntimeServices::new(store.clone(), crate::RuntimeExecutionPolicy::default())
                .with_bound_coupling_set(crate::BoundCouplingSet::new("snapshot-a", vec![coupling]))
                .with_operation_registry_root(&root);

        services
            .append_thread_event(
                &coordinates,
                crate::NewEventRecord::witnessed(
                    coordinates.clone(),
                    crate::EventKind::TurnCompleted,
                    serde_json::json!({"turn_id": "t1"}),
                ),
            )
            .await
            .unwrap();

        let derived_stream =
            crate::EventStreamId::new(format!("derived:counter:{}", coordinates.thread_id));
        let derived = store.read_events(&derived_stream, None).await.unwrap();
        assert_eq!(derived.len(), 1);
        assert_eq!(derived[0].payload["count"], 1);
        let control_stream =
            crate::EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let control = store.read_events(&control_stream, None).await.unwrap();
        assert_eq!(control[0].kind, crate::EventKind::CouplingRunCompleted);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn wasm_coupling_discharges_with_kernel_stamped_provenance_and_receipt() {
        let root = temp_dir("wasm-coupling-completed");
        let output = serde_json::json!({
            "abi": verlet_abi::COUPLING_DISCHARGE_ABI,
            "events": [{
                "stream": "derived:counter",
                "kind": "placement.decision",
                "payload": {"count": 3},
                "provenance": {"guest": "forged"}
            }]
        });
        let operation = publish_coupling_operation(&root, "counter", "run", &output).await;
        let store = crate::InMemorySessionStore::default();
        let coordinates = crate::ThreadCoordinates::new("tenant", "user", "session");
        let appended = append_turn_completed(&store, &coordinates).await;
        let coupling = test_coupling(
            "org.example.counter",
            "counter",
            "run",
            &operation,
            "derived:counter",
            vec![crate::EventKind::PlacementDecision],
        );
        let executor = crate::kernel::wasm_couplings::WasmCouplingExecutor::new(&root);
        let scheduler = crate::CouplingScheduler::new(&store, &executor);

        let receipt = scheduler
            .run_batch(
                &crate::BoundCouplingSet::new("snapshot-a", vec![coupling]),
                appended,
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs[0].status, crate::CouplingRunStatus::Completed);
        assert_eq!(receipt.runs[0].discharged_event_ids.len(), 1);
        let derived_stream = scheduler.stream_id_for(&coordinates, "derived:counter");
        let derived = store.read_events(&derived_stream, None).await.unwrap();
        assert_eq!(derived.len(), 1);
        assert_eq!(derived[0].origin, crate::EventOrigin::Discharged);
        assert_eq!(
            derived[0].provenance.discharged_by.as_deref(),
            Some("coupling:org.example.counter")
        );
        assert_eq!(
            derived[0].provenance.function.as_deref(),
            Some(format!("op://counter/run@sha256:{}", operation.active_artifact_hash).as_str())
        );
        assert_eq!(derived[0].provenance.source_event_ids.len(), 1);
        assert_eq!(derived[0].payload["count"], 3);
        assert_eq!(
            derived[0].provenance,
            crate::EventProvenance {
                source_streams: vec![crate::EventStreamId::for_thread(&coordinates)],
                source_event_ids: vec![receipt.runs[0].trigger_event_id],
                discharged_by: Some("coupling:org.example.counter".to_string()),
                function: Some(format!(
                    "op://counter/run@sha256:{}",
                    operation.active_artifact_hash
                )),
                config_hash: Some("sha256:test".to_string()),
                ..crate::EventProvenance::default()
            }
        );
        let control_events = store
            .read_events(&scheduler.stream_id_for(&coordinates, "control"), None)
            .await
            .unwrap();
        assert_eq!(
            control_events[0].kind,
            crate::EventKind::CouplingRunCompleted
        );
        assert_eq!(
            control_events[0].payload["discharged_event_ids"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn wasm_coupling_sink_violation_fails_without_partial_append() {
        let root = temp_dir("wasm-coupling-sink-violation");
        let output = serde_json::json!({
            "abi": verlet_abi::COUPLING_DISCHARGE_ABI,
            "events": [{
                "stream": "derived:counter",
                "kind": "loop.completed",
                "payload": {}
            }]
        });
        let operation = publish_coupling_operation(&root, "counter", "run", &output).await;
        let store = crate::InMemorySessionStore::default();
        let coordinates = crate::ThreadCoordinates::new("tenant", "user", "session");
        let appended = append_turn_completed(&store, &coordinates).await;
        let coupling = test_coupling(
            "org.example.counter",
            "counter",
            "run",
            &operation,
            "derived:counter",
            vec![crate::EventKind::PlacementDecision],
        );
        let executor = crate::kernel::wasm_couplings::WasmCouplingExecutor::new(&root);
        let scheduler = crate::CouplingScheduler::new(&store, &executor);

        let receipt = scheduler
            .run_batch(
                &crate::BoundCouplingSet::new("snapshot-a", vec![coupling]),
                appended,
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs[0].status, crate::CouplingRunStatus::Failed);
        assert!(
            receipt.runs[0]
                .reason
                .as_deref()
                .unwrap()
                .contains("sink-violation")
        );
        let derived = store
            .read_events(
                &scheduler.stream_id_for(&coordinates, "derived:counter"),
                None,
            )
            .await
            .unwrap();
        assert!(derived.is_empty());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn wasm_coupling_discharge_budget_fails_without_partial_append() {
        let root = temp_dir("wasm-coupling-budget");
        let output = serde_json::json!({
            "abi": verlet_abi::COUPLING_DISCHARGE_ABI,
            "events": [
                {"stream": "derived:counter", "kind": "placement.decision", "payload": {"n": 1}},
                {"stream": "derived:counter", "kind": "placement.decision", "payload": {"n": 2}}
            ]
        });
        let operation = publish_coupling_operation(&root, "counter", "run", &output).await;
        let store = crate::InMemorySessionStore::default();
        let coordinates = crate::ThreadCoordinates::new("tenant", "user", "session");
        let appended = append_turn_completed(&store, &coordinates).await;
        let mut coupling = test_coupling(
            "org.example.counter",
            "counter",
            "run",
            &operation,
            "derived:counter",
            vec![crate::EventKind::PlacementDecision],
        );
        coupling.budget.max_discharge_events = Some(1);
        let executor = crate::kernel::wasm_couplings::WasmCouplingExecutor::new(&root);
        let scheduler = crate::CouplingScheduler::new(&store, &executor);

        let receipt = scheduler
            .run_batch(
                &crate::BoundCouplingSet::new("snapshot-a", vec![coupling]),
                appended,
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs[0].status, crate::CouplingRunStatus::Failed);
        assert!(
            receipt.runs[0]
                .reason
                .as_deref()
                .unwrap()
                .contains("budget")
        );
        let derived = store
            .read_events(
                &scheduler.stream_id_for(&coordinates, "derived:counter"),
                None,
            )
            .await
            .unwrap();
        assert!(derived.is_empty());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn wasm_coupling_timeout_fails_with_paused_time() {
        let root = temp_dir("wasm-coupling-timeout");
        let operation = publish_spin_operation(&root, "spinner", "run").await;
        let store = crate::InMemorySessionStore::default();
        let coordinates = crate::ThreadCoordinates::new("tenant", "user", "session");
        let appended = append_turn_completed(&store, &coordinates).await;
        let mut coupling = test_coupling(
            "org.example.spinner",
            "spinner",
            "run",
            &operation,
            "derived:counter",
            vec![crate::EventKind::PlacementDecision],
        );
        coupling.budget.max_ms = Some(10);
        let executor = crate::kernel::wasm_couplings::WasmCouplingExecutor::new(&root);
        let scheduler = crate::CouplingScheduler::new(&store, &executor);
        let bindings = crate::BoundCouplingSet::new("snapshot-a", vec![coupling]);
        let run = scheduler.run_batch(&bindings, appended);
        tokio::pin!(run);
        let receipt = loop {
            tokio::select! {
                receipt = &mut run => break receipt.unwrap(),
                _ = tokio::task::yield_now() => {
                    tokio::time::advance(std::time::Duration::from_millis(10)).await;
                }
            }
        };

        assert_eq!(receipt.runs[0].status, crate::CouplingRunStatus::Failed);
        assert!(
            receipt.runs[0]
                .reason
                .as_deref()
                .unwrap()
                .contains("timeout")
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn wasm_coupling_rejects_effectful_import_without_partial_append() {
        let root = temp_dir("wasm-coupling-effectful-import");
        let operation = publish_operation(&root, "httpy", "run", http_import_guest("run")).await;
        let store = crate::InMemorySessionStore::default();
        let coordinates = crate::ThreadCoordinates::new("tenant", "user", "session");
        let appended = append_turn_completed(&store, &coordinates).await;
        let coupling = test_coupling(
            "org.example.httpy",
            "httpy",
            "run",
            &operation,
            "derived:counter",
            vec![crate::EventKind::PlacementDecision],
        );
        let executor = crate::kernel::wasm_couplings::WasmCouplingExecutor::new(&root);
        let scheduler = crate::CouplingScheduler::new(&store, &executor);

        let receipt = scheduler
            .run_batch(
                &crate::BoundCouplingSet::new("snapshot-a", vec![coupling]),
                appended,
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs[0].status, crate::CouplingRunStatus::Failed);
        assert!(
            receipt.runs[0]
                .reason
                .as_deref()
                .unwrap()
                .contains("pure-compute")
        );
        let derived = store
            .read_events(
                &scheduler.stream_id_for(&coordinates, "derived:counter"),
                None,
            )
            .await
            .unwrap();
        assert!(derived.is_empty());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn wasm_coupling_cyclic_trigger_halts_at_depth_eight() {
        let root = temp_dir("wasm-coupling-depth");
        let output = serde_json::json!({
            "abi": verlet_abi::COUPLING_DISCHARGE_ABI,
            "events": [{
                "stream": "control",
                "kind": "turn.completed",
                "payload": {}
            }]
        });
        let operation = publish_coupling_operation(&root, "loop", "run", &output).await;
        let store = crate::InMemorySessionStore::default();
        let coordinates = crate::ThreadCoordinates::new("tenant", "user", "session");
        let appended = append_turn_completed(&store, &coordinates).await;
        let coupling = test_coupling(
            "org.example.loop",
            "loop",
            "run",
            &operation,
            "control",
            vec![crate::EventKind::TurnCompleted],
        );
        let executor = crate::kernel::wasm_couplings::WasmCouplingExecutor::new(&root);
        let scheduler = crate::CouplingScheduler::with_config(
            &store,
            &executor,
            crate::CouplingSchedulerConfig::default(),
        );

        let receipt = scheduler
            .run_batch(
                &crate::BoundCouplingSet::new("snapshot-a", vec![coupling]),
                appended,
            )
            .await
            .unwrap();

        assert_eq!(
            receipt
                .runs
                .iter()
                .filter(|run| run.status == crate::CouplingRunStatus::Completed)
                .count(),
            9
        );
        assert!(
            receipt
                .runs
                .iter()
                .any(|run| run.status == crate::CouplingRunStatus::Skipped
                    && run.reason.as_deref() == Some("depth_limit_exhausted"))
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn wasm_coupling_reuses_validated_runtime_for_same_artifact() {
        let root = temp_dir("wasm-coupling-cache");
        let output = serde_json::json!({
            "abi": verlet_abi::COUPLING_DISCHARGE_ABI,
            "events": [{
                "stream": "derived:counter",
                "kind": "placement.decision",
                "payload": {"count": 1}
            }]
        });
        let operation = publish_coupling_operation(&root, "counter", "run", &output).await;
        let store = crate::InMemorySessionStore::default();
        let coordinates = crate::ThreadCoordinates::new("tenant", "user", "session");
        let coupling = test_coupling(
            "org.example.counter",
            "counter",
            "run",
            &operation,
            "derived:counter",
            vec![crate::EventKind::PlacementDecision],
        );
        let coupling_set = crate::BoundCouplingSet::new("snapshot-a", vec![coupling]);
        let executor = crate::kernel::wasm_couplings::WasmCouplingExecutor::new(&root);
        let scheduler = crate::CouplingScheduler::new(&store, &executor);

        let first = append_turn_completed(&store, &coordinates).await;
        scheduler.run_batch(&coupling_set, first).await.unwrap();
        let second = append_turn_completed(&store, &coordinates).await;
        scheduler.run_batch(&coupling_set, second).await.unwrap();

        let stats = executor.cache_stats();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.fills, 1);
        assert_eq!(stats.hits, 1);
        let _ = std::fs::remove_dir_all(root);
    }

    async fn append_turn_completed(
        store: &crate::InMemorySessionStore,
        coordinates: &crate::ThreadCoordinates,
    ) -> Vec<crate::EventRecord> {
        store
            .append_events(
                &crate::EventStreamId::for_thread(coordinates),
                vec![crate::NewEventRecord::witnessed(
                    coordinates.clone(),
                    crate::EventKind::TurnCompleted,
                    serde_json::json!({"turn_id": "t1"}),
                )],
            )
            .await
            .unwrap()
    }

    fn test_coupling(
        id: &str,
        record_name: &str,
        operation_name: &str,
        operation: &crate::PublishedOperationRecord,
        sink_stream: &str,
        sink_kinds: Vec<crate::EventKind>,
    ) -> crate::BoundCoupling {
        crate::BoundCoupling {
            id: id.to_string(),
            role: if sink_stream == "control" {
                crate::CouplingRole::Controller
            } else {
                crate::CouplingRole::Projection
            },
            trigger_kind: crate::EventKind::TurnCompleted,
            trigger_match: Default::default(),
            trigger_quota: Default::default(),
            source_selectors: vec![crate::BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![crate::EventKind::TurnCompleted],
                scope: None,
                since: None,
            }],
            sink: crate::BoundCouplingSink {
                stream: sink_stream.to_string(),
                kinds: sink_kinds,
            },
            function_ref: format!(
                "op://{record_name}/{operation_name}@sha256:{}",
                operation.active_artifact_hash
            ),
            function: crate::BoundCouplingFunction {
                name: record_name.to_string(),
                artifact_hash: operation.active_artifact_hash.clone(),
                operation_name: Some(operation_name.to_string()),
            },
            grants: vec![
                "stream.read:thread".to_string(),
                format!("stream.write:{sink_stream}"),
            ],
            budget: Default::default(),
            config: serde_json::Value::Null,
            config_hash: "sha256:test".to_string(),
        }
    }

    async fn publish_coupling_operation(
        root: &std::path::Path,
        record_name: &str,
        operation_name: &str,
        output: &serde_json::Value,
    ) -> crate::PublishedOperationRecord {
        publish_operation(
            root,
            record_name,
            operation_name,
            coupling_guest(operation_name, output),
        )
        .await
    }

    async fn publish_spin_operation(
        root: &std::path::Path,
        record_name: &str,
        operation_name: &str,
    ) -> crate::PublishedOperationRecord {
        publish_operation(
            root,
            record_name,
            operation_name,
            spin_guest(operation_name),
        )
        .await
    }

    async fn publish_operation(
        root: &std::path::Path,
        record_name: &str,
        _operation_name: &str,
        wat: String,
    ) -> crate::PublishedOperationRecord {
        std::fs::create_dir_all(root).unwrap();
        let wasm = wat::parse_str(wat).expect("coupling test WAT should compile");
        let artifact_path = root.join(format!("{record_name}.wasm"));
        std::fs::write(&artifact_path, wasm).unwrap();
        crate::LocalOperationRegistry::new(root)
            .publish_artifact(crate::PublishOperationRequest {
                name: record_name.to_string(),
                artifact_path: artifact_path.clone(),
                source: crate::PublishedOperationSource::Wasm {
                    bin_path: artifact_path,
                },
                interface: None,
                capability_grants: Default::default(),
                metadata: Default::default(),
            })
            .await
            .unwrap()
    }

    fn coupling_guest(operation_name: &str, output: &serde_json::Value) -> String {
        let manifest = manifest(operation_name);
        let output = serde_json::to_string(output).unwrap();
        format!(
            r#"
(module
  (import "cooldis_0.1" "sink_write" (func $sink_write (param i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 4096) "{manifest}")
  (data (i32.const 8192) "{output}")
  (func (export "__verlet_describe_module__") (param $sink i32) (result i32)
    i32.const 0
    i32.const {manifest_len}
    i32.store
    local.get $sink
    i32.const 4096
    i32.const 0
    call $sink_write)
  (func (export "__verlet_call_operation__")
    (param $op i32)
    (param $invocation i32)
    (param $source i32)
    (param $output i32)
    (param $events i32)
    (result i32)
    local.get $op
    i32.const 1
    i32.ne
    if
      i32.const 2
      return
    end
    i32.const 0
    i32.const {output_len}
    i32.store
    local.get $output
    i32.const 8192
    i32.const 0
    call $sink_write
    drop
    i32.const 0))
"#,
            manifest = wat_bytes(manifest.as_bytes()),
            manifest_len = manifest.len(),
            output = wat_bytes(output.as_bytes()),
            output_len = output.len(),
        )
    }

    fn spin_guest(operation_name: &str) -> String {
        let manifest = manifest(operation_name);
        format!(
            r#"
(module
  (import "cooldis_0.1" "sink_write" (func $sink_write (param i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 4096) "{manifest}")
  (func (export "__verlet_describe_module__") (param $sink i32) (result i32)
    i32.const 0
    i32.const {manifest_len}
    i32.store
    local.get $sink
    i32.const 4096
    i32.const 0
    call $sink_write)
  (func (export "__verlet_call_operation__")
    (param $op i32)
    (param $invocation i32)
    (param $source i32)
    (param $output i32)
    (param $events i32)
    (result i32)
    (loop $spin
      br $spin)
    i32.const 0))
"#,
            manifest = wat_bytes(manifest.as_bytes()),
            manifest_len = manifest.len(),
        )
    }

    fn http_import_guest(operation_name: &str) -> String {
        let manifest = manifest(operation_name);
        let output = serde_json::to_string(&serde_json::json!({
            "abi": verlet_abi::COUPLING_DISCHARGE_ABI,
            "events": [{
                "stream": "derived:counter",
                "kind": "placement.decision",
                "payload": {"count": 1}
            }]
        }))
        .unwrap();
        format!(
            r#"
(module
  (import "cooldis_0.1" "sink_write" (func $sink_write (param i32 i32 i32) (result i32)))
  (import "cooldis_0.1" "http_request" (func $http_request (param i32 i32 i32 i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 4096) "{manifest}")
  (data (i32.const 8192) "{output}")
  (func (export "__verlet_describe_module__") (param $sink i32) (result i32)
    i32.const 0
    i32.const {manifest_len}
    i32.store
    local.get $sink
    i32.const 4096
    i32.const 0
    call $sink_write)
  (func (export "__verlet_call_operation__")
    (param $op i32)
    (param $invocation i32)
    (param $source i32)
    (param $output i32)
    (param $events i32)
    (result i32)
    local.get $op
    i32.const 1
    i32.ne
    if
      i32.const 2
      return
    end
    i32.const 0
    i32.const {output_len}
    i32.store
    local.get $output
    i32.const 8192
    i32.const 0
    call $sink_write
    drop
    i32.const 0))
"#,
            manifest = wat_bytes(manifest.as_bytes()),
            manifest_len = manifest.len(),
            output = wat_bytes(output.as_bytes()),
            output_len = output.len(),
        )
    }

    fn manifest(operation_name: &str) -> String {
        serde_json::to_string(&crate::WasmOperationManifest {
            abi: "cooldis.operation/0.1".to_string(),
            operations: vec![crate::WasmOperationDefinition {
                id: 1,
                name: operation_name.to_string(),
                input: crate::WasmOperationValueKind::Json,
                output: crate::WasmOperationValueKind::Json,
                events: crate::WasmOperationEventKind::None,
                mode: crate::WasmOperationMode::Sync,
                required_capabilities: Vec::new(),
            }],
        })
        .unwrap()
    }

    fn wat_bytes(bytes: &[u8]) -> String {
        bytes
            .iter()
            .map(|byte| format!("\\{:02x}", byte))
            .collect::<Vec<_>>()
            .join("")
    }

    fn temp_dir(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("{label}-{}", uuid::Uuid::now_v7()))
    }
}
