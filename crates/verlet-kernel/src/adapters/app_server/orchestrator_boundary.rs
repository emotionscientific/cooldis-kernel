//! Orchestrator boundary v0 (ADR 0009): typed envelope submission
//! (`ingress/submit`) and client streams (`stream/append`, `stream/read`).
//!
//! Architect skeleton for EMO-532. Param/result shapes and validation rules
//! are normative per the ADR; handler bodies are filled by the
//! implementation ticket. Wire-up (dispatcher arms, authority tables, both
//! classifiers, drift test, docs) follows the standard method checklist.

use verlet_history::EventStore as _;
/// Stream id prefix for client-owned streams. Grammar per ADR 0009:
/// `client:` followed by one or more `[a-z0-9][a-z0-9-]*` segments joined
/// by `:`. Recovery and ingress sweeps skip this prefix exactly as they
/// skip `sync-ingress:`.
pub(super) const CLIENT_STREAM_PREFIX: &str = "client:";

/// Payload-schema cohort reserved to the kernel; `stream/append` rejects
/// declared schema ids in it so client cohorts can never collide.
pub(super) const RESERVED_SCHEMA_COHORT_PREFIX: &str = "cooldis.";

/// Turn-entry surface name `ingress/submit` registers in
/// `TURN_ENTRY_SURFACES` (admission coverage ratchet).
pub(super) const ENVELOPE_INGRESS_SURFACE: &str =
    crate::kernel::admission::APP_SERVER_ENVELOPE_INGRESS_SURFACE;

pub(super) const CLIENT_STREAM_THREAD_NAMESPACE: &str = "530827e2-57cf-405e-9ca7-bb08b18c1ab0";

// ---------------------------------------------------------------------------
// ingress/submit

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct IngressSubmitParams {
    pub(super) thread_id: String,
    pub(super) input: serde_json::Value,
    pub(super) delivery: IngressSubmitDelivery,
    #[serde(default)]
    pub(super) dedupe_key: Option<IngressSubmitDedupeKey>,
    #[serde(default)]
    pub(super) correlation_id: Option<String>,
    /// `"attested"` (default). `"recorded"` is reserved for the
    /// foreign-harness lane and rejected until that lane exists.
    #[serde(default)]
    pub(super) tier: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct IngressSubmitDelivery {
    pub(super) delivery_id: String,
    #[serde(default)]
    pub(super) attempt: Option<u32>,
    #[serde(default)]
    pub(super) metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct IngressSubmitDedupeKey {
    pub(super) scope: String,
    pub(super) key: String,
}

// ---------------------------------------------------------------------------
// stream/append

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StreamAppendParams {
    pub(super) stream: String,
    pub(super) records: Vec<StreamAppendRecord>,
    /// Optional fence: the append succeeds only if the stream's next
    /// sequence equals this value (compare-and-set for writers; the
    /// orchestrator's placement lease fence rides on it). Uses the store's
    /// fenced append; a stale expectation fails closed with error data
    /// `{ "expected": …, "actual": … }`.
    #[serde(default)]
    pub(super) expected_sequence: Option<u64>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StreamAppendRecord {
    /// Lowercase dotted lifecycle name, `[a-z]+(\.[a-z_]+)+`.
    pub(super) kind: String,
    /// Declared client cohort id, `[a-z][a-z0-9.-]*/[0-9]+`, not in the
    /// reserved kernel cohort. Recorded, not interpreted.
    pub(super) payload_schema: String,
    pub(super) payload: serde_json::Value,
}

// ---------------------------------------------------------------------------
// stream/read

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StreamReadParams {
    pub(super) stream: String,
    #[serde(default)]
    pub(super) stream_cursor: Option<verlet_history::StreamCursorV1>,
    /// 1..=500, default 100 (same clamp as `thread/events/list`).
    #[serde(default)]
    pub(super) limit: Option<usize>,
    #[serde(default)]
    pub(super) kinds: Vec<String>,
}

// ---------------------------------------------------------------------------
// Validation (normative rules from ADR 0009; pure, unit-testable)

/// Accepts `client:<name>` per the ADR grammar, rejects everything else.
pub(super) fn validate_client_stream_id(
    stream: &str,
) -> Result<(), crate::adapters::app_server::connection::JsonRpcErrorError> {
    let Some(name) = stream.strip_prefix(CLIENT_STREAM_PREFIX) else {
        return Err(crate::adapters::app_server::connection::jsonrpc_error(
            -32602,
            "stream must be a client stream id beginning with client:",
        ));
    };
    let valid = !name.is_empty()
        && name.split(':').all(|segment| {
            let mut chars = segment.chars();
            chars
                .next()
                .is_some_and(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit())
                && chars.all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
        });
    if valid {
        Ok(())
    } else {
        Err(crate::adapters::app_server::connection::jsonrpc_error(
            -32602,
            "client stream id must match client:<name> with lowercase alphanumeric or hyphen segments",
        ))
    }
}

/// Kind grammar + declared-schema grammar + reserved-cohort rejection.
pub(super) fn validate_append_record(
    record: &StreamAppendRecord,
) -> Result<(), crate::adapters::app_server::connection::JsonRpcErrorError> {
    let kind_segments = record.kind.split('.').collect::<Vec<_>>();
    let kind_valid = kind_segments.len() >= 2
        && kind_segments[0].chars().all(|ch| ch.is_ascii_lowercase())
        && !kind_segments[0].is_empty()
        && kind_segments[1..].iter().all(|segment| {
            !segment.is_empty()
                && segment
                    .chars()
                    .all(|ch| ch.is_ascii_lowercase() || ch == '_')
        });
    if !kind_valid {
        return Err(crate::adapters::app_server::connection::jsonrpc_error(
            -32602,
            format!("record kind {:?} must be lowercase dotted", record.kind),
        ));
    }

    let schema_valid = record
        .payload_schema
        .split_once('/')
        .is_some_and(|(cohort, version)| {
            !version.is_empty()
                && !version.contains('/')
                && version.chars().all(|ch| ch.is_ascii_digit())
                && cohort
                    .chars()
                    .next()
                    .is_some_and(|ch| ch.is_ascii_lowercase())
                && cohort.chars().skip(1).all(|ch| {
                    ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '-')
                })
        });
    if !schema_valid {
        return Err(crate::adapters::app_server::connection::jsonrpc_error(
            -32602,
            format!(
                "record payloadSchema {:?} must match [a-z][a-z0-9.-]*/[0-9]+",
                record.payload_schema
            ),
        ));
    }
    if record
        .payload_schema
        .starts_with(RESERVED_SCHEMA_COHORT_PREFIX)
    {
        return Err(crate::adapters::app_server::connection::jsonrpc_error(
            -32602,
            format!(
                "record payloadSchema {:?} uses the reserved kernel cohort",
                record.payload_schema
            ),
        ));
    }
    Ok(())
}

/// `"attested"` or absent passes; `"recorded"` and anything else rejects
/// with a validation error naming the reserved lane.
pub(super) fn validate_tier(
    tier: Option<&str>,
) -> Result<(), crate::adapters::app_server::connection::JsonRpcErrorError> {
    match tier {
        None | Some("attested") => Ok(()),
        Some("recorded") => Err(crate::adapters::app_server::connection::jsonrpc_error(
            -32602,
            "guarantee tier recorded is reserved for the unimplemented foreign-harness lane",
        )),
        Some(other) => Err(crate::adapters::app_server::connection::jsonrpc_error(
            -32602,
            format!("unsupported guarantee tier {other:?}; expected attested"),
        )),
    }
}

// ---------------------------------------------------------------------------
// Handlers (bodies: implementation ticket)

impl crate::adapters::app_server::VerletAppServer {
    /// `ingress/submit`: build the envelope server-side (principal from the
    /// session, `via: "caller:{session_id}"`), witness
    /// `io.ingress.received` on the control stream, run admission before
    /// scheduling, dedupe on the effective key (duplicates return the
    /// original ingress event id with `deduped: true` and schedule
    /// nothing). Result per ADR 0009.
    pub(super) async fn ingress_submit(
        &self,
        connection: &crate::adapters::app_server::connection::ConnectionState,
        params: IngressSubmitParams,
    ) -> Result<serde_json::Value, crate::adapters::app_server::connection::JsonRpcErrorError> {
        validate_tier(params.tier.as_deref())?;
        let input_values = params.input.as_array().cloned().ok_or_else(|| {
            crate::adapters::app_server::connection::jsonrpc_error(
                -32602,
                "ingress/submit input must be an array",
            )
        })?;
        let handle = self.handle_for_thread(&params.thread_id).await?;
        let coordinates = handle.context().coordinates.clone();
        connection.subscribe_thread(handle.clone()).await;

        let principal = verlet_io_core::IoPrincipal::new(
            self.inner.tenant_id.clone(),
            connection.resolved_principal.principal_id.to_string(),
            format!("caller:{}", connection.witnessed_session_id),
        );
        let source = verlet_io_core::IoSource::new(
            "app-server-envelope",
            connection.resolved_principal.principal_id.to_string(),
        );
        let mut envelope = verlet_io_core::IngressEnvelope::new(
            source,
            verlet_io_core::IoConversation::new(
                params.thread_id.clone(),
                verlet_io_core::ConversationKind::Thread,
            ),
            verlet_io_core::IngressContent::Event {
                kind: "app-server.turn-input".to_string(),
                payload: params.input.clone(),
            },
            crate::adapters::app_server::connection::now_ms(),
        )
        .with_delivery(verlet_io_core::IoDelivery {
            delivery_id: params.delivery.delivery_id,
            attempt: params.delivery.attempt,
            metadata: params.delivery.metadata,
        })
        .with_principal(principal.clone());
        if let Some(dedupe_key) = params.dedupe_key {
            envelope = envelope.with_dedupe_key(verlet_io_core::IoDedupeKey::new(
                dedupe_key.scope,
                dedupe_key.key,
            ));
        }
        if let Some(correlation_id) = params.correlation_id {
            envelope
                .metadata
                .insert("correlation_id".to_string(), correlation_id);
        }
        envelope
            .metadata
            .insert("guarantee_tier".to_string(), "attested".to_string());
        envelope.require_witnessed().map_err(|err| {
            crate::adapters::app_server::connection::jsonrpc_error(-32602, err.to_string())
        })?;
        let dedupe_key = envelope
            .effective_dedupe_key()
            .expect("witnessed envelope has an effective dedupe key")
            .stable_key();
        let envelope_value = serde_json::to_value(&envelope)
            .map_err(crate::adapters::app_server::connection::json_codec_error)?;
        let envelope_digest = crate::agent::manifest_bind::canonical_json_hash(&envelope_value)
            .map_err(crate::adapters::app_server::connection::internal_error)?;
        let payload = verlet_history::IoIngressReceivedPayload {
            route_id: Some(format!("surface:{ENVELOPE_INGRESS_SURFACE}")),
            dedupe_key: Some(dedupe_key.clone()),
            external_conversation_id: Some(params.thread_id.clone()),
            external_actor_id: None,
            external_message_id: None,
            content: Some(
                serde_json::to_value(&envelope.content)
                    .map_err(crate::adapters::app_server::connection::json_codec_error)?,
            ),
            envelope_digest,
        };
        let ingress_record = crate::adapters::app_server::connection::rpc_ingress_received_record(
            coordinates.clone(),
            payload,
            &principal,
            Some(&envelope.metadata),
        )?;
        let store = verlet_history_sqlite::SqliteSessionStore::open(&self.inner.session_store_path)
            .await
            .map_err(history_jsonrpc_error)?
            .with_lease_epoch(self.inner.lease_epoch);
        let stream_id = crate::kernel::control_decision::control_stream_id(&coordinates);
        let ingress_event = loop {
            let events = store
                .read_events(&stream_id, None)
                .await
                .map_err(history_jsonrpc_error)?;
            if let Some(existing) = events.iter().find(|event| {
                event.kind == verlet_history::EventKind::IoIngressReceived
                    && event
                        .payload
                        .get("dedupe_key")
                        .and_then(serde_json::Value::as_str)
                        == Some(dedupe_key.as_str())
            }) {
                return Ok(ingress_submit_result(existing.id, true));
            }
            let expected = events
                .last()
                .map(|event| verlet_history::EventSequence::new(event.sequence.get() + 1))
                .unwrap_or_else(|| verlet_history::EventSequence::new(1));
            match store
                .append_events_fenced(&stream_id, expected, vec![ingress_record.clone()])
                .await
            {
                Ok(mut appended) => {
                    break appended.pop().ok_or_else(|| {
                        crate::adapters::app_server::connection::internal_error(
                            crate::kernel::runtime_host::VerletError::History(
                                "ingress witness append returned no record".to_string(),
                            ),
                        )
                    })?;
                }
                Err(verlet_history::HistoryError::AppendFenceConflict { .. }) => continue,
                Err(err) => return Err(history_jsonrpc_error(err)),
            }
        };

        let admission = crate::kernel::admission::AdmissionGateContext::surface_default(
            ENVELOPE_INGRESS_SURFACE,
            vec![ingress_event.id],
        )
        .map_err(crate::adapters::app_server::connection::internal_error)?;
        crate::kernel::admission::append_admission_decided(&handle, admission)
            .await
            .map_err(crate::adapters::app_server::connection::internal_error)?;

        let turn_id = format!("turn-{}", uuid::Uuid::now_v7());
        let input = crate::adapters::app_server::threads::turn_input_from_values(&input_values)
            .with_provider(self.inner.model_provider.clone())
            .with_model(self.inner.model.clone());
        let turn = {
            let mut state = self.inner.state.write().await;
            let thread = state.threads.get_mut(&params.thread_id).ok_or_else(|| {
                crate::adapters::app_server::connection::thread_not_found(&params.thread_id)
            })?;
            let turn = crate::adapters::app_server::threads::AppServerTurnState::new(
                turn_id.clone(),
                input_values.clone(),
            );
            if thread.preview.is_empty() {
                thread.preview =
                    crate::adapters::app_server::threads::user_input_preview(&input_values);
            }
            thread.updated_at_ms = crate::adapters::app_server::connection::now_ms();
            thread.active_turn_id = Some(turn_id.clone());
            let value = crate::adapters::app_server::threads::turn_json(&turn);
            thread.turns.insert(turn_id.clone(), turn);
            value
        };
        self.inner
            .supervisor
            .submit_admitted_turn_to(
                &coordinates,
                turn_id,
                input,
                verlet_runtime_contracts::TurnSubmissionMode::Queue,
                None,
            )
            .await
            .map_err(crate::adapters::app_server::connection::internal_error)?;
        connection
            .notify(
                "turn/started",
                serde_json::json!({"threadId": params.thread_id, "turn": turn}),
            )
            .await;
        Ok(ingress_submit_result(ingress_event.id, false))
    }

    /// `stream/append`: validate, then append the batch atomically as
    /// witnessed records (coordinates: thread_id = UUIDv5 of the full stream
    /// id, tenant from session identity; principal stamped). Fenced when
    /// `expected_sequence` is present. Host-effect witnessed.
    pub(super) async fn stream_append(
        &self,
        connection: &crate::adapters::app_server::connection::ConnectionState,
        params: StreamAppendParams,
    ) -> Result<serde_json::Value, crate::adapters::app_server::connection::JsonRpcErrorError> {
        validate_client_stream_id(&params.stream)?;
        if params.records.is_empty() {
            return Err(crate::adapters::app_server::connection::jsonrpc_error(
                -32602,
                "stream/append records must not be empty",
            ));
        }
        for record in &params.records {
            validate_append_record(record)?;
        }
        let coordinates = client_stream_coordinates(connection, &params.stream)?;
        let principal_id = connection.resolved_principal.principal_id.to_string();
        let records = params
            .records
            .into_iter()
            .map(|record| {
                let payload = verlet_history::ClientRecordAppendedPayload {
                    client_kind: record.kind,
                    client_schema: record.payload_schema,
                    principal_id: principal_id.clone(),
                    body: record.payload,
                };
                serde_json::to_value(payload)
                    .map(|payload| {
                        verlet_history::NewEventRecord::witnessed(
                            coordinates.clone(),
                            verlet_history::EventKind::ClientRecordAppended,
                            payload,
                        )
                    })
                    .map_err(crate::adapters::app_server::connection::json_codec_error)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let store = verlet_history_sqlite::SqliteSessionStore::open(&self.inner.session_store_path)
            .await
            .map_err(history_jsonrpc_error)?
            .with_lease_epoch(self.inner.lease_epoch);
        let stream_id = verlet_history::EventStreamId::new(params.stream.clone());
        let appended = match params.expected_sequence {
            Some(expected) => {
                let expected = i64::try_from(expected).map_err(|_| {
                    crate::adapters::app_server::connection::jsonrpc_error(
                        -32602,
                        "expectedSequence is larger than i64::MAX",
                    )
                })?;
                if expected < 1 {
                    return Err(crate::adapters::app_server::connection::jsonrpc_error(
                        -32602,
                        "expectedSequence must be at least 1",
                    ));
                }
                match store
                    .append_events_fenced(
                        &stream_id,
                        verlet_history::EventSequence::new(expected),
                        records,
                    )
                    .await
                {
                    Ok(appended) => appended,
                    Err(verlet_history::HistoryError::AppendFenceConflict {
                        expected_next_sequence,
                        actual_next_sequence,
                        ..
                    }) => {
                        return Err(crate::adapters::app_server::connection::JsonRpcErrorError {
                            code: -32004,
                            data: Some(serde_json::json!({
                                "expected": expected_next_sequence,
                                "actual": actual_next_sequence,
                            })),
                            message: "stream append fence conflict".to_string(),
                        });
                    }
                    Err(err) => return Err(history_jsonrpc_error(err)),
                }
            }
            None => store
                .append_events(&stream_id, records)
                .await
                .map_err(history_jsonrpc_error)?,
        };
        let records = appended
            .iter()
            .map(|event| serde_json::json!({"eventId": event.id, "sequence": event.sequence}))
            .collect::<Vec<_>>();
        Ok(serde_json::json!({"streamId": params.stream, "records": records}))
    }

    /// `stream/read`: client streams only; paging/cursor/kinds semantics
    /// mirror `thread/events/list`.
    pub(super) async fn stream_read(
        &self,
        _connection: &crate::adapters::app_server::connection::ConnectionState,
        params: StreamReadParams,
    ) -> Result<serde_json::Value, crate::adapters::app_server::connection::JsonRpcErrorError> {
        validate_client_stream_id(&params.stream)?;
        let stream_id = verlet_history::EventStreamId::new(params.stream);
        let store = verlet_history_sqlite::SqliteSessionStore::open(&self.inner.session_store_path)
            .await
            .map_err(history_jsonrpc_error)?
            .with_lease_epoch(self.inner.lease_epoch);
        let mut events = if let Some(cursor) = params.stream_cursor.as_ref() {
            store
                .read_events_after_cursor(&stream_id, cursor)
                .await
                .map_err(stream_read_history_error)?
        } else {
            store
                .read_events(&stream_id, None)
                .await
                .map_err(history_jsonrpc_error)?
        };
        if !params.kinds.is_empty() {
            let kinds = params
                .kinds
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>();
            events.retain(|event| {
                event
                    .payload
                    .get("client_kind")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|kind| kinds.contains(kind))
            });
        }
        let limit = params.limit.unwrap_or(100).clamp(1, 500);
        let mut page = events.into_iter().take(limit + 1).collect::<Vec<_>>();
        let stream_cursor = if page.len() > limit {
            page.pop();
            page.last().map(verlet_history::EventRecord::cursor_v1)
        } else {
            None
        };
        let data = page
            .iter()
            .map(client_stream_record_json)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(serde_json::json!({"data": data, "streamCursor": stream_cursor}))
    }
}

fn ingress_submit_result(
    ingress_event_id: verlet_history::EventRecordId,
    deduped: bool,
) -> serde_json::Value {
    serde_json::json!({
        "ingressEventId": ingress_event_id,
        "deduped": deduped,
        "admission": {"decision": "queue", "admissible": true},
    })
}

fn client_stream_coordinates(
    connection: &crate::adapters::app_server::connection::ConnectionState,
    stream: &str,
) -> Result<
    verlet_runtime_contracts::ThreadCoordinates,
    crate::adapters::app_server::connection::JsonRpcErrorError,
> {
    let namespace = uuid::Uuid::parse_str(CLIENT_STREAM_THREAD_NAMESPACE).map_err(|err| {
        crate::adapters::app_server::connection::internal_error(
            crate::kernel::runtime_host::VerletError::History(format!(
                "invalid client stream UUID namespace: {err}"
            )),
        )
    })?;
    let thread_id = verlet_runtime_contracts::ThreadId::parse_str(
        &uuid::Uuid::new_v5(&namespace, stream.as_bytes()).to_string(),
    )
    .map_err(|err| {
        crate::adapters::app_server::connection::internal_error(
            crate::kernel::runtime_host::VerletError::History(format!(
                "client stream coordinate UUID failed: {err}"
            )),
        )
    })?;
    Ok(verlet_runtime_contracts::ThreadCoordinates {
        tenant_id: connection.app.inner.tenant_id.clone(),
        user_id: connection.resolved_principal.principal_id.to_string(),
        session_id: connection.witnessed_session_id.clone(),
        thread_id,
    })
}

fn client_stream_record_json(
    record: &verlet_history::EventRecord,
) -> Result<serde_json::Value, crate::adapters::app_server::connection::JsonRpcErrorError> {
    if record.kind != verlet_history::EventKind::ClientRecordAppended {
        return Err(crate::adapters::app_server::connection::internal_error(
            crate::kernel::runtime_host::VerletError::History(format!(
                "client stream {} contains non-client carrier kind {}",
                record.stream_id, record.kind
            )),
        ));
    }
    let payload = serde_json::from_value::<verlet_history::ClientRecordAppendedPayload>(
        record.payload.clone(),
    )
    .map_err(crate::adapters::app_server::connection::json_codec_error)?;
    let mut value = serde_json::to_value(record.to_stream_record_v1())
        .map_err(crate::adapters::app_server::connection::json_codec_error)?;
    let object = value.as_object_mut().ok_or_else(|| {
        crate::adapters::app_server::connection::internal_error(
            crate::kernel::runtime_host::VerletError::History(
                "client stream record envelope did not encode as an object".to_string(),
            ),
        )
    })?;
    object.insert("kind".to_string(), serde_json::json!(payload.client_kind));
    object.insert(
        "payload_schema".to_string(),
        serde_json::json!(payload.client_schema),
    );
    object.insert(
        "principal_id".to_string(),
        serde_json::json!(payload.principal_id),
    );
    object.insert("payload".to_string(), payload.body);
    object.insert("eventId".to_string(), serde_json::json!(record.id));
    object.insert("atMs".to_string(), serde_json::json!(record.created_at_ms));
    Ok(value)
}

fn history_jsonrpc_error(
    error: verlet_history::HistoryError,
) -> crate::adapters::app_server::connection::JsonRpcErrorError {
    match error {
        verlet_history::HistoryError::StaleLeaseEpoch {
            stream_id,
            presented_epoch,
            minimum_epoch,
        } => crate::adapters::app_server::connection::JsonRpcErrorError {
            code: -32005,
            data: Some(serde_json::json!({
                "streamId": stream_id,
                "presentedEpoch": presented_epoch,
                "minimumEpoch": minimum_epoch,
            })),
            message: "journal lease epoch is stale".to_string(),
        },
        other => crate::adapters::app_server::connection::internal_error(
            crate::kernel::runtime_host::VerletError::History(other.to_string()),
        ),
    }
}

fn stream_read_history_error(
    error: verlet_history::HistoryError,
) -> crate::adapters::app_server::connection::JsonRpcErrorError {
    match error {
        verlet_history::HistoryError::StreamCursorStreamMismatch { .. }
        | verlet_history::HistoryError::StreamCursorMismatch { .. } => {
            crate::adapters::app_server::connection::jsonrpc_error(
                -32602,
                format!("malformed stream/read cursor: {error}"),
            )
        }
        verlet_history::HistoryError::Codec(message) if message.contains("stream cursor") => {
            crate::adapters::app_server::connection::jsonrpc_error(
                -32602,
                format!("malformed stream/read cursor: {message}"),
            )
        }
        other => history_jsonrpc_error(other),
    }
}

#[cfg(test)]
mod tests {

    fn record(
        kind: &str,
        payload_schema: &str,
    ) -> crate::adapters::app_server::orchestrator_boundary::StreamAppendRecord {
        crate::adapters::app_server::orchestrator_boundary::StreamAppendRecord {
            kind: kind.to_string(),
            payload_schema: payload_schema.to_string(),
            payload: serde_json::json!({"value": 1}),
        }
    }

    #[test]
    fn client_stream_id_validation_pins_the_wire_grammar() {
        for stream in ["client:a", "client:orch:fleet", "client:a-b:c0:d-9"] {
            crate::adapters::app_server::orchestrator_boundary::validate_client_stream_id(stream)
                .unwrap();
        }
        for stream in [
            "client:",
            "client::fleet",
            "client:orch:",
            "client:-orch",
            "client:Orch",
            "client:orch_fleet",
            ":client:orch",
            "thread:orch",
            "sync-ingress:orch",
        ] {
            assert!(
                crate::adapters::app_server::orchestrator_boundary::validate_client_stream_id(
                    stream
                )
                .is_err(),
                "accepted invalid client stream {stream:?}"
            );
        }
    }

    #[test]
    fn append_record_validation_pins_kind_schema_and_reserved_cohort_rules() {
        for valid in [
            record("placement.bound", "verlet.orch.placement.bound/1"),
            record("run.outcome_recorded", "a-b.client.v1/12"),
        ] {
            crate::adapters::app_server::orchestrator_boundary::validate_append_record(&valid)
                .unwrap();
        }
        for invalid in [
            record("placement", "verlet.orch.placement/1"),
            record("Placement.bound", "verlet.orch.placement/1"),
            record("placement.bound2", "verlet.orch.placement/1"),
            record("placement..bound", "verlet.orch.placement/1"),
            record("placement-bound.value", "verlet.orch.placement/1"),
            record("placement.bound", "cooldis.orch.placement/1"),
            record("placement.bound", "COOLDIS.orch.placement/1"),
            record("placement.bound", " cooldis.orch.placement/1"),
            record("placement.bound", "verlet_orch.placement/1"),
            record("placement.bound", "verlet.orch.placement"),
            record("placement.bound", "verlet.orch.placement/v1"),
        ] {
            assert!(
                crate::adapters::app_server::orchestrator_boundary::validate_append_record(
                    &invalid
                )
                .is_err(),
                "accepted invalid record {invalid:?}"
            );
        }
    }

    #[test]
    fn guarantee_tier_accepts_only_attested_or_default() {
        crate::adapters::app_server::orchestrator_boundary::validate_tier(None).unwrap();
        crate::adapters::app_server::orchestrator_boundary::validate_tier(Some("attested"))
            .unwrap();
        let recorded =
            crate::adapters::app_server::orchestrator_boundary::validate_tier(Some("recorded"))
                .unwrap_err();
        assert_eq!(recorded.code, -32602);
        assert!(recorded.message.contains("foreign-harness"));
        for tier in ["", "Attested", "recorded ", "foreign"] {
            assert!(
                crate::adapters::app_server::orchestrator_boundary::validate_tier(Some(tier))
                    .is_err()
            );
        }
    }
}
