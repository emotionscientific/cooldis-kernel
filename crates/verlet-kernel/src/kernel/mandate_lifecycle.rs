use std::str::FromStr as _;
pub const MIN_MANDATE_INTERVAL_MS: u64 = 60_000;
const SCHEDULE_SNAPSHOT_ID: &str = "schedule.v1";

#[derive(Clone, Debug, PartialEq)]
pub struct MandateStartRequest {
    pub schedule: crate::kernel::control_decision::MandateSchedulePayload,
    pub max_occurrences: Option<u32>,
    pub catch_up: Option<crate::kernel::control_decision::MandateCatchUpPolicy>,
    pub input_template: Option<String>,
    pub snapshot_id: Option<String>,
    pub expires_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MandateStartReceipt {
    pub event: crate::kernel::history::EventRecord,
    pub payload: crate::kernel::control_decision::MandateStartedPayload,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ActiveMandate {
    pub event: crate::kernel::history::EventRecord,
    pub payload: crate::kernel::control_decision::MandateStartedPayload,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MandateRevokeStatus {
    Revoked,
    AlreadyRevoked,
}

impl MandateRevokeStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Revoked => "revoked",
            Self::AlreadyRevoked => "already_revoked",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MandateRevokeReceipt {
    pub status: MandateRevokeStatus,
    pub start_event: crate::kernel::history::EventRecord,
    pub revoke_event: crate::kernel::history::EventRecord,
    pub payload: crate::kernel::control_decision::MandateRevokedPayload,
}

pub fn parse_mandate_event_id(
    value: &str,
) -> crate::VerletResult<crate::kernel::history::EventRecordId> {
    uuid::Uuid::parse_str(value)
        .map(crate::kernel::history::EventRecordId::from_uuid)
        .map_err(|err| {
            crate::VerletError::RuntimeExecution(format!(
                "mandate_event_id is not a valid event id: {err}"
            ))
        })
}

pub fn validate_mandate_start_request(
    request: &MandateStartRequest,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::VerletResult<()> {
    let catch_up = request.catch_up.unwrap_or_default();
    validate_schedule(&request.schedule, catch_up, now)?;
    let _ = mandate_expiry_ms(request.expires_at.as_deref(), now)?;
    Ok(())
}

pub async fn start_mandate(
    store: &dyn crate::kernel::history::EventStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    request: MandateStartRequest,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::VerletResult<MandateStartReceipt> {
    validate_mandate_start_request(&request, now)?;
    let catch_up = request.catch_up.unwrap_or_default();
    let expires_at_ms = mandate_expiry_ms(request.expires_at.as_deref(), now)?;
    let thread_id = coordinates.thread_id.to_string();
    let payload = crate::kernel::control_decision::MandateStartedPayload {
        subject: crate::kernel::control_decision::MandateSubject {
            thread_id: Some(thread_id.clone()),
            loop_id: None,
        },
        mandate_id: format!("mandate-{}", uuid::Uuid::now_v7()),
        snapshot_id: request
            .snapshot_id
            .filter(|snapshot_id| !snapshot_id.trim().is_empty())
            .unwrap_or_else(|| SCHEDULE_SNAPSHOT_ID.to_string()),
        thread_id: Some(thread_id),
        max_continuations: None,
        expires_at_ms,
        schedule: Some(request.schedule),
        max_occurrences: request.max_occurrences,
        catch_up: Some(catch_up),
        input_template: request.input_template,
    };
    let mut appended = store
        .append_events(
            &crate::control_stream_id(coordinates),
            vec![crate::kernel::history::NewEventRecord::witnessed(
                coordinates.clone(),
                crate::kernel::history::EventKind::MandateStarted,
                serde_json::to_value(&payload).map_err(json_error)?,
            )],
        )
        .await
        .map_err(|err| crate::VerletError::History(err.to_string()))?;
    let event = appended.pop().ok_or_else(|| {
        crate::VerletError::History("mandate.start appended no event".to_string())
    })?;
    Ok(MandateStartReceipt { event, payload })
}

fn mandate_expiry_ms(
    expires_at: Option<&str>,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::VerletResult<Option<i64>> {
    let Some(expires_at) = expires_at else {
        return Ok(None);
    };
    let parsed = chrono::DateTime::parse_from_rfc3339(expires_at).map_err(|err| {
        crate::VerletError::RuntimeExecution(format!(
            "mandate expiry {expires_at:?} must be an RFC3339 UTC instant: {err}"
        ))
    })?;
    if parsed.offset().local_minus_utc() != 0 {
        return Err(crate::VerletError::RuntimeExecution(format!(
            "mandate expiry {expires_at:?} must be an RFC3339 UTC instant"
        )));
    }
    let parsed = parsed.with_timezone(&chrono::Utc);
    if now > parsed {
        return Err(crate::VerletError::RuntimeExecution(format!(
            "mandate expiry {expires_at} is already expired"
        )));
    }
    Ok(Some(parsed.timestamp_millis()))
}

pub async fn list_active_mandates(
    store: &dyn crate::kernel::history::EventStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
) -> crate::VerletResult<Vec<ActiveMandate>> {
    let events = store
        .read_events(&crate::control_stream_id(coordinates), None)
        .await
        .map_err(|err| crate::VerletError::History(err.to_string()))?;
    active_mandates_from_events(coordinates, &events)
}

pub async fn revoke_mandate(
    store: &dyn crate::kernel::history::EventStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    mandate_event_id: crate::kernel::history::EventRecordId,
) -> crate::VerletResult<MandateRevokeReceipt> {
    let events = store
        .read_events(&crate::control_stream_id(coordinates), None)
        .await
        .map_err(|err| crate::VerletError::History(err.to_string()))?;
    let (start_event, start_payload) = mandate_start_event(coordinates, &events, mandate_event_id)?
        .ok_or_else(|| {
            crate::VerletError::RuntimeExecution(format!(
                "mandate event {mandate_event_id} is not active for thread {}",
                coordinates.thread_id
            ))
        })?;
    if let Some((revoke_event, payload)) =
        mandate_revoked_event(&events, mandate_event_id, &start_payload)?
    {
        return Ok(MandateRevokeReceipt {
            status: MandateRevokeStatus::AlreadyRevoked,
            start_event,
            revoke_event,
            payload,
        });
    }

    let payload = crate::kernel::control_decision::MandateRevokedPayload {
        subject: start_payload.subject.clone(),
        mandate_id: start_payload.mandate_id.clone(),
        mandate_event_id: Some(mandate_event_id.to_string()),
        snapshot_id: start_payload.snapshot_id.clone(),
        reason: None,
    };
    let mut appended = store
        .append_events(
            &crate::control_stream_id(coordinates),
            vec![crate::kernel::history::NewEventRecord::witnessed(
                coordinates.clone(),
                crate::kernel::history::EventKind::MandateRevoked,
                serde_json::to_value(&payload).map_err(json_error)?,
            )],
        )
        .await
        .map_err(|err| crate::VerletError::History(err.to_string()))?;
    let revoke_event = appended.pop().ok_or_else(|| {
        crate::VerletError::History("mandate.revoke appended no event".to_string())
    })?;
    Ok(MandateRevokeReceipt {
        status: MandateRevokeStatus::Revoked,
        start_event,
        revoke_event,
        payload,
    })
}

fn active_mandates_from_events(
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    events: &[crate::kernel::history::EventRecord],
) -> crate::VerletResult<Vec<ActiveMandate>> {
    let mut started = std::collections::BTreeMap::new();
    let mut revoked_event_ids = std::collections::BTreeSet::new();
    let mut revoked_mandate_ids = std::collections::BTreeSet::new();
    for event in events {
        match event.kind {
            crate::kernel::history::EventKind::MandateStarted => {
                let payload = decode_started(event)?;
                if mandate_targets_thread(&payload, coordinates) {
                    started.insert(event.id.to_string(), (event.clone(), payload));
                }
            }
            crate::kernel::history::EventKind::MandateRevoked => {
                let payload = decode_revoked(event)?;
                if revoked_targets_thread(&payload, coordinates) {
                    if let Some(mandate_event_id) = payload.mandate_event_id {
                        revoked_event_ids.insert(mandate_event_id);
                    }
                    revoked_mandate_ids.insert(payload.mandate_id);
                }
            }
            _ => {}
        }
    }
    let mut active = started
        .into_iter()
        .filter_map(|(event_id, (event, payload))| {
            if revoked_event_ids.contains(&event_id)
                || revoked_mandate_ids.contains(&payload.mandate_id)
            {
                None
            } else {
                Some(ActiveMandate { event, payload })
            }
        })
        .collect::<Vec<_>>();
    active.sort_by_key(|mandate| mandate.event.sequence.get());
    Ok(active)
}

fn mandate_start_event(
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    events: &[crate::kernel::history::EventRecord],
    mandate_event_id: crate::kernel::history::EventRecordId,
) -> crate::VerletResult<
    Option<(
        crate::kernel::history::EventRecord,
        crate::kernel::control_decision::MandateStartedPayload,
    )>,
> {
    for event in events {
        if event.id != mandate_event_id
            || event.kind != crate::kernel::history::EventKind::MandateStarted
        {
            continue;
        }
        let payload = decode_started(event)?;
        if mandate_targets_thread(&payload, coordinates) {
            return Ok(Some((event.clone(), payload)));
        }
    }
    Ok(None)
}

fn mandate_revoked_event(
    events: &[crate::kernel::history::EventRecord],
    mandate_event_id: crate::kernel::history::EventRecordId,
    started: &crate::kernel::control_decision::MandateStartedPayload,
) -> crate::VerletResult<
    Option<(
        crate::kernel::history::EventRecord,
        crate::kernel::control_decision::MandateRevokedPayload,
    )>,
> {
    for event in events {
        if event.kind != crate::kernel::history::EventKind::MandateRevoked {
            continue;
        }
        let payload = decode_revoked(event)?;
        if payload
            .mandate_event_id
            .as_deref()
            .map(|id| id == mandate_event_id.to_string())
            .unwrap_or(false)
            || payload.mandate_id == started.mandate_id
        {
            return Ok(Some((event.clone(), payload)));
        }
    }
    Ok(None)
}

fn mandate_targets_thread(
    payload: &crate::kernel::control_decision::MandateStartedPayload,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
) -> bool {
    let thread_id = coordinates.thread_id.to_string();
    payload
        .subject
        .thread_id
        .as_deref()
        .or(payload.thread_id.as_deref())
        .map(|subject| subject == thread_id)
        .unwrap_or(true)
}

fn revoked_targets_thread(
    payload: &crate::kernel::control_decision::MandateRevokedPayload,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
) -> bool {
    let thread_id = coordinates.thread_id.to_string();
    payload
        .subject
        .thread_id
        .as_deref()
        .map(|subject| subject == thread_id)
        .unwrap_or(true)
}

fn validate_schedule(
    schedule: &crate::kernel::control_decision::MandateSchedulePayload,
    catch_up: crate::kernel::control_decision::MandateCatchUpPolicy,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::VerletResult<()> {
    match schedule {
        crate::kernel::control_decision::MandateSchedulePayload::Cron { expr, tz } => {
            let cron = croner::Cron::from_str(expr).map_err(|err| {
                crate::VerletError::RuntimeExecution(format!("malformed cron expression: {err}"))
            })?;
            let timezone = tz.parse::<chrono_tz::Tz>().map_err(|err| {
                crate::VerletError::RuntimeExecution(format!("unknown IANA timezone {tz:?}: {err}"))
            })?;
            let first = cron_next(&cron, timezone, now)?;
            let second = cron.find_next_occurrence(&first, false).map_err(|err| {
                crate::VerletError::RuntimeExecution(format!(
                    "cron expression could not produce a second occurrence: {err}"
                ))
            })?;
            if second.signed_duration_since(first).num_milliseconds()
                < MIN_MANDATE_INTERVAL_MS as i64
            {
                return Err(crate::VerletError::RuntimeExecution(format!(
                    "cron schedules must have a minimum interval of {MIN_MANDATE_INTERVAL_MS}ms"
                )));
            }
        }
        crate::kernel::control_decision::MandateSchedulePayload::Interval { every_ms } => {
            if *every_ms < MIN_MANDATE_INTERVAL_MS {
                return Err(crate::VerletError::RuntimeExecution(format!(
                    "interval schedules must be at least {MIN_MANDATE_INTERVAL_MS}ms"
                )));
            }
        }
        crate::kernel::control_decision::MandateSchedulePayload::At { when } => {
            let when = chrono::DateTime::parse_from_rfc3339(when)
                .map_err(|err| {
                    crate::VerletError::RuntimeExecution(format!(
                        "at schedule requires RFC3339 when: {err}"
                    ))
                })?
                .with_timezone(&chrono::Utc);
            if when < now
                && catch_up != crate::kernel::control_decision::MandateCatchUpPolicy::CoalesceMissed
            {
                return Err(crate::VerletError::RuntimeExecution(
                    "at schedule is in the past; use catch_up = coalesce_missed to witness a missed occurrence"
                        .to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn cron_next(
    cron: &croner::Cron,
    timezone: chrono_tz::Tz,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::VerletResult<chrono::DateTime<chrono_tz::Tz>> {
    cron.find_next_occurrence(&now.with_timezone(&timezone), false)
        .map_err(|err| {
            crate::VerletError::RuntimeExecution(format!(
                "cron expression could not produce an occurrence: {err}"
            ))
        })
}

fn decode_started(
    event: &crate::kernel::history::EventRecord,
) -> crate::VerletResult<crate::kernel::control_decision::MandateStartedPayload> {
    serde_json::from_value(event.payload.clone()).map_err(|err| {
        crate::VerletError::History(format!("mandate.started payload is invalid: {err}"))
    })
}

fn decode_revoked(
    event: &crate::kernel::history::EventRecord,
) -> crate::VerletResult<crate::kernel::control_decision::MandateRevokedPayload> {
    serde_json::from_value(event.payload.clone()).map_err(|err| {
        crate::VerletError::History(format!("mandate.revoked payload is invalid: {err}"))
    })
}

fn json_error(err: serde_json::Error) -> crate::VerletError {
    crate::VerletError::RuntimeFactory(format!("mandate payload JSON codec failed: {err}"))
}

#[cfg(test)]
mod tests {
    use crate::kernel::history::EventStore as _;
    use chrono::TimeZone as _;

    fn now() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc.with_ymd_and_hms(2026, 7, 4, 12, 0, 0).unwrap()
    }

    #[test]
    fn mandate_validation_rejects_bad_schedule_inputs() {
        let malformed_cron = crate::kernel::mandate_lifecycle::MandateStartRequest {
            schedule: crate::kernel::control_decision::MandateSchedulePayload::Cron {
                expr: "not cron".to_string(),
                tz: "UTC".to_string(),
            },
            max_occurrences: None,
            catch_up: None,
            input_template: None,
            snapshot_id: None,
            expires_at: None,
        };
        assert!(
            crate::kernel::mandate_lifecycle::validate_mandate_start_request(
                &malformed_cron,
                now()
            )
            .is_err()
        );

        let unknown_tz = crate::kernel::mandate_lifecycle::MandateStartRequest {
            schedule: crate::kernel::control_decision::MandateSchedulePayload::Cron {
                expr: "0 * * * * *".to_string(),
                tz: "Mars/Olympus".to_string(),
            },
            max_occurrences: None,
            catch_up: None,
            input_template: None,
            snapshot_id: None,
            expires_at: None,
        };
        assert!(
            crate::kernel::mandate_lifecycle::validate_mandate_start_request(&unknown_tz, now())
                .is_err()
        );

        let short_interval = crate::kernel::mandate_lifecycle::MandateStartRequest {
            schedule: crate::kernel::control_decision::MandateSchedulePayload::Interval {
                every_ms: 59_999,
            },
            max_occurrences: None,
            catch_up: None,
            input_template: None,
            snapshot_id: None,
            expires_at: None,
        };
        assert!(
            crate::kernel::mandate_lifecycle::validate_mandate_start_request(
                &short_interval,
                now()
            )
            .is_err()
        );

        let sub_minute_cron = crate::kernel::mandate_lifecycle::MandateStartRequest {
            schedule: crate::kernel::control_decision::MandateSchedulePayload::Cron {
                expr: "* * * * * *".to_string(),
                tz: "UTC".to_string(),
            },
            max_occurrences: None,
            catch_up: None,
            input_template: None,
            snapshot_id: None,
            expires_at: None,
        };
        assert!(
            crate::kernel::mandate_lifecycle::validate_mandate_start_request(
                &sub_minute_cron,
                now()
            )
            .is_err()
        );
    }

    #[test]
    fn at_schedule_past_requires_coalesce_catch_up() {
        let past = crate::kernel::control_decision::MandateSchedulePayload::At {
            when: "2026-07-04T11:59:00Z".to_string(),
        };
        let skip = crate::kernel::mandate_lifecycle::MandateStartRequest {
            schedule: past.clone(),
            max_occurrences: None,
            catch_up: Some(crate::kernel::control_decision::MandateCatchUpPolicy::SkipMissed),
            input_template: None,
            snapshot_id: None,
            expires_at: None,
        };
        assert!(
            crate::kernel::mandate_lifecycle::validate_mandate_start_request(&skip, now()).is_err()
        );

        let coalesce = crate::kernel::mandate_lifecycle::MandateStartRequest {
            schedule: past,
            max_occurrences: None,
            catch_up: Some(crate::kernel::control_decision::MandateCatchUpPolicy::CoalesceMissed),
            input_template: None,
            snapshot_id: None,
            expires_at: None,
        };
        crate::kernel::mandate_lifecycle::validate_mandate_start_request(&coalesce, now()).unwrap();
    }

    #[tokio::test]
    async fn mandate_lifecycle_folds_started_minus_revoked() {
        let store = crate::kernel::history::InMemorySessionStore::default();
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let receipt = crate::kernel::mandate_lifecycle::start_mandate(
            &store,
            &coordinates,
            crate::kernel::mandate_lifecycle::MandateStartRequest {
                schedule: crate::kernel::control_decision::MandateSchedulePayload::Interval {
                    every_ms: 60_000,
                },
                max_occurrences: Some(2),
                catch_up: Some(crate::kernel::control_decision::MandateCatchUpPolicy::SkipMissed),
                input_template: Some("continue".to_string()),
                snapshot_id: Some("snapshot-a".to_string()),
                expires_at: Some("2026-07-04T12:05:00Z".to_string()),
            },
            now(),
        )
        .await
        .unwrap();
        assert_eq!(
            receipt.event.kind,
            crate::kernel::history::EventKind::MandateStarted
        );
        assert_eq!(receipt.payload.schedule.is_some(), true);
        assert_eq!(receipt.payload.expires_at_ms, Some(1_783_166_700_000));

        let active = crate::kernel::mandate_lifecycle::list_active_mandates(&store, &coordinates)
            .await
            .unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].event.id, receipt.event.id);

        let revoked = crate::kernel::mandate_lifecycle::revoke_mandate(
            &store,
            &coordinates,
            receipt.event.id,
        )
        .await
        .unwrap();
        assert_eq!(
            revoked.status,
            crate::kernel::mandate_lifecycle::MandateRevokeStatus::Revoked
        );
        assert_eq!(
            revoked.revoke_event.kind,
            crate::kernel::history::EventKind::MandateRevoked
        );

        let second = crate::kernel::mandate_lifecycle::revoke_mandate(
            &store,
            &coordinates,
            receipt.event.id,
        )
        .await
        .unwrap();
        assert_eq!(
            second.status,
            crate::kernel::mandate_lifecycle::MandateRevokeStatus::AlreadyRevoked
        );
        assert_eq!(
            crate::kernel::mandate_lifecycle::list_active_mandates(&store, &coordinates)
                .await
                .unwrap()
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn mandate_start_rejects_an_already_expired_expiry() {
        let store = crate::kernel::history::InMemorySessionStore::default();
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let err = crate::kernel::mandate_lifecycle::start_mandate(
            &store,
            &coordinates,
            crate::kernel::mandate_lifecycle::MandateStartRequest {
                schedule: crate::kernel::control_decision::MandateSchedulePayload::Interval {
                    every_ms: 60_000,
                },
                max_occurrences: None,
                catch_up: None,
                input_template: None,
                snapshot_id: None,
                expires_at: Some("2026-07-04T11:59:59Z".to_string()),
            },
            now(),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("mandate expiry"));
        assert!(err.to_string().contains("2026-07-04T11:59:59Z"));
        assert!(
            store
                .read_events(&crate::control_stream_id(&coordinates), None)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
