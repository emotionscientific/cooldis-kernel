use crate::{
    ActiveMandate, CooldisError, CooldisResult, EventKind, EventRecordId, EventStore,
    MandateCatchUpPolicy, MandateSchedulePayload, SqliteSessionStore, ThreadCoordinates,
    TimerFiredPayload, control_stream_id, list_active_mandates,
};
use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use cooldis_io_core::{
    ConversationKind, IngressContent, IngressEnvelope, IngressSink, IoConversation, IoDedupeKey,
    IoSource,
};
use croner::Cron;
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

pub const CLOCK_TICK_ROUTE_KIND: &str = "clock.tick";
pub const TIMER_FIRED_ENVELOPE_KIND: &str = "timer.fired";
const DEFAULT_CLOCK_POLL_INTERVAL: Duration = Duration::from_secs(30);

pub trait DaemonClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Clone, Debug)]
pub struct SystemDaemonClock;

impl DaemonClock for SystemDaemonClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

pub struct CooldisDaemonClockRoute {
    route_id: String,
    store: SqliteSessionStore,
    sink: Arc<dyn IngressSink>,
    clock: Arc<dyn DaemonClock>,
    started_at: DateTime<Utc>,
    poll_interval: Duration,
}

impl CooldisDaemonClockRoute {
    pub fn new(
        route_id: impl Into<String>,
        store: SqliteSessionStore,
        sink: Arc<dyn IngressSink>,
        clock: Arc<dyn DaemonClock>,
    ) -> Self {
        let started_at = clock.now();
        Self {
            route_id: route_id.into(),
            store,
            sink,
            clock,
            started_at,
            poll_interval: DEFAULT_CLOCK_POLL_INTERVAL,
        }
    }

    #[cfg(test)]
    pub fn with_started_at(mut self, started_at: DateTime<Utc>) -> Self {
        self.started_at = started_at;
        self
    }

    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    pub async fn enqueue_due_once(&self) -> CooldisResult<usize> {
        let now = self.clock.now();
        let mut schedule = self.build_due_schedule(now).await?;
        let mut enqueued = 0;
        while let Some(Reverse(tick)) = schedule.peek().cloned() {
            if tick.scheduled_for > now {
                break;
            }
            schedule.pop();
            let ack = self
                .sink
                .submit(tick.envelope(&self.route_id, now)?)
                .await
                .map_err(|err| CooldisError::RuntimeFactory(err.to_string()))?;
            if ack.accepted {
                enqueued += 1;
            }
        }
        Ok(enqueued)
    }

    pub async fn run(self) {
        loop {
            if let Err(err) = self.enqueue_due_once().await {
                eprintln!("cooldis clock route {} failed: {err}", self.route_id);
            }
            tokio::time::sleep(self.poll_interval).await;
        }
    }

    async fn build_due_schedule(
        &self,
        now: DateTime<Utc>,
    ) -> CooldisResult<BinaryHeap<Reverse<ScheduledTick>>> {
        let mut heap = BinaryHeap::new();
        for coordinates in self
            .store
            .list_control_stream_coordinates()
            .map_err(|err| CooldisError::History(err.to_string()))?
        {
            let active = list_active_mandates(&self.store, &coordinates).await?;
            let fired = fired_occurrence_indices(&self.store, &coordinates).await?;
            for mandate in active {
                if let Some(tick) =
                    next_tick_for_mandate(&coordinates, &mandate, &fired, self.started_at, now)?
                {
                    heap.push(Reverse(tick));
                }
            }
        }
        Ok(heap)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ScheduledTick {
    coordinates: ThreadCoordinates,
    mandate_event_id: EventRecordId,
    scheduled_for: DateTime<Utc>,
    occurrence_index: u64,
    catch_up: bool,
}

impl ScheduledTick {
    fn envelope(&self, route_id: &str, now: DateTime<Utc>) -> CooldisResult<IngressEnvelope> {
        let payload = TimerFiredPayload {
            mandate_event_id: self.mandate_event_id,
            scheduled_for: self
                .scheduled_for
                .to_rfc3339_opts(SecondsFormat::Millis, true),
            occurrence_index: self.occurrence_index,
            catch_up: self.catch_up,
        };
        let source = IoSource::new(CLOCK_TICK_ROUTE_KIND, route_id);
        let dedupe_key = IoDedupeKey::for_source(
            &source,
            format!("{}:{}", self.mandate_event_id, self.occurrence_index),
        );
        Ok(IngressEnvelope::new(
            source,
            IoConversation::new(
                format!("thread:{}", self.coordinates.thread_id),
                ConversationKind::System,
            ),
            IngressContent::Event {
                kind: TIMER_FIRED_ENVELOPE_KIND.to_string(),
                payload: serde_json::to_value(&payload).map_err(json_error)?,
            },
            now.timestamp_millis().max(0) as u64,
        )
        .with_dedupe_key(dedupe_key)
        .with_metadata("cooldis_route_id", route_id.to_string())
        .with_metadata("cooldis_tenant_id", self.coordinates.tenant_id.clone())
        .with_metadata("cooldis_user_id", self.coordinates.user_id.clone())
        .with_metadata("cooldis_session_id", self.coordinates.session_id.clone())
        .with_metadata("cooldis_thread_id", self.coordinates.thread_id.to_string())
        .with_metadata(
            "cooldis_mandate_event_id",
            self.mandate_event_id.to_string(),
        )
        .with_metadata(
            "cooldis_scheduled_for",
            self.scheduled_for
                .to_rfc3339_opts(SecondsFormat::Millis, true),
        )
        .with_metadata(
            "cooldis_occurrence_index",
            self.occurrence_index.to_string(),
        )
        .with_metadata("cooldis_catch_up", self.catch_up.to_string()))
    }
}

impl Ord for ScheduledTick {
    fn cmp(&self, other: &Self) -> Ordering {
        self.scheduled_for
            .cmp(&other.scheduled_for)
            .then_with(|| self.occurrence_index.cmp(&other.occurrence_index))
            .then_with(|| {
                self.mandate_event_id
                    .to_string()
                    .cmp(&other.mandate_event_id.to_string())
            })
    }
}

impl PartialOrd for ScheduledTick {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

async fn fired_occurrence_indices(
    store: &SqliteSessionStore,
    coordinates: &ThreadCoordinates,
) -> CooldisResult<HashSet<(EventRecordId, u64)>> {
    let events = store
        .read_events(&control_stream_id(coordinates), None)
        .await
        .map_err(|err| CooldisError::History(err.to_string()))?;
    let mut fired = HashSet::new();
    for event in events {
        if event.kind != EventKind::TimerFired {
            continue;
        }
        let payload =
            serde_json::from_value::<TimerFiredPayload>(event.payload).map_err(|err| {
                CooldisError::History(format!("timer.fired payload is invalid: {err}"))
            })?;
        fired.insert((payload.mandate_event_id, payload.occurrence_index));
    }
    Ok(fired)
}

fn next_tick_for_mandate(
    coordinates: &ThreadCoordinates,
    mandate: &ActiveMandate,
    fired: &HashSet<(EventRecordId, u64)>,
    route_started_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> CooldisResult<Option<ScheduledTick>> {
    let Some(schedule) = mandate.payload.schedule.as_ref() else {
        return Ok(None);
    };
    let catch_up_policy = mandate.payload.catch_up.unwrap_or_default();
    let mandate_event_id = mandate.event.id;
    let max_occurrences = mandate.payload.max_occurrences.map(u64::from);
    let start = datetime_from_millis(mandate.event.created_at_ms)?;
    let last_fired = fired
        .iter()
        .filter_map(|(event_id, index)| (*event_id == mandate_event_id).then_some(*index))
        .max();
    let mut occurrence_index = last_fired.map(|index| index + 1).unwrap_or(0);
    if max_occurrences.is_some_and(|max| occurrence_index >= max) {
        return Ok(None);
    }

    let mut scheduled_for = occurrence_at_index(schedule, start, occurrence_index)?;
    match catch_up_policy {
        MandateCatchUpPolicy::SkipMissed if scheduled_for < route_started_at => {
            let Some(index) = first_occurrence_index_at_or_after(
                schedule,
                start,
                route_started_at,
                max_occurrences,
            )?
            else {
                return Ok(None);
            };
            occurrence_index = index;
            scheduled_for = occurrence_at_index(schedule, start, occurrence_index)?;
        }
        MandateCatchUpPolicy::CoalesceMissed
            if scheduled_for < route_started_at && scheduled_for <= now =>
        {
            if let Some(index) =
                latest_occurrence_index_at_or_before(schedule, start, now, max_occurrences)?
                && index >= occurrence_index
            {
                occurrence_index = index;
                scheduled_for = occurrence_at_index(schedule, start, occurrence_index)?;
            }
        }
        _ => {}
    }

    if max_occurrences.is_some_and(|max| occurrence_index >= max) {
        return Ok(None);
    }
    if fired.contains(&(mandate_event_id, occurrence_index)) {
        return Ok(None);
    }
    Ok(Some(ScheduledTick {
        coordinates: coordinates.clone(),
        mandate_event_id,
        scheduled_for,
        occurrence_index,
        catch_up: catch_up_policy == MandateCatchUpPolicy::CoalesceMissed
            && scheduled_for < route_started_at,
    }))
}

fn occurrence_at_index(
    schedule: &MandateSchedulePayload,
    start: DateTime<Utc>,
    index: u64,
) -> CooldisResult<DateTime<Utc>> {
    match schedule {
        MandateSchedulePayload::Interval { every_ms } => {
            let multiplier = index.checked_add(1).ok_or_else(|| {
                CooldisError::RuntimeExecution("interval occurrence index overflowed".to_string())
            })?;
            let offset_ms = every_ms.checked_mul(multiplier).ok_or_else(|| {
                CooldisError::RuntimeExecution("interval occurrence offset overflowed".to_string())
            })?;
            let offset_ms = i64::try_from(offset_ms).map_err(|_| {
                CooldisError::RuntimeExecution("interval occurrence offset overflowed".to_string())
            })?;
            start
                .checked_add_signed(chrono::Duration::milliseconds(offset_ms))
                .ok_or_else(|| {
                    CooldisError::RuntimeExecution(
                        "interval occurrence timestamp overflowed".to_string(),
                    )
                })
        }
        MandateSchedulePayload::At { when } => {
            if index > 0 {
                return Err(CooldisError::RuntimeExecution(
                    "at schedule has only one occurrence".to_string(),
                ));
            }
            parse_utc(when)
        }
        MandateSchedulePayload::Cron { expr, tz } => {
            let (cron, timezone) = cron_schedule(expr, tz)?;
            cron.iter_after(start.with_timezone(&timezone))
                .nth(index as usize)
                .map(|dt| dt.with_timezone(&Utc))
                .ok_or_else(|| {
                    CooldisError::RuntimeExecution(
                        "cron schedule did not produce an occurrence".to_string(),
                    )
                })
        }
    }
}

fn first_occurrence_index_at_or_after(
    schedule: &MandateSchedulePayload,
    start: DateTime<Utc>,
    threshold: DateTime<Utc>,
    max_occurrences: Option<u64>,
) -> CooldisResult<Option<u64>> {
    match schedule {
        MandateSchedulePayload::Interval { every_ms } => {
            let every_ms_i64 = i64::try_from(*every_ms).map_err(|_| {
                CooldisError::RuntimeExecution("interval duration overflowed".to_string())
            })?;
            let Some(first) =
                start.checked_add_signed(chrono::Duration::milliseconds(every_ms_i64))
            else {
                return Err(CooldisError::RuntimeExecution(
                    "interval occurrence timestamp overflowed".to_string(),
                ));
            };
            if threshold <= first {
                return Ok(Some(0));
            }
            let diff_ms = threshold.signed_duration_since(first).num_milliseconds() as u64;
            let index = diff_ms.div_ceil(*every_ms);
            if max_occurrences.is_some_and(|max| index >= max) {
                Ok(None)
            } else {
                Ok(Some(index))
            }
        }
        MandateSchedulePayload::At { when } => {
            let when = parse_utc(when)?;
            Ok((when >= threshold)
                .then_some(0)
                .filter(|index| !max_occurrences.is_some_and(|max| *index >= max)))
        }
        MandateSchedulePayload::Cron { expr, tz } => {
            let (cron, timezone) = cron_schedule(expr, tz)?;
            for (index, occurrence) in cron.iter_after(start.with_timezone(&timezone)).enumerate() {
                let index = index as u64;
                if max_occurrences.is_some_and(|max| index >= max) {
                    return Ok(None);
                }
                if occurrence.with_timezone(&Utc) >= threshold {
                    return Ok(Some(index));
                }
            }
            Ok(None)
        }
    }
}

fn latest_occurrence_index_at_or_before(
    schedule: &MandateSchedulePayload,
    start: DateTime<Utc>,
    threshold: DateTime<Utc>,
    max_occurrences: Option<u64>,
) -> CooldisResult<Option<u64>> {
    match schedule {
        MandateSchedulePayload::Interval { every_ms } => {
            let every_ms_i64 = i64::try_from(*every_ms).map_err(|_| {
                CooldisError::RuntimeExecution("interval duration overflowed".to_string())
            })?;
            let Some(first) =
                start.checked_add_signed(chrono::Duration::milliseconds(every_ms_i64))
            else {
                return Err(CooldisError::RuntimeExecution(
                    "interval occurrence timestamp overflowed".to_string(),
                ));
            };
            if threshold < first {
                return Ok(None);
            }
            let diff_ms = threshold.signed_duration_since(first).num_milliseconds() as u64;
            let mut index = diff_ms / every_ms;
            if let Some(max) = max_occurrences {
                if max == 0 {
                    return Ok(None);
                }
                index = index.min(max - 1);
            }
            Ok(Some(index))
        }
        MandateSchedulePayload::At { when } => {
            let when = parse_utc(when)?;
            Ok((when <= threshold)
                .then_some(0)
                .filter(|index| !max_occurrences.is_some_and(|max| *index >= max)))
        }
        MandateSchedulePayload::Cron { expr, tz } => {
            let (cron, timezone) = cron_schedule(expr, tz)?;
            let mut latest = None;
            for (index, occurrence) in cron.iter_after(start.with_timezone(&timezone)).enumerate() {
                let index = index as u64;
                if max_occurrences.is_some_and(|max| index >= max) {
                    break;
                }
                if occurrence.with_timezone(&Utc) > threshold {
                    break;
                }
                latest = Some(index);
            }
            Ok(latest)
        }
    }
}

fn cron_schedule(expr: &str, tz: &str) -> CooldisResult<(Cron, chrono_tz::Tz)> {
    let cron = Cron::from_str(expr).map_err(|err| {
        CooldisError::RuntimeExecution(format!("malformed cron expression: {err}"))
    })?;
    let timezone = tz.parse::<chrono_tz::Tz>().map_err(|err| {
        CooldisError::RuntimeExecution(format!("unknown IANA timezone {tz:?}: {err}"))
    })?;
    Ok((cron, timezone))
}

fn datetime_from_millis(value: i64) -> CooldisResult<DateTime<Utc>> {
    Utc.timestamp_millis_opt(value).single().ok_or_else(|| {
        CooldisError::RuntimeExecution(format!("invalid mandate event timestamp {value}"))
    })
}

fn parse_utc(value: &str) -> CooldisResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|err| CooldisError::RuntimeExecution(format!("invalid RFC3339 instant: {err}")))
}

fn json_error(err: serde_json::Error) -> CooldisError {
    CooldisError::RuntimeExecution(format!("failed to encode clock tick payload: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EventRecord, EventSequence, EventStreamId, MandateStartedPayload};
    use cooldis_io_core::{IngressAck, IoResult};
    use std::sync::Mutex;

    #[derive(Clone)]
    struct FakeClock {
        now: Arc<Mutex<DateTime<Utc>>>,
    }

    impl FakeClock {
        fn new(now: DateTime<Utc>) -> Self {
            Self {
                now: Arc::new(Mutex::new(now)),
            }
        }
    }

    impl DaemonClock for FakeClock {
        fn now(&self) -> DateTime<Utc> {
            *self.now.lock().unwrap()
        }
    }

    struct CaptureSink {
        envelopes: Arc<Mutex<Vec<IngressEnvelope>>>,
    }

    #[async_trait::async_trait]
    impl IngressSink for CaptureSink {
        async fn submit(&self, envelope: IngressEnvelope) -> IoResult<IngressAck> {
            let ack = IngressAck::accepted(&envelope);
            self.envelopes.lock().unwrap().push(envelope);
            Ok(ack)
        }
    }

    fn dt(value: &str) -> DateTime<Utc> {
        parse_utc(value).unwrap()
    }

    fn coordinates() -> ThreadCoordinates {
        ThreadCoordinates::new("tenant", "user", "session")
    }

    fn active_mandate(
        schedule: MandateSchedulePayload,
        catch_up: MandateCatchUpPolicy,
        started_at: DateTime<Utc>,
        max_occurrences: Option<u32>,
    ) -> ActiveMandate {
        let coordinates = coordinates();
        let event = EventRecord {
            id: EventRecordId::new(),
            stream_id: EventStreamId::new(format!("control:{}", coordinates.thread_id)),
            sequence: EventSequence::new(1),
            coordinates: coordinates.clone(),
            created_at_ms: started_at.timestamp_millis(),
            kind: EventKind::MandateStarted,
            origin: crate::EventOrigin::Witnessed,
            provenance: Default::default(),
            payload: serde_json::json!({}),
        };
        ActiveMandate {
            event,
            payload: MandateStartedPayload {
                subject: crate::MandateSubject {
                    thread_id: Some(coordinates.thread_id.to_string()),
                    loop_id: None,
                },
                mandate_id: "mandate-test".to_string(),
                snapshot_id: "schedule.v1".to_string(),
                thread_id: Some(coordinates.thread_id.to_string()),
                max_continuations: None,
                expires_at_ms: None,
                schedule: Some(schedule),
                max_occurrences,
                catch_up: Some(catch_up),
                input_template: None,
            },
        }
    }

    #[test]
    fn coalesce_missed_interval_fires_once_at_latest_missed_index() {
        let mandate = active_mandate(
            MandateSchedulePayload::Interval { every_ms: 60_000 },
            MandateCatchUpPolicy::CoalesceMissed,
            dt("2026-01-01T00:00:00Z"),
            None,
        );
        let tick = next_tick_for_mandate(
            &coordinates(),
            &mandate,
            &HashSet::new(),
            dt("2026-01-01T00:02:30Z"),
            dt("2026-01-01T00:02:30Z"),
        )
        .unwrap()
        .unwrap();

        assert_eq!(tick.occurrence_index, 1);
        assert_eq!(tick.scheduled_for, dt("2026-01-01T00:02:00Z"));
        assert!(tick.catch_up);
    }

    #[test]
    fn skip_missed_interval_waits_until_next_on_time_occurrence() {
        let mandate = active_mandate(
            MandateSchedulePayload::Interval { every_ms: 60_000 },
            MandateCatchUpPolicy::SkipMissed,
            dt("2026-01-01T00:00:00Z"),
            None,
        );
        let tick = next_tick_for_mandate(
            &coordinates(),
            &mandate,
            &HashSet::new(),
            dt("2026-01-01T00:01:30Z"),
            dt("2026-01-01T00:01:30Z"),
        )
        .unwrap()
        .unwrap();

        assert_eq!(tick.occurrence_index, 1);
        assert_eq!(tick.scheduled_for, dt("2026-01-01T00:02:00Z"));
        assert!(!tick.catch_up);
    }

    #[test]
    fn at_schedule_fires_once_and_respects_max_occurrences() {
        let schedule = MandateSchedulePayload::At {
            when: "2026-01-01T00:01:00Z".to_string(),
        };
        assert_eq!(
            occurrence_at_index(&schedule, dt("2026-01-01T00:00:00Z"), 0).unwrap(),
            dt("2026-01-01T00:01:00Z")
        );
        assert!(
            first_occurrence_index_at_or_after(
                &schedule,
                dt("2026-01-01T00:00:00Z"),
                dt("2026-01-01T00:02:00Z"),
                None,
            )
            .unwrap()
            .is_none()
        );
        assert!(
            latest_occurrence_index_at_or_before(
                &schedule,
                dt("2026-01-01T00:00:00Z"),
                dt("2026-01-01T00:02:00Z"),
                Some(0),
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn cron_schedule_crosses_dst_boundary_with_stable_indices() {
        let schedule = MandateSchedulePayload::Cron {
            expr: "0 30 1 * * *".to_string(),
            tz: "America/New_York".to_string(),
        };
        let start = dt("2026-10-31T04:00:00Z");
        let first = occurrence_at_index(&schedule, start, 0).unwrap();
        let second = occurrence_at_index(&schedule, start, 1).unwrap();
        let third = occurrence_at_index(&schedule, start, 2).unwrap();

        assert_eq!(first, dt("2026-10-31T05:30:00Z"));
        assert_eq!(second, dt("2026-11-01T05:30:00Z"));
        assert_eq!(third, dt("2026-11-02T06:30:00Z"));
        assert_eq!(
            latest_occurrence_index_at_or_before(
                &schedule,
                start,
                dt("2026-11-01T06:45:00Z"),
                None,
            )
            .unwrap(),
            Some(1)
        );
    }

    #[tokio::test]
    async fn route_uses_injected_clock_when_building_tick_envelope() {
        let root = std::env::temp_dir()
            .join("cooldis-clock-route-tests")
            .join(uuid::Uuid::now_v7().to_string());
        let store = SqliteSessionStore::open(root.join("history.sqlite3")).unwrap();
        let coordinates = coordinates();
        let mandate = crate::NewEventRecord {
            id: EventRecordId::new(),
            coordinates: coordinates.clone(),
            created_at_ms: dt("2026-01-01T00:00:00Z").timestamp_millis(),
            kind: EventKind::MandateStarted,
            origin: crate::EventOrigin::Witnessed,
            provenance: Default::default(),
            payload: serde_json::to_value(MandateStartedPayload {
                subject: crate::MandateSubject {
                    thread_id: Some(coordinates.thread_id.to_string()),
                    loop_id: None,
                },
                mandate_id: "mandate-clock".to_string(),
                snapshot_id: "schedule.v1".to_string(),
                thread_id: Some(coordinates.thread_id.to_string()),
                max_continuations: None,
                expires_at_ms: None,
                schedule: Some(MandateSchedulePayload::At {
                    when: "2026-01-01T00:01:00Z".to_string(),
                }),
                max_occurrences: None,
                catch_up: Some(MandateCatchUpPolicy::CoalesceMissed),
                input_template: None,
            })
            .unwrap(),
        };
        store
            .append_events(&control_stream_id(&coordinates), vec![mandate])
            .await
            .unwrap();
        let envelopes = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(CaptureSink {
            envelopes: envelopes.clone(),
        });
        let clock = Arc::new(FakeClock::new(dt("2026-01-01T00:02:00Z")));
        let route = CooldisDaemonClockRoute::new("clock-main", store, sink, clock)
            .with_started_at(dt("2026-01-01T00:02:00Z"));

        assert_eq!(route.enqueue_due_once().await.unwrap(), 1);
        let captured = envelopes.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].source.protocol, CLOCK_TICK_ROUTE_KIND);
        assert_eq!(
            captured[0]
                .metadata
                .get("cooldis_catch_up")
                .map(String::as_str),
            Some("true")
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
