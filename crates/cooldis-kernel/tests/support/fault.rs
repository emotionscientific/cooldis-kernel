#![allow(dead_code)]

use super::kernel_test::{
    EventProvenance, EventRecord, EventSequence, EventStore, EventStreamId, HistoryError,
    HistoryResult, NewEventRecord, NewObservationRecord, ObservationRecord, ObservationStore,
    ProviderCapabilityRecord, ProviderClient, ProviderError, ProviderRequest, ProviderResponse,
    ProviderResult, ProviderStreamEvent, RuntimeStore, SessionContext, SessionEntry,
    SessionEntryId, SessionEntryKind, SessionStore, StreamCursorV1, ThreadBaseRef,
    ThreadCoordinates,
};
use async_trait::async_trait;
use cooldis_io_core::{
    IngressAck, IngressEnvelope, IngressQueueStore, IngressSink, IoError, IoResult,
    LeasedIngressEnvelope,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

enum FaultAction<F> {
    Fail(F),
    Delay(Duration),
}

struct FaultRule<F> {
    operation: &'static str,
    nth: usize,
    action: FaultAction<F>,
}

struct FaultScript<F> {
    calls: Mutex<BTreeMap<&'static str, usize>>,
    rules: Mutex<Vec<FaultRule<F>>>,
}

impl<F> Default for FaultScript<F> {
    fn default() -> Self {
        Self {
            calls: Mutex::new(BTreeMap::new()),
            rules: Mutex::new(Vec::new()),
        }
    }
}

impl<F> FaultScript<F> {
    fn fail_nth(&self, operation: &'static str, nth: usize, failure: F) {
        assert!(nth > 0, "fault operation index is one-based");
        self.rules.lock().unwrap().push(FaultRule {
            operation,
            nth,
            action: FaultAction::Fail(failure),
        });
    }

    fn delay_nth(&self, operation: &'static str, nth: usize, delay: Duration) {
        assert!(nth > 0, "fault operation index is one-based");
        self.rules.lock().unwrap().push(FaultRule {
            operation,
            nth,
            action: FaultAction::Delay(delay),
        });
    }

    fn next(&self, operation: &'static str) -> Option<FaultAction<F>> {
        let call = {
            let mut calls = self.calls.lock().unwrap();
            let call = calls.entry(operation).or_default();
            *call += 1;
            *call
        };
        let mut rules = self.rules.lock().unwrap();
        rules
            .iter()
            .position(|rule| rule.operation == operation && rule.nth == call)
            .map(|index| rules.remove(index).action)
    }

    fn call_count(&self, operation: &'static str) -> usize {
        self.calls
            .lock()
            .unwrap()
            .get(operation)
            .copied()
            .unwrap_or_default()
    }
}

pub struct FaultingRuntimeStore<S> {
    inner: Arc<S>,
    faults: FaultScript<String>,
}

impl<S> FaultingRuntimeStore<S> {
    pub fn new(inner: Arc<S>) -> Self {
        Self {
            inner,
            faults: FaultScript::default(),
        }
    }

    pub fn fail_nth(self, operation: &'static str, nth: usize, message: impl Into<String>) -> Self {
        self.faults.fail_nth(operation, nth, message.into());
        self
    }

    pub fn delay_nth(self, operation: &'static str, nth: usize, delay: Duration) -> Self {
        self.faults.delay_nth(operation, nth, delay);
        self
    }

    pub fn call_count(&self, operation: &'static str) -> usize {
        self.faults.call_count(operation)
    }

    pub fn inner(&self) -> &Arc<S> {
        &self.inner
    }

    async fn before(&self, operation: &'static str) -> HistoryResult<()> {
        match self.faults.next(operation) {
            Some(FaultAction::Fail(message)) => Err(HistoryError::Storage(message)),
            Some(FaultAction::Delay(delay)) => {
                tokio::time::sleep(delay).await;
                Ok(())
            }
            None => Ok(()),
        }
    }
}

#[async_trait]
impl<S: RuntimeStore + 'static> SessionStore for FaultingRuntimeStore<S> {
    async fn append(
        &self,
        coordinates: &ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
    ) -> HistoryResult<SessionEntry> {
        self.before("append").await?;
        self.inner.append(coordinates, parent_entry_id, kind).await
    }

    async fn append_with_provenance(
        &self,
        coordinates: &ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
        provenance: EventProvenance,
    ) -> HistoryResult<SessionEntry> {
        self.before("append_with_provenance").await?;
        self.inner
            .append_with_provenance(coordinates, parent_entry_id, kind, provenance)
            .await
    }

    async fn active_leaf(
        &self,
        coordinates: &ThreadCoordinates,
    ) -> HistoryResult<Option<SessionEntryId>> {
        self.before("active_leaf").await?;
        self.inner.active_leaf(coordinates).await
    }

    async fn select_branch(
        &self,
        coordinates: &ThreadCoordinates,
        leaf_entry_id: Option<SessionEntryId>,
    ) -> HistoryResult<()> {
        self.before("select_branch").await?;
        self.inner.select_branch(coordinates, leaf_entry_id).await
    }

    async fn build_context(
        &self,
        coordinates: &ThreadCoordinates,
    ) -> HistoryResult<SessionContext> {
        self.before("build_context").await?;
        self.inner.build_context(coordinates).await
    }

    async fn clone_branch(
        &self,
        source_coordinates: &ThreadCoordinates,
        source_leaf: Option<SessionEntryId>,
        target_coordinates: &ThreadCoordinates,
    ) -> HistoryResult<Option<SessionEntryId>> {
        self.before("clone_branch").await?;
        self.inner
            .clone_branch(source_coordinates, source_leaf, target_coordinates)
            .await
    }

    async fn fork_by_reference(
        &self,
        source_coordinates: &ThreadCoordinates,
        target_coordinates: &ThreadCoordinates,
        base: ThreadBaseRef,
    ) -> HistoryResult<()> {
        self.before("fork_by_reference").await?;
        self.inner
            .fork_by_reference(source_coordinates, target_coordinates, base)
            .await
    }
}

#[async_trait]
impl<S: RuntimeStore + 'static> EventStore for FaultingRuntimeStore<S> {
    async fn append_events(
        &self,
        stream_id: &EventStreamId,
        records: Vec<NewEventRecord>,
    ) -> HistoryResult<Vec<EventRecord>> {
        self.before("append_events").await?;
        self.inner.append_events(stream_id, records).await
    }

    async fn append_events_fenced(
        &self,
        stream_id: &EventStreamId,
        expected_next_sequence: EventSequence,
        records: Vec<NewEventRecord>,
    ) -> HistoryResult<Vec<EventRecord>> {
        self.before("append_events_fenced").await?;
        self.inner
            .append_events_fenced(stream_id, expected_next_sequence, records)
            .await
    }

    async fn read_events(
        &self,
        stream_id: &EventStreamId,
        from_sequence: Option<EventSequence>,
    ) -> HistoryResult<Vec<EventRecord>> {
        self.before("read_events").await?;
        self.inner.read_events(stream_id, from_sequence).await
    }

    async fn read_events_after_cursor(
        &self,
        stream_id: &EventStreamId,
        cursor: &StreamCursorV1,
    ) -> HistoryResult<Vec<EventRecord>> {
        self.before("read_events_after_cursor").await?;
        self.inner.read_events_after_cursor(stream_id, cursor).await
    }
}

#[async_trait]
impl<S: RuntimeStore + 'static> ObservationStore for FaultingRuntimeStore<S> {
    async fn append_observation(
        &self,
        record: NewObservationRecord,
    ) -> HistoryResult<ObservationRecord> {
        self.before("append_observation").await?;
        self.inner.append_observation(record).await
    }

    async fn list_observations(
        &self,
        scope: &ThreadCoordinates,
        kind: Option<&str>,
    ) -> HistoryResult<Vec<ObservationRecord>> {
        self.before("list_observations").await?;
        self.inner.list_observations(scope, kind).await
    }
}

#[derive(Clone)]
enum ProviderFailure {
    Http(String),
    Decode(String),
}

pub struct FaultingProviderClient<C> {
    inner: Arc<C>,
    faults: FaultScript<ProviderFailure>,
}

impl<C> FaultingProviderClient<C> {
    pub fn new(inner: Arc<C>) -> Self {
        Self {
            inner,
            faults: FaultScript::default(),
        }
    }

    pub fn fail_nth_http(
        self,
        operation: &'static str,
        nth: usize,
        message: impl Into<String>,
    ) -> Self {
        self.faults
            .fail_nth(operation, nth, ProviderFailure::Http(message.into()));
        self
    }

    pub fn fail_nth_decode(
        self,
        operation: &'static str,
        nth: usize,
        message: impl Into<String>,
    ) -> Self {
        self.faults
            .fail_nth(operation, nth, ProviderFailure::Decode(message.into()));
        self
    }

    pub fn delay_nth(self, operation: &'static str, nth: usize, delay: Duration) -> Self {
        self.faults.delay_nth(operation, nth, delay);
        self
    }

    pub fn call_count(&self, operation: &'static str) -> usize {
        self.faults.call_count(operation)
    }

    pub fn inner(&self) -> &Arc<C> {
        &self.inner
    }

    async fn before(&self, operation: &'static str) -> ProviderResult<()> {
        match self.faults.next(operation) {
            Some(FaultAction::Fail(ProviderFailure::Http(message))) => {
                Err(ProviderError::Http(message))
            }
            Some(FaultAction::Fail(ProviderFailure::Decode(message))) => {
                Err(ProviderError::Decode(message))
            }
            Some(FaultAction::Delay(delay)) => {
                tokio::time::sleep(delay).await;
                Ok(())
            }
            None => Ok(()),
        }
    }
}

#[async_trait]
impl<C: ProviderClient + 'static> ProviderClient for FaultingProviderClient<C> {
    fn capabilities(&self) -> Option<ProviderCapabilityRecord> {
        self.inner.capabilities()
    }

    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        self.before("complete").await?;
        self.inner.complete(request).await
    }

    async fn stream(&self, request: &ProviderRequest) -> ProviderResult<Vec<ProviderStreamEvent>> {
        self.before("stream").await?;
        self.inner.stream(request).await
    }
}

pub struct FaultingIngressQueue<Q> {
    inner: Arc<Q>,
    faults: FaultScript<String>,
}

impl<Q> FaultingIngressQueue<Q> {
    pub fn new(inner: Arc<Q>) -> Self {
        Self {
            inner,
            faults: FaultScript::default(),
        }
    }

    pub fn fail_nth(self, operation: &'static str, nth: usize, message: impl Into<String>) -> Self {
        self.faults.fail_nth(operation, nth, message.into());
        self
    }

    pub fn delay_nth(self, operation: &'static str, nth: usize, delay: Duration) -> Self {
        self.faults.delay_nth(operation, nth, delay);
        self
    }

    pub fn call_count(&self, operation: &'static str) -> usize {
        self.faults.call_count(operation)
    }

    pub fn inner(&self) -> &Arc<Q> {
        &self.inner
    }

    async fn before(&self, operation: &'static str) -> IoResult<()> {
        match self.faults.next(operation) {
            Some(FaultAction::Fail(message)) => Err(IoError::Queue(message)),
            Some(FaultAction::Delay(delay)) => {
                tokio::time::sleep(delay).await;
                Ok(())
            }
            None => Ok(()),
        }
    }
}

#[async_trait]
impl<Q: IngressQueueStore + 'static> IngressSink for FaultingIngressQueue<Q> {
    async fn submit(&self, envelope: IngressEnvelope) -> IoResult<IngressAck> {
        self.before("submit").await?;
        self.inner.submit(envelope).await
    }
}

#[async_trait]
impl<Q: IngressQueueStore + 'static> IngressQueueStore for FaultingIngressQueue<Q> {
    async fn lease_ingress(
        &self,
        worker_id: &str,
        max_messages: usize,
        visibility_timeout_secs: u32,
    ) -> IoResult<Vec<LeasedIngressEnvelope>> {
        self.before("lease_ingress").await?;
        self.inner
            .lease_ingress(worker_id, max_messages, visibility_timeout_secs)
            .await
    }

    async fn complete_ingress(&self, message_id: &str) -> IoResult<()> {
        self.before("complete_ingress").await?;
        self.inner.complete_ingress(message_id).await
    }

    async fn hold_ingress_until(&self, message_id: &str, visible_at_ms: u64) -> IoResult<()> {
        self.before("hold_ingress_until").await?;
        self.inner
            .hold_ingress_until(message_id, visible_at_ms)
            .await
    }

    async fn retry_ingress(&self, message_id: &str, reason: &str) -> IoResult<()> {
        self.before("retry_ingress").await?;
        self.inner.retry_ingress(message_id, reason).await
    }
}
