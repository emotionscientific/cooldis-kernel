#![allow(dead_code)]

enum FaultAction<F> {
    Fail(F),
    Delay(std::time::Duration),
    FailAfter(F),
    DelayAfter(std::time::Duration),
}

enum AfterAction<F> {
    Fail(F),
    Delay(std::time::Duration),
}

struct FaultRule<F> {
    operation: &'static str,
    nth: usize,
    action: FaultAction<F>,
}

struct FaultScript<F> {
    calls: std::sync::Mutex<std::collections::BTreeMap<&'static str, usize>>,
    rules: std::sync::Mutex<Vec<FaultRule<F>>>,
}

impl<F> Default for FaultScript<F> {
    fn default() -> Self {
        Self {
            calls: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            rules: std::sync::Mutex::new(Vec::new()),
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

    fn delay_nth(&self, operation: &'static str, nth: usize, delay: std::time::Duration) {
        assert!(nth > 0, "fault operation index is one-based");
        self.rules.lock().unwrap().push(FaultRule {
            operation,
            nth,
            action: FaultAction::Delay(delay),
        });
    }

    fn fail_nth_after(&self, operation: &'static str, nth: usize, failure: F) {
        assert!(nth > 0, "fault operation index is one-based");
        self.rules.lock().unwrap().push(FaultRule {
            operation,
            nth,
            action: FaultAction::FailAfter(failure),
        });
    }

    fn delay_nth_after(&self, operation: &'static str, nth: usize, delay: std::time::Duration) {
        assert!(nth > 0, "fault operation index is one-based");
        self.rules.lock().unwrap().push(FaultRule {
            operation,
            nth,
            action: FaultAction::DelayAfter(delay),
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
    inner: std::sync::Arc<S>,
    faults: FaultScript<String>,
}

impl<S> FaultingRuntimeStore<S> {
    pub fn new(inner: std::sync::Arc<S>) -> Self {
        Self {
            inner,
            faults: FaultScript::default(),
        }
    }

    pub fn fail_nth(self, operation: &'static str, nth: usize, message: impl Into<String>) -> Self {
        self.faults.fail_nth(operation, nth, message.into());
        self
    }

    pub fn panic_next(&self, operation: &'static str, message: impl Into<String>) {
        let nth = self.faults.call_count(operation).saturating_add(1);
        self.faults
            .fail_nth(operation, nth, format!("test panic: {}", message.into()));
    }

    pub fn delay_nth(
        self,
        operation: &'static str,
        nth: usize,
        delay: std::time::Duration,
    ) -> Self {
        self.faults.delay_nth(operation, nth, delay);
        self
    }

    pub fn fail_nth_after(
        self,
        operation: &'static str,
        nth: usize,
        message: impl Into<String>,
    ) -> Self {
        self.faults.fail_nth_after(operation, nth, message.into());
        self
    }

    pub fn delay_nth_after(
        self,
        operation: &'static str,
        nth: usize,
        delay: std::time::Duration,
    ) -> Self {
        self.faults.delay_nth_after(operation, nth, delay);
        self
    }

    pub fn call_count(&self, operation: &'static str) -> usize {
        self.faults.call_count(operation)
    }

    pub fn inner(&self) -> &std::sync::Arc<S> {
        &self.inner
    }

    async fn start(
        &self,
        operation: &'static str,
    ) -> verlet_history::HistoryResult<Option<AfterAction<String>>> {
        match self.faults.next(operation) {
            Some(FaultAction::Fail(message)) => match message.strip_prefix("test panic: ") {
                Some(message) => panic!("{message}"),
                None => Err(verlet_history::HistoryError::Storage(message)),
            },
            Some(FaultAction::Delay(delay)) => {
                tokio::time::sleep(delay).await;
                Ok(None)
            }
            Some(FaultAction::FailAfter(message)) => Ok(Some(AfterAction::Fail(message))),
            Some(FaultAction::DelayAfter(delay)) => Ok(Some(AfterAction::Delay(delay))),
            None => Ok(None),
        }
    }

    async fn finish<T>(
        result: verlet_history::HistoryResult<T>,
        after: Option<AfterAction<String>>,
    ) -> verlet_history::HistoryResult<T> {
        let value = result?;
        match after {
            Some(AfterAction::Fail(message)) => Err(verlet_history::HistoryError::Storage(message)),
            Some(AfterAction::Delay(delay)) => {
                tokio::time::sleep(delay).await;
                Ok(value)
            }
            None => Ok(value),
        }
    }
}

#[async_trait::async_trait]
impl<S: verlet_history::RuntimeStore + 'static> verlet_history::SessionStore
    for FaultingRuntimeStore<S>
{
    async fn append(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        parent_entry_id: Option<verlet_history::SessionEntryId>,
        kind: verlet_history::SessionEntryKind,
    ) -> verlet_history::HistoryResult<verlet_history::SessionEntry> {
        let after = self.start("append").await?;
        Self::finish(
            self.inner.append(coordinates, parent_entry_id, kind).await,
            after,
        )
        .await
    }

    async fn append_with_provenance(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        parent_entry_id: Option<verlet_history::SessionEntryId>,
        kind: verlet_history::SessionEntryKind,
        provenance: verlet_history::EventProvenance,
    ) -> verlet_history::HistoryResult<verlet_history::SessionEntry> {
        let after = self.start("append_with_provenance").await?;
        Self::finish(
            self.inner
                .append_with_provenance(coordinates, parent_entry_id, kind, provenance)
                .await,
            after,
        )
        .await
    }

    async fn append_turn_input(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        turn_id: &str,
        kind: verlet_history::SessionEntryKind,
    ) -> verlet_history::HistoryResult<verlet_history::SessionEntry> {
        let after = self.start("append_turn_input").await?;
        Self::finish(
            self.inner
                .append_turn_input(coordinates, turn_id, kind)
                .await,
            after,
        )
        .await
    }

    async fn active_leaf(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> verlet_history::HistoryResult<Option<verlet_history::SessionEntryId>> {
        let after = self.start("active_leaf").await?;
        Self::finish(self.inner.active_leaf(coordinates).await, after).await
    }

    async fn select_branch(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        leaf_entry_id: Option<verlet_history::SessionEntryId>,
    ) -> verlet_history::HistoryResult<()> {
        let after = self.start("select_branch").await?;
        Self::finish(
            self.inner.select_branch(coordinates, leaf_entry_id).await,
            after,
        )
        .await
    }

    async fn build_context(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> verlet_history::HistoryResult<verlet_history::SessionContext> {
        let after = self.start("build_context").await?;
        Self::finish(self.inner.build_context(coordinates).await, after).await
    }

    async fn clone_branch(
        &self,
        source_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        source_leaf: Option<verlet_history::SessionEntryId>,
        target_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> verlet_history::HistoryResult<Option<verlet_history::SessionEntryId>> {
        let after = self.start("clone_branch").await?;
        Self::finish(
            self.inner
                .clone_branch(source_coordinates, source_leaf, target_coordinates)
                .await,
            after,
        )
        .await
    }

    async fn fork_by_reference(
        &self,
        source_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        target_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        base: verlet_history::ThreadBaseRef,
    ) -> verlet_history::HistoryResult<()> {
        let after = self.start("fork_by_reference").await?;
        Self::finish(
            self.inner
                .fork_by_reference(source_coordinates, target_coordinates, base)
                .await,
            after,
        )
        .await
    }
}

#[async_trait::async_trait]
impl<S: verlet_history::RuntimeStore + 'static> verlet_history::EventStore
    for FaultingRuntimeStore<S>
{
    async fn append_events(
        &self,
        stream_id: &verlet_history::EventStreamId,
        records: Vec<verlet_history::NewEventRecord>,
    ) -> verlet_history::HistoryResult<Vec<verlet_history::EventRecord>> {
        let after = self.start("append_events").await?;
        Self::finish(self.inner.append_events(stream_id, records).await, after).await
    }

    async fn append_events_fenced(
        &self,
        stream_id: &verlet_history::EventStreamId,
        expected_next_sequence: verlet_history::EventSequence,
        records: Vec<verlet_history::NewEventRecord>,
    ) -> verlet_history::HistoryResult<Vec<verlet_history::EventRecord>> {
        let after = self.start("append_events_fenced").await?;
        Self::finish(
            self.inner
                .append_events_fenced(stream_id, expected_next_sequence, records)
                .await,
            after,
        )
        .await
    }

    async fn read_events(
        &self,
        stream_id: &verlet_history::EventStreamId,
        from_sequence: Option<verlet_history::EventSequence>,
    ) -> verlet_history::HistoryResult<Vec<verlet_history::EventRecord>> {
        let after = self.start("read_events").await?;
        Self::finish(
            self.inner.read_events(stream_id, from_sequence).await,
            after,
        )
        .await
    }

    async fn read_events_after_cursor(
        &self,
        stream_id: &verlet_history::EventStreamId,
        cursor: &verlet_history::StreamCursorV1,
    ) -> verlet_history::HistoryResult<Vec<verlet_history::EventRecord>> {
        let after = self.start("read_events_after_cursor").await?;
        Self::finish(
            self.inner.read_events_after_cursor(stream_id, cursor).await,
            after,
        )
        .await
    }
}

#[async_trait::async_trait]
impl<S: verlet_history::RuntimeStore + 'static> verlet_history::ObservationStore
    for FaultingRuntimeStore<S>
{
    async fn append_observation(
        &self,
        record: verlet_history::NewObservationRecord,
    ) -> verlet_history::HistoryResult<verlet_history::ObservationRecord> {
        let after = self.start("append_observation").await?;
        Self::finish(self.inner.append_observation(record).await, after).await
    }

    async fn list_observations(
        &self,
        scope: &verlet_runtime_contracts::ThreadCoordinates,
        kind: Option<&str>,
    ) -> verlet_history::HistoryResult<Vec<verlet_history::ObservationRecord>> {
        let after = self.start("list_observations").await?;
        Self::finish(self.inner.list_observations(scope, kind).await, after).await
    }
}

#[derive(Clone)]
enum ProviderFailure {
    Http(String),
    Decode(String),
}

pub struct FaultingProviderClient<C> {
    inner: std::sync::Arc<C>,
    faults: FaultScript<ProviderFailure>,
}

impl<C> FaultingProviderClient<C> {
    pub fn new(inner: std::sync::Arc<C>) -> Self {
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

    pub fn delay_nth(
        self,
        operation: &'static str,
        nth: usize,
        delay: std::time::Duration,
    ) -> Self {
        self.faults.delay_nth(operation, nth, delay);
        self
    }

    pub fn fail_nth_after_http(
        self,
        operation: &'static str,
        nth: usize,
        message: impl Into<String>,
    ) -> Self {
        self.faults
            .fail_nth_after(operation, nth, ProviderFailure::Http(message.into()));
        self
    }

    pub fn delay_nth_after(
        self,
        operation: &'static str,
        nth: usize,
        delay: std::time::Duration,
    ) -> Self {
        self.faults.delay_nth_after(operation, nth, delay);
        self
    }

    pub fn call_count(&self, operation: &'static str) -> usize {
        self.faults.call_count(operation)
    }

    pub fn inner(&self) -> &std::sync::Arc<C> {
        &self.inner
    }

    async fn start(
        &self,
        operation: &'static str,
    ) -> verlet_provider::ProviderResult<Option<AfterAction<ProviderFailure>>> {
        match self.faults.next(operation) {
            Some(FaultAction::Fail(ProviderFailure::Http(message))) => {
                Err(verlet_provider::ProviderError::Http(message))
            }
            Some(FaultAction::Fail(ProviderFailure::Decode(message))) => {
                Err(verlet_provider::ProviderError::Decode(message))
            }
            Some(FaultAction::Delay(delay)) => {
                tokio::time::sleep(delay).await;
                Ok(None)
            }
            Some(FaultAction::FailAfter(failure)) => Ok(Some(AfterAction::Fail(failure))),
            Some(FaultAction::DelayAfter(delay)) => Ok(Some(AfterAction::Delay(delay))),
            None => Ok(None),
        }
    }

    async fn finish<T>(
        result: verlet_provider::ProviderResult<T>,
        after: Option<AfterAction<ProviderFailure>>,
    ) -> verlet_provider::ProviderResult<T> {
        let value = result?;
        match after {
            Some(AfterAction::Fail(ProviderFailure::Http(message))) => {
                Err(verlet_provider::ProviderError::Http(message))
            }
            Some(AfterAction::Fail(ProviderFailure::Decode(message))) => {
                Err(verlet_provider::ProviderError::Decode(message))
            }
            Some(AfterAction::Delay(delay)) => {
                tokio::time::sleep(delay).await;
                Ok(value)
            }
            None => Ok(value),
        }
    }
}

#[async_trait::async_trait]
impl<C: verlet_provider::ProviderClient + 'static> verlet_provider::ProviderClient
    for FaultingProviderClient<C>
{
    fn capabilities(&self) -> Option<verlet_provider::ProviderCapabilityRecord> {
        self.inner.capabilities()
    }

    async fn complete(
        &self,
        request: &verlet_provider::ProviderRequest,
    ) -> verlet_provider::ProviderResult<verlet_provider::ProviderResponse> {
        let after = self.start("complete").await?;
        Self::finish(self.inner.complete(request).await, after).await
    }

    async fn stream(
        &self,
        request: &verlet_provider::ProviderRequest,
    ) -> verlet_provider::ProviderResult<Vec<verlet_provider::ProviderStreamEvent>> {
        let after = self.start("stream").await?;
        Self::finish(self.inner.stream(request).await, after).await
    }
}

pub struct FaultingIngressQueue<Q> {
    inner: std::sync::Arc<Q>,
    faults: FaultScript<String>,
}

impl<Q> FaultingIngressQueue<Q> {
    pub fn new(inner: std::sync::Arc<Q>) -> Self {
        Self {
            inner,
            faults: FaultScript::default(),
        }
    }

    pub fn fail_nth(self, operation: &'static str, nth: usize, message: impl Into<String>) -> Self {
        self.faults.fail_nth(operation, nth, message.into());
        self
    }

    pub fn delay_nth(
        self,
        operation: &'static str,
        nth: usize,
        delay: std::time::Duration,
    ) -> Self {
        self.faults.delay_nth(operation, nth, delay);
        self
    }

    pub fn fail_nth_after(
        self,
        operation: &'static str,
        nth: usize,
        message: impl Into<String>,
    ) -> Self {
        self.faults.fail_nth_after(operation, nth, message.into());
        self
    }

    pub fn delay_nth_after(
        self,
        operation: &'static str,
        nth: usize,
        delay: std::time::Duration,
    ) -> Self {
        self.faults.delay_nth_after(operation, nth, delay);
        self
    }

    pub fn call_count(&self, operation: &'static str) -> usize {
        self.faults.call_count(operation)
    }

    pub fn inner(&self) -> &std::sync::Arc<Q> {
        &self.inner
    }

    async fn start(
        &self,
        operation: &'static str,
    ) -> verlet_io_core::IoResult<Option<AfterAction<String>>> {
        match self.faults.next(operation) {
            Some(FaultAction::Fail(message)) => Err(verlet_io_core::IoError::Queue(message)),
            Some(FaultAction::Delay(delay)) => {
                tokio::time::sleep(delay).await;
                Ok(None)
            }
            Some(FaultAction::FailAfter(message)) => Ok(Some(AfterAction::Fail(message))),
            Some(FaultAction::DelayAfter(delay)) => Ok(Some(AfterAction::Delay(delay))),
            None => Ok(None),
        }
    }

    async fn finish<T>(
        result: verlet_io_core::IoResult<T>,
        after: Option<AfterAction<String>>,
    ) -> verlet_io_core::IoResult<T> {
        let value = result?;
        match after {
            Some(AfterAction::Fail(message)) => Err(verlet_io_core::IoError::Queue(message)),
            Some(AfterAction::Delay(delay)) => {
                tokio::time::sleep(delay).await;
                Ok(value)
            }
            None => Ok(value),
        }
    }
}

#[async_trait::async_trait]
impl<Q: verlet_io_core::IngressQueueStore + 'static> verlet_io_core::IngressSink
    for FaultingIngressQueue<Q>
{
    async fn submit(
        &self,
        envelope: verlet_io_core::IngressEnvelope,
    ) -> verlet_io_core::IoResult<verlet_io_core::IngressAck> {
        let after = self.start("submit").await?;
        Self::finish(self.inner.submit(envelope).await, after).await
    }
}

#[async_trait::async_trait]
impl<Q: verlet_io_core::IngressQueueStore + 'static> verlet_io_core::IngressQueueStore
    for FaultingIngressQueue<Q>
{
    async fn lease_ingress(
        &self,
        worker_id: &str,
        max_messages: usize,
        visibility_timeout_secs: u32,
    ) -> verlet_io_core::IoResult<Vec<verlet_io_core::LeasedIngressEnvelope>> {
        let after = self.start("lease_ingress").await?;
        Self::finish(
            self.inner
                .lease_ingress(worker_id, max_messages, visibility_timeout_secs)
                .await,
            after,
        )
        .await
    }

    async fn complete_ingress(&self, message_id: &str) -> verlet_io_core::IoResult<()> {
        let after = self.start("complete_ingress").await?;
        Self::finish(self.inner.complete_ingress(message_id).await, after).await
    }

    async fn hold_ingress_until(
        &self,
        message_id: &str,
        visible_at_ms: u64,
    ) -> verlet_io_core::IoResult<()> {
        let after = self.start("hold_ingress_until").await?;
        Self::finish(
            self.inner
                .hold_ingress_until(message_id, visible_at_ms)
                .await,
            after,
        )
        .await
    }

    async fn retry_ingress(&self, message_id: &str, reason: &str) -> verlet_io_core::IoResult<()> {
        let after = self.start("retry_ingress").await?;
        Self::finish(self.inner.retry_ingress(message_id, reason).await, after).await
    }
}

/// The three existing wrappers configured from one derived plan. Process
/// directives remain available to the crash-cut harness instead of creating a
/// fourth fault mechanism.
pub struct AppliedFaultPlan<S, Q, C> {
    pub store: FaultingRuntimeStore<S>,
    pub queue: FaultingIngressQueue<Q>,
    pub provider: FaultingProviderClient<C>,
    pub process_cuts: Vec<crate::support::fault_plan::FaultDirective>,
}

impl crate::support::fault_plan::FaultPlan {
    pub fn apply<S, Q, C>(
        &self,
        store: std::sync::Arc<S>,
        queue: std::sync::Arc<Q>,
        provider: std::sync::Arc<C>,
    ) -> AppliedFaultPlan<S, Q, C> {
        let mut store = FaultingRuntimeStore::new(store);
        let mut queue = FaultingIngressQueue::new(queue);
        let mut provider = FaultingProviderClient::new(provider);
        let mut process_cuts = Vec::new();
        for directive in &self.directives {
            let message = format!(
                "fault plan seed={} {} occurrence {}",
                self.seed, directive.operation, directive.nth
            );
            match (directive.component, directive.timing, &directive.action) {
                (
                    crate::support::fault_plan::FaultComponent::Store,
                    crate::support::fault_plan::FaultTiming::Before,
                    crate::support::fault_plan::PlannedAction::Fail,
                ) => {
                    store = store.fail_nth(directive.operation, directive.nth, message);
                }
                (
                    crate::support::fault_plan::FaultComponent::Store,
                    crate::support::fault_plan::FaultTiming::After,
                    crate::support::fault_plan::PlannedAction::Fail,
                ) => {
                    store = store.fail_nth_after(directive.operation, directive.nth, message);
                }
                (
                    crate::support::fault_plan::FaultComponent::Store,
                    crate::support::fault_plan::FaultTiming::Before,
                    crate::support::fault_plan::PlannedAction::Delay(delay),
                ) => {
                    store = store.delay_nth(directive.operation, directive.nth, *delay);
                }
                (
                    crate::support::fault_plan::FaultComponent::Store,
                    crate::support::fault_plan::FaultTiming::After,
                    crate::support::fault_plan::PlannedAction::Delay(delay),
                ) => {
                    store = store.delay_nth_after(directive.operation, directive.nth, *delay);
                }
                (
                    crate::support::fault_plan::FaultComponent::Queue,
                    crate::support::fault_plan::FaultTiming::Before,
                    crate::support::fault_plan::PlannedAction::Fail,
                ) => {
                    queue = queue.fail_nth(directive.operation, directive.nth, message);
                }
                (
                    crate::support::fault_plan::FaultComponent::Queue,
                    crate::support::fault_plan::FaultTiming::After,
                    crate::support::fault_plan::PlannedAction::Fail,
                ) => {
                    queue = queue.fail_nth_after(directive.operation, directive.nth, message);
                }
                (
                    crate::support::fault_plan::FaultComponent::Queue,
                    crate::support::fault_plan::FaultTiming::Before,
                    crate::support::fault_plan::PlannedAction::Delay(delay),
                ) => {
                    queue = queue.delay_nth(directive.operation, directive.nth, *delay);
                }
                (
                    crate::support::fault_plan::FaultComponent::Queue,
                    crate::support::fault_plan::FaultTiming::After,
                    crate::support::fault_plan::PlannedAction::Delay(delay),
                ) => {
                    queue = queue.delay_nth_after(directive.operation, directive.nth, *delay);
                }
                (
                    crate::support::fault_plan::FaultComponent::Provider,
                    crate::support::fault_plan::FaultTiming::Before,
                    crate::support::fault_plan::PlannedAction::Fail,
                ) => {
                    provider = provider.fail_nth_http(directive.operation, directive.nth, message);
                }
                (
                    crate::support::fault_plan::FaultComponent::Provider,
                    crate::support::fault_plan::FaultTiming::After,
                    crate::support::fault_plan::PlannedAction::Fail,
                ) => {
                    provider =
                        provider.fail_nth_after_http(directive.operation, directive.nth, message);
                }
                (
                    crate::support::fault_plan::FaultComponent::Provider,
                    crate::support::fault_plan::FaultTiming::Before,
                    crate::support::fault_plan::PlannedAction::Delay(delay),
                ) => {
                    provider = provider.delay_nth(directive.operation, directive.nth, *delay);
                }
                (
                    crate::support::fault_plan::FaultComponent::Provider,
                    crate::support::fault_plan::FaultTiming::After,
                    crate::support::fault_plan::PlannedAction::Delay(delay),
                ) => {
                    provider = provider.delay_nth_after(directive.operation, directive.nth, *delay);
                }
                (crate::support::fault_plan::FaultComponent::Process, _, _) => {
                    process_cuts.push(directive.clone())
                }
            }
        }
        AppliedFaultPlan {
            store,
            queue,
            provider,
            process_cuts,
        }
    }
}

#[cfg(test)]
mod tests {
    use verlet_history::SessionStore as _;
    use verlet_io_core::IngressQueueStore as _;
    use verlet_provider::ProviderClient as _;

    #[tokio::test]
    async fn store_append_after_fault_keeps_durable_effect_and_reports_storage_error() {
        let inner = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
        let store = crate::support::fault::FaultingRuntimeStore::new(inner.clone()).fail_nth_after(
            "append",
            1,
            "append committed before disconnect",
        );
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");

        let error = store
            .append(
                &coordinates,
                None,
                verlet_history::SessionEntryKind::Message {
                    message: verlet_history::CanonicalMessage::user_text("durable input"),
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, verlet_history::HistoryError::Storage(message) if message.contains("disconnect"))
        );
        let context = inner.build_context(&coordinates).await.unwrap();
        assert_eq!(context.entries.len(), 1);
    }

    #[derive(Default)]
    struct DurableAckQueue {
        completed: std::sync::atomic::AtomicBool,
        complete_calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl verlet_io_core::IngressSink for DurableAckQueue {
        async fn submit(
            &self,
            envelope: verlet_io_core::IngressEnvelope,
        ) -> verlet_io_core::IoResult<verlet_io_core::IngressAck> {
            Ok(verlet_io_core::IngressAck::accepted(&envelope))
        }
    }

    #[async_trait::async_trait]
    impl verlet_io_core::IngressQueueStore for DurableAckQueue {
        async fn lease_ingress(
            &self,
            _worker_id: &str,
            _max_messages: usize,
            _visibility_timeout_secs: u32,
        ) -> verlet_io_core::IoResult<Vec<verlet_io_core::LeasedIngressEnvelope>> {
            Ok(Vec::new())
        }

        async fn complete_ingress(&self, _message_id: &str) -> verlet_io_core::IoResult<()> {
            self.complete_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.completed
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }

        async fn hold_ingress_until(
            &self,
            _message_id: &str,
            _visible_at_ms: u64,
        ) -> verlet_io_core::IoResult<()> {
            Ok(())
        }

        async fn retry_ingress(
            &self,
            _message_id: &str,
            _reason: &str,
        ) -> verlet_io_core::IoResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn complete_ingress_after_fault_keeps_durable_ack_and_reports_queue_error() {
        let inner = std::sync::Arc::new(DurableAckQueue::default());
        let queue = crate::support::fault::FaultingIngressQueue::new(inner.clone()).fail_nth_after(
            "complete_ingress",
            1,
            "ack committed before disconnect",
        );

        let error = queue.complete_ingress("message-1").await.unwrap_err();

        assert!(
            matches!(error, verlet_io_core::IoError::Queue(message) if message.contains("disconnect"))
        );
        assert!(inner.completed.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            inner
                .complete_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    #[tokio::test]
    async fn complete_ingress_after_effect_redelivery_does_not_duplicate_turn() {
        let inner = std::sync::Arc::new(DurableAckQueue::default());
        let queue = crate::support::fault::FaultingIngressQueue::new(inner.clone()).fail_nth_after(
            "complete_ingress",
            1,
            "ack committed before disconnect",
        );
        let mut applied_ingress = std::collections::BTreeSet::new();
        let mut durable_turns = 0;

        for delivery in ["ingress-1", "ingress-1"] {
            if applied_ingress.insert(delivery) {
                durable_turns += 1;
            }
            let result = queue.complete_ingress("message-1").await;
            if inner
                .complete_calls
                .load(std::sync::atomic::Ordering::SeqCst)
                == 1
            {
                assert!(matches!(result, Err(verlet_io_core::IoError::Queue(_))));
            } else {
                result.unwrap();
            }
        }

        assert_eq!(durable_turns, 1, "redelivery must adopt the durable turn");
        assert!(inner.completed.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            inner
                .complete_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            2
        );
    }

    struct NeverCalledProvider;

    #[async_trait::async_trait]
    impl verlet_provider::ProviderClient for NeverCalledProvider {
        fn capabilities(&self) -> Option<verlet_provider::ProviderCapabilityRecord> {
            None
        }

        async fn complete(
            &self,
            _request: &verlet_provider::ProviderRequest,
        ) -> verlet_provider::ProviderResult<verlet_provider::ProviderResponse> {
            panic!("planned provider failure should fire before the inner call")
        }

        async fn stream(
            &self,
            _request: &verlet_provider::ProviderRequest,
        ) -> verlet_provider::ProviderResult<Vec<verlet_provider::ProviderStreamEvent>> {
            panic!("planned provider failure should fire before the inner call")
        }
    }

    #[tokio::test]
    async fn applying_plan_maps_failures_to_wrapper_error_types() {
        let plan = crate::support::fault_plan::FaultPlan {
            seed: 399,
            vocabulary_version: crate::support::fault_plan::FAULT_VOCABULARY_VERSION,
            intensity: crate::support::fault_plan::Intensity::Sparse,
            directives: vec![
                crate::support::fault_plan::FaultDirective {
                    component: crate::support::fault_plan::FaultComponent::Store,
                    operation: "append",
                    nth: 1,
                    timing: crate::support::fault_plan::FaultTiming::Before,
                    action: crate::support::fault_plan::PlannedAction::Fail,
                },
                crate::support::fault_plan::FaultDirective {
                    component: crate::support::fault_plan::FaultComponent::Queue,
                    operation: "complete_ingress",
                    nth: 1,
                    timing: crate::support::fault_plan::FaultTiming::Before,
                    action: crate::support::fault_plan::PlannedAction::Fail,
                },
                crate::support::fault_plan::FaultDirective {
                    component: crate::support::fault_plan::FaultComponent::Provider,
                    operation: "complete",
                    nth: 1,
                    timing: crate::support::fault_plan::FaultTiming::Before,
                    action: crate::support::fault_plan::PlannedAction::Fail,
                },
                crate::support::fault_plan::FaultDirective {
                    component: crate::support::fault_plan::FaultComponent::Process,
                    operation: "queue-apply",
                    nth: 1,
                    timing: crate::support::fault_plan::FaultTiming::Before,
                    action: crate::support::fault_plan::PlannedAction::Fail,
                },
            ],
        };
        let applied = plan.apply(
            std::sync::Arc::new(verlet_history::InMemorySessionStore::new()),
            std::sync::Arc::new(DurableAckQueue::default()),
            std::sync::Arc::new(NeverCalledProvider),
        );
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");

        assert!(matches!(
            applied
                .store
                .append(
                    &coordinates,
                    None,
                    verlet_history::SessionEntryKind::Message {
                        message: verlet_history::CanonicalMessage::user_text("blocked")
                    }
                )
                .await,
            Err(verlet_history::HistoryError::Storage(_))
        ));
        assert!(matches!(
            applied.queue.complete_ingress("message-1").await,
            Err(verlet_io_core::IoError::Queue(_))
        ));
        assert!(matches!(
            applied
                .provider
                .complete(&verlet_provider::ProviderRequest::new(
                    verlet_history::ProviderApi::Other("test".to_string()),
                    "test",
                    "model"
                ))
                .await,
            Err(verlet_provider::ProviderError::Http(_))
        ));
        assert_eq!(applied.process_cuts, vec![plan.directives[3].clone()]);
    }
}
