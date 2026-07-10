use super::connection::{ConnectionState, now_ms, turn_error};
use super::threads::*;
use super::*;
use crate::{EventKind, EventRecord, ToolCallCompletedPayload};

#[derive(Default)]
pub(super) struct AppServerSubscriptions {
    pub(super) next_subscriber_id: u64,
    pub(super) next_watcher_id: u64,
    pub(super) subscribers: HashMap<String, BTreeMap<u64, AppServerSubscriber>>,
    pub(super) watchers: HashMap<String, AppServerThreadWatcher>,
}

#[derive(Clone)]
pub(super) struct AppServerSubscriber {
    pub(super) outbound: mpsc::UnboundedSender<JsonRpcMessage>,
    pub(super) opt_out_notifications: Arc<RwLock<HashSet<String>>>,
}

pub(super) struct AppServerThreadWatcher {
    pub(super) id: u64,
    pub(super) handle: JoinHandle<()>,
}

enum ResyncedTurnItem {
    AgentMessage,
    AgentThinking,
    DynamicTool(Value),
}

struct ResyncedTurnProjection {
    assistant_text: String,
    thinking_text: String,
    items: Vec<ResyncedTurnItem>,
}

#[cfg(test)]
#[derive(Clone)]
pub(super) struct ThreadResyncTestGate {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
impl ThreadResyncTestGate {
    pub(super) async fn wait_until_entered(&self) {
        self.entered.notified().await;
    }

    pub(super) fn release(&self) {
        self.release.notify_one();
    }
}

#[cfg(test)]
pub(super) fn install_thread_resync_test_gate(thread_id: &str) -> ThreadResyncTestGate {
    let gate = ThreadResyncTestGate {
        entered: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    thread_resync_test_gates()
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .insert(thread_id.to_string(), gate.clone());
    gate
}

#[cfg(test)]
fn thread_resync_test_gates() -> &'static std::sync::Mutex<HashMap<String, ThreadResyncTestGate>> {
    static GATES: std::sync::OnceLock<std::sync::Mutex<HashMap<String, ThreadResyncTestGate>>> =
        std::sync::OnceLock::new();
    GATES.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

#[cfg(test)]
async fn pause_thread_resync_for_test(thread_id: &str) {
    let gate = thread_resync_test_gates()
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .get(thread_id)
        .cloned();
    let Some(gate) = gate else {
        return;
    };
    gate.entered.notify_one();
    gate.release.notified().await;
    thread_resync_test_gates()
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .remove(thread_id);
}

impl CooldisAppServer {
    pub(super) async fn subscribe_thread_connection(
        &self,
        handle: RuntimeThreadHandle,
        subscriber: AppServerSubscriber,
    ) -> u64 {
        let thread_id = handle.context().coordinates.thread_id.to_string();
        let mut subscriptions = self.inner.subscriptions.lock().await;
        let subscriber_id = subscriptions.next_subscriber_id;
        subscriptions.next_subscriber_id = subscriptions.next_subscriber_id.saturating_add(1);
        subscriptions
            .subscribers
            .entry(thread_id.clone())
            .or_default()
            .insert(subscriber_id, subscriber);

        let should_spawn = subscriptions
            .watchers
            .get(&thread_id)
            .is_none_or(|watcher| watcher.handle.is_finished());
        if should_spawn {
            subscriptions.watchers.remove(&thread_id);
            let watcher_id = subscriptions.next_watcher_id;
            subscriptions.next_watcher_id = subscriptions.next_watcher_id.saturating_add(1);
            let app = self.clone();
            let watcher_thread_id = thread_id.clone();
            let watcher = tokio::spawn(async move {
                watch_thread(app.clone(), handle).await;
                app.finish_thread_watcher(&watcher_thread_id, watcher_id)
                    .await;
            });
            subscriptions.watchers.insert(
                thread_id,
                AppServerThreadWatcher {
                    id: watcher_id,
                    handle: watcher,
                },
            );
        }
        subscriber_id
    }

    pub(super) async fn unsubscribe_thread_connection(&self, thread_id: &str, subscriber_id: u64) {
        let thread_has_active_turn = self.thread_has_active_turn(thread_id).await;
        let watcher = {
            let mut subscriptions = self.inner.subscriptions.lock().await;
            let remove_thread = subscriptions
                .subscribers
                .get_mut(thread_id)
                .map(|subscribers| {
                    subscribers.remove(&subscriber_id);
                    subscribers.is_empty()
                })
                .unwrap_or(false);
            if remove_thread {
                subscriptions.subscribers.remove(thread_id);
                if thread_has_active_turn {
                    None
                } else {
                    subscriptions.watchers.remove(thread_id)
                }
            } else {
                None
            }
        };
        if let Some(watcher) = watcher {
            watcher.handle.abort();
        }
    }

    pub(super) async fn finish_thread_watcher(&self, thread_id: &str, watcher_id: u64) {
        let mut subscriptions = self.inner.subscriptions.lock().await;
        if subscriptions
            .watchers
            .get(thread_id)
            .is_some_and(|watcher| watcher.id == watcher_id)
        {
            subscriptions.watchers.remove(thread_id);
        }
    }

    pub(super) async fn thread_has_active_turn(&self, thread_id: &str) -> bool {
        let state = self.inner.state.read().await;
        state
            .threads
            .get(thread_id)
            .is_some_and(|thread| thread.active_turn_id.is_some())
    }

    pub(super) async fn abort_thread_watcher_if_idle_and_unsubscribed(&self, thread_id: &str) {
        if self.thread_has_active_turn(thread_id).await {
            return;
        }
        let watcher = {
            let mut subscriptions = self.inner.subscriptions.lock().await;
            let has_subscribers = subscriptions
                .subscribers
                .get(thread_id)
                .is_some_and(|subscribers| !subscribers.is_empty());
            if has_subscribers {
                None
            } else {
                subscriptions.watchers.remove(thread_id)
            }
        };
        if let Some(watcher) = watcher {
            watcher.handle.abort();
        }
    }

    pub(super) async fn notify_thread_subscribers(
        &self,
        thread_id: &str,
        method: &str,
        params: Value,
    ) {
        let subscribers = {
            let subscriptions = self.inner.subscriptions.lock().await;
            subscriptions
                .subscribers
                .get(thread_id)
                .map(|subscribers| subscribers.values().cloned().collect::<Vec<_>>())
                .unwrap_or_default()
        };
        for subscriber in subscribers {
            subscriber.notify(method, params.clone()).await;
        }
    }
}

impl AppServerSubscriber {
    pub(super) async fn notify(&self, method: &str, params: Value) {
        if self.opt_out_notifications.read().await.contains(method) {
            return;
        }
        let _ = self
            .outbound
            .send(JsonRpcMessage::Notification(JsonRpcNotification {
                method: method.to_string(),
                params: Some(params),
            }));
    }
}

pub(super) async fn watch_thread(app: CooldisAppServer, handle: RuntimeThreadHandle) {
    let thread_id = handle.context().coordinates.thread_id.to_string();
    let mut events = handle.subscribe_events();
    let mut status = handle.subscribe_status();
    let mut lagged_events = 0_u64;
    let mut lagged_turn = None;
    {
        let _ = status.borrow_and_update();
    }
    loop {
        tokio::select! {
            biased;
            event = events.recv() => {
                match event {
                    Ok(event) => {
                        handle_thread_event(&app, &thread_id, event).await;
                        let mut status_value = handle.status();
                        if lagged_events > 0
                            && events.len() == 0
                            && matches!(
                                status_value,
                                ThreadStatus::Idle | ThreadStatus::Stopped | ThreadStatus::Failed
                            )
                        {
                            if !resynchronize_thread_after_lag(
                                &app,
                                &handle,
                                &thread_id,
                                lagged_events,
                                lagged_turn.as_deref(),
                            )
                            .await
                            {
                                status_value = handle.status();
                                handle_thread_status(&app, &thread_id, status_value).await;
                                break;
                            }
                            lagged_events = 0;
                            lagged_turn = None;
                            status_value = handle.status();
                        }
                        if matches!(status_value, ThreadStatus::Stopped | ThreadStatus::Failed)
                            && !app.thread_has_active_turn(&thread_id).await
                        {
                            if lagged_events > 0
                                && !resynchronize_thread_after_lag(
                                    &app,
                                    &handle,
                                    &thread_id,
                                    lagged_events,
                                    lagged_turn.as_deref(),
                                )
                                .await
                            {
                                handle_thread_status(&app, &thread_id, status_value).await;
                                break;
                            }
                            handle_thread_status(&app, &thread_id, status_value).await;
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        let should_announce = lagged_events == 0;
                        lagged_events = lagged_events.saturating_add(skipped);
                        if lagged_turn.is_none() {
                            lagged_turn = {
                                let state = app.inner.state.read().await;
                                state
                                    .threads
                                    .get(&thread_id)
                                    .and_then(|thread| thread.active_turn_id.clone())
                            };
                        }
                        if should_announce {
                            app.notify_thread_subscribers(
                                &thread_id,
                                "thread/resync/started",
                                json!({
                                    "threadId": thread_id,
                                    "reason": "broadcastLag",
                                    "laggedEvents": skipped,
                                }),
                            )
                            .await;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        if lagged_events > 0 {
                            let _ = resynchronize_thread_after_lag(
                                &app,
                                &handle,
                                &thread_id,
                                lagged_events,
                                lagged_turn.as_deref(),
                            )
                            .await;
                        }
                        break;
                    }
                }
            }
            changed = status.changed() => {
                if changed.is_err() {
                    if lagged_events > 0 {
                        let _ = resynchronize_thread_after_lag(
                            &app,
                            &handle,
                            &thread_id,
                            lagged_events,
                            lagged_turn.as_deref(),
                        )
                        .await;
                    }
                    break;
                }
                let mut status_value = *status.borrow();
                if lagged_events > 0
                    && matches!(
                        status_value,
                        ThreadStatus::Idle | ThreadStatus::Stopped | ThreadStatus::Failed
                    )
                {
                    if !resynchronize_thread_after_lag(
                        &app,
                        &handle,
                        &thread_id,
                        lagged_events,
                        lagged_turn.as_deref(),
                    )
                    .await
                    {
                        status_value = handle.status();
                        handle_thread_status(&app, &thread_id, status_value).await;
                        break;
                    }
                    lagged_events = 0;
                    lagged_turn = None;
                    status_value = handle.status();
                }
                handle_thread_status(&app, &thread_id, status_value).await;
                if status_value == ThreadStatus::Failed {
                    handle_failed_thread_status(&app, &thread_id, &mut events).await;
                    break;
                }
                if status_value == ThreadStatus::Stopped {
                    break;
                }
            }
        }
    }
}

async fn resynchronize_thread_after_lag(
    app: &CooldisAppServer,
    handle: &RuntimeThreadHandle,
    thread_id: &str,
    lagged_events: u64,
    lagged_turn: Option<&str>,
) -> bool {
    #[cfg(test)]
    pause_thread_resync_for_test(thread_id).await;

    let projection = if let Some(turn_id) = lagged_turn {
        let context = match handle.session_context().await {
            Ok(context) => context,
            Err(err) => {
                notify_thread_resync_failed(app, thread_id, lagged_events, err.to_string()).await;
                return false;
            }
        };
        let events = match handle.read_thread_events(None).await {
            Ok(events) => events,
            Err(err) => {
                notify_thread_resync_failed(app, thread_id, lagged_events, err.to_string()).await;
                return false;
            }
        };
        let (entry_id, completions) = match resynced_turn_facts(&events, turn_id) {
            Ok(facts) => facts,
            Err(message) => {
                notify_thread_resync_failed(app, thread_id, lagged_events, message).await;
                return false;
            }
        };
        let Some(projection) = resynced_turn_projection(&context.entries, &entry_id, &completions)
        else {
            notify_thread_resync_failed(
                app,
                thread_id,
                lagged_events,
                format!("durable session did not contain active turn {turn_id}"),
            )
            .await;
            return false;
        };
        Some(projection)
    } else {
        None
    };

    let thread = {
        let mut state = app.inner.state.write().await;
        state.threads.get_mut(thread_id).map(|thread| {
            thread.status = handle.status();
            thread.updated_at_ms = now_ms();
            if let (Some(turn_id), Some(projection)) = (lagged_turn, projection)
                && let Some(turn) = thread.turns.get_mut(turn_id)
            {
                apply_resynced_turn_projection(turn, projection);
            }
            thread_json(thread, true)
        })
    };
    let Some(thread) = thread else {
        notify_thread_resync_failed(
            app,
            thread_id,
            lagged_events,
            "thread projection disappeared during resynchronization".to_string(),
        )
        .await;
        return false;
    };

    app.notify_thread_subscribers(
        thread_id,
        "thread/resynced",
        json!({
            "threadId": thread_id,
            "reason": "broadcastLag",
            "laggedEvents": lagged_events,
            "thread": thread,
        }),
    )
    .await;
    true
}

async fn notify_thread_resync_failed(
    app: &CooldisAppServer,
    thread_id: &str,
    lagged_events: u64,
    message: String,
) {
    app.notify_thread_subscribers(
        thread_id,
        "thread/resync/failed",
        json!({
            "threadId": thread_id,
            "reason": "broadcastLag",
            "laggedEvents": lagged_events,
            "error": {
                "code": "resync_failed",
                "message": message,
            },
        }),
    )
    .await;
}

fn resynced_turn_projection(
    entries: &[SessionEntry],
    entry_id: &str,
    completions: &HashMap<String, ToolCallCompletedPayload>,
) -> Option<ResyncedTurnProjection> {
    let user_index = entries.iter().position(|entry| {
        entry.entry_id.to_string() == entry_id
            && matches!(
                &entry.kind,
                SessionEntryKind::Message {
                    message: CanonicalMessage::User { .. }
                }
            )
    })?;
    let mut projection = ResyncedTurnProjection {
        assistant_text: String::new(),
        thinking_text: String::new(),
        items: Vec::new(),
    };
    let mut saw_agent_message = false;
    let mut saw_agent_thinking = false;

    for entry in &entries[user_index + 1..] {
        let SessionEntryKind::Message { message } = &entry.kind else {
            continue;
        };
        match message {
            CanonicalMessage::User { .. } => break,
            CanonicalMessage::Assistant { content, .. } => {
                for content in content {
                    match content {
                        CanonicalContent::Text { text, .. } => {
                            if !text.is_empty() && !saw_agent_message {
                                projection.items.push(ResyncedTurnItem::AgentMessage);
                                saw_agent_message = true;
                            }
                            projection.assistant_text.push_str(text);
                        }
                        CanonicalContent::Thinking { text, .. } => {
                            if !text.is_empty() && !saw_agent_thinking {
                                projection.items.push(ResyncedTurnItem::AgentThinking);
                                saw_agent_thinking = true;
                            }
                            projection.thinking_text.push_str(text);
                        }
                        CanonicalContent::ToolCall {
                            id,
                            name,
                            arguments,
                        } => {
                            projection.items.push(ResyncedTurnItem::DynamicTool(json!({
                                "type": "dynamicToolCall",
                                "id": id,
                                "namespace": null,
                                "tool": name,
                                "arguments": arguments,
                                "status": "inProgress",
                                "contentItems": null,
                                "success": null,
                                "durationMs": null,
                            })));
                        }
                        CanonicalContent::Image { .. } => {}
                    }
                }
            }
            CanonicalMessage::ToolResult {
                tool_call_id,
                content,
                is_error,
                ..
            } => {
                if let Some(ResyncedTurnItem::DynamicTool(item)) =
                    projection.items.iter_mut().find(|item| {
                        matches!(
                            item,
                            ResyncedTurnItem::DynamicTool(value)
                                if value.get("id").and_then(Value::as_str)
                                    == Some(tool_call_id.as_str())
                        )
                    })
                {
                    item["status"] =
                        Value::String(if *is_error { "failed" } else { "completed" }.to_string());
                    item["success"] = Value::Bool(!is_error);
                    item["contentItems"] = json!([{
                        "type": "inputText",
                        "text": text_from_canonical_content(content),
                    }]);
                    if let Some(completion) = completions.get(tool_call_id) {
                        item["status"] = Value::String(
                            if completion.success {
                                "completed"
                            } else {
                                "failed"
                            }
                            .to_string(),
                        );
                        item["success"] = Value::Bool(completion.success);
                        item["durationMs"] = completion
                            .duration_ms
                            .map(Value::from)
                            .unwrap_or(Value::Null);
                    }
                }
            }
        }
    }
    Some(projection)
}

fn resynced_turn_facts(
    events: &[EventRecord],
    turn_id: &str,
) -> Result<(String, HashMap<String, ToolCallCompletedPayload>), String> {
    let mut submitted = None;
    let mut completions = HashMap::new();
    for event in events {
        match event.kind {
            EventKind::TurnSubmitted
                if event.payload.get("turn_id").and_then(Value::as_str) == Some(turn_id) =>
            {
                let entry_id = event
                    .payload
                    .get("entry_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "turn.submitted payload is missing entry_id".to_string())?;
                if submitted
                    .as_ref()
                    .is_none_or(|(sequence, _)| event.sequence.get() > *sequence)
                {
                    submitted = Some((event.sequence.get(), entry_id.to_string()));
                }
            }
            EventKind::ToolCallCompleted => {
                let payload =
                    serde_json::from_value::<ToolCallCompletedPayload>(event.payload.clone())
                        .map_err(|err| format!("tool.call.completed payload is invalid: {err}"))?;
                if payload.subject.turn_id == turn_id {
                    completions.insert(payload.subject.call_id.clone(), payload);
                }
            }
            _ => {}
        }
    }
    let entry_id = submitted
        .map(|(_, entry_id)| entry_id)
        .ok_or_else(|| format!("durable event stream did not contain turn {turn_id}"))?;
    Ok((entry_id, completions))
}

fn apply_resynced_turn_projection(
    turn: &mut AppServerTurnState,
    projection: ResyncedTurnProjection,
) {
    turn.items.retain(|item| {
        !matches!(
            item.get("type").and_then(Value::as_str),
            Some("agentMessage" | "agentThinking" | "dynamicToolCall")
        )
    });
    for item in projection.items {
        turn.items.push(match item {
            ResyncedTurnItem::AgentMessage => {
                agent_message_item_from_text(&turn.assistant_item_id, &projection.assistant_text)
            }
            ResyncedTurnItem::AgentThinking => {
                agent_thinking_item_from_text(&turn.thinking_item_id, &projection.thinking_text)
            }
            ResyncedTurnItem::DynamicTool(item) => item,
        });
    }
    turn.assistant_text = projection.assistant_text;
    turn.assistant_started = !turn.assistant_text.is_empty();
    turn.assistant_completed = false;
    turn.thinking_text = projection.thinking_text;
    turn.thinking_started = !turn.thinking_text.is_empty();
    turn.thinking_completed = false;
}

pub(super) async fn wait_for_initial_thread_status(handle: &RuntimeThreadHandle) {
    if handle.status() != ThreadStatus::Starting {
        return;
    }
    let mut status = handle.subscribe_status();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if *status.borrow() != ThreadStatus::Starting {
                return;
            }
            if status.changed().await.is_err() {
                return;
            }
        }
    })
    .await;
}

pub(super) async fn handle_thread_status(
    app: &CooldisAppServer,
    thread_id: &str,
    status: ThreadStatus,
) {
    let completion_to_schedule = {
        let mut state = app.inner.state.write().await;
        let Some(thread) = state.threads.get_mut(thread_id) else {
            return;
        };
        thread.status = status;
        thread.updated_at_ms = now_ms();
        if matches!(status, ThreadStatus::Running | ThreadStatus::Cancelling)
            && let Some(turn_id) = thread.active_turn_id.clone()
            && let Some(turn) = thread.turns.get_mut(&turn_id)
        {
            turn.observed_running = true;
        }
        if status == ThreadStatus::Idle {
            thread.active_turn_id.as_ref().and_then(|turn_id| {
                let turn = thread.turns.get_mut(turn_id)?;
                if turn.completion_scheduled {
                    return None;
                }
                turn.completion_scheduled = true;
                Some(turn_id.clone())
            })
        } else {
            None
        }
    };

    app.notify_thread_subscribers(
        thread_id,
        "thread/status/changed",
        json!({
            "threadId": thread_id,
            "status": thread_status_json(status),
        }),
    )
    .await;

    if let Some(turn_id) = completion_to_schedule {
        let app = app.clone();
        let thread_id = thread_id.to_string();
        tokio::spawn(async move {
            complete_turn_after_settle(app, thread_id, turn_id).await;
        });
    }
    if matches!(status, ThreadStatus::Stopped | ThreadStatus::Failed) {
        app.notify_thread_subscribers(thread_id, "thread/closed", json!({ "threadId": thread_id }))
            .await;
    }
}

/// Lets queued runtime failure events provide their code and message before a
/// failed thread status falls back to a generic failed-turn projection.
pub(super) async fn handle_failed_thread_status(
    app: &CooldisAppServer,
    thread_id: &str,
    events: &mut tokio::sync::broadcast::Receiver<ThreadEvent>,
) {
    if !app.thread_has_active_turn(thread_id).await {
        return;
    }
    if let Ok(Ok(event)) =
        tokio::time::timeout(std::time::Duration::from_millis(20), events.recv()).await
    {
        handle_thread_event(app, thread_id, event).await;
    }
    while let Ok(event) = events.try_recv() {
        handle_thread_event(app, thread_id, event).await;
    }
    fail_active_turn(
        app,
        thread_id,
        "runtime_failed",
        "thread failed before runtime failure details were received".to_string(),
    )
    .await;
}

pub(super) async fn complete_turn_after_settle(
    app: CooldisAppServer,
    thread_id: String,
    turn_id: String,
) {
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let current_turn_agent_content =
        wait_for_current_turn_assistant_content(&app, &thread_id, &turn_id).await;
    let completed_turn = {
        let mut state = app.inner.state.write().await;
        let Some(thread) = state.threads.get_mut(&thread_id) else {
            return;
        };
        if thread.active_turn_id.as_deref() != Some(turn_id.as_str()) {
            return;
        }
        thread.active_turn_id.take();
        thread.turns.get_mut(&turn_id).map(|turn| {
            if turn.status == AppServerTurnStatus::InProgress {
                turn.status = AppServerTurnStatus::Completed;
            }
            turn.completed_at_ms = Some(now_ms());
            let synthesized_message = current_turn_agent_content
                .as_ref()
                .map(|content| content.text.as_str())
                .and_then(|text| reconcile_turn_assistant_text(turn, text));
            let synthesized_thinking = current_turn_agent_content
                .as_ref()
                .map(|content| content.thinking.as_str())
                .and_then(|text| reconcile_turn_thinking_text(turn, text));
            let (turn, completed_items) = finalize_turn_payload(turn);
            (
                turn,
                synthesized_message,
                synthesized_thinking,
                completed_items,
            )
        })
    };

    let Some((turn, synthesized_message, synthesized_thinking, completed_items)) = completed_turn
    else {
        return;
    };
    if let Some((item, delta, item_id)) = synthesized_message {
        if let Some(item) = item {
            app.notify_thread_subscribers(
                &thread_id,
                "item/started",
                json!({
                    "item": item,
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "startedAtMs": now_ms(),
                }),
            )
            .await;
        }
        app.notify_thread_subscribers(
            &thread_id,
            "item/agentMessage/delta",
            json!({
                "threadId": thread_id,
                "turnId": turn_id,
                "itemId": item_id,
                "delta": delta,
            }),
        )
        .await;
    }
    if let Some((item, delta, item_id)) = synthesized_thinking {
        if let Some(item) = item {
            app.notify_thread_subscribers(
                &thread_id,
                "item/started",
                json!({
                    "item": item,
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "startedAtMs": now_ms(),
                }),
            )
            .await;
        }
        app.notify_thread_subscribers(
            &thread_id,
            "item/agentThinking/delta",
            json!({
                "threadId": thread_id,
                "turnId": turn_id,
                "itemId": item_id,
                "delta": delta,
            }),
        )
        .await;
    }
    for item in completed_items {
        app.notify_thread_subscribers(
            &thread_id,
            "item/completed",
            json!({
                "item": item,
                "threadId": thread_id,
                "turnId": turn_id,
                "completedAtMs": now_ms(),
            }),
        )
        .await;
    }
    app.notify_thread_subscribers(
        &thread_id,
        "turn/completed",
        json!({ "threadId": thread_id, "turn": turn }),
    )
    .await;
    app.abort_thread_watcher_if_idle_and_unsubscribed(&thread_id)
        .await;
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct AssistantContentProjection {
    pub(super) text: String,
    pub(super) thinking: String,
}

impl AssistantContentProjection {
    fn is_empty(&self) -> bool {
        self.text.is_empty() && self.thinking.is_empty()
    }
}

pub(super) async fn wait_for_current_turn_assistant_content(
    app: &CooldisAppServer,
    thread_id: &str,
    turn_id: &str,
) -> Option<AssistantContentProjection> {
    for _ in 0..25 {
        if !turn_is_active(app, thread_id, turn_id).await {
            return None;
        }
        if let Some(content) = current_turn_assistant_content(app, thread_id, turn_id).await {
            return Some(content);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    None
}

pub(super) async fn turn_is_active(app: &CooldisAppServer, thread_id: &str, turn_id: &str) -> bool {
    let state = app.inner.state.read().await;
    let Some(thread) = state.threads.get(thread_id) else {
        return false;
    };
    thread.active_turn_id.as_deref() == Some(turn_id) && thread.turns.contains_key(turn_id)
}

/// Reconciles completion payloads from saved session text after fast runtime
/// turns, because status notifications can outrun queued text notifications.
pub(super) fn reconcile_turn_assistant_text(
    turn: &mut AppServerTurnState,
    text: &str,
) -> Option<(Option<Value>, String, String)> {
    if text.is_empty() {
        return None;
    }

    let item_id = turn.assistant_item_id.clone();
    if !turn.assistant_started {
        let started_item = agent_message_item_from_text(&item_id, "");
        turn.assistant_started = true;
        turn.assistant_completed = false;
        turn.assistant_text = text.to_string();
        upsert_agent_message_item(turn);
        return Some((Some(started_item), text.to_string(), item_id));
    }

    let prior_text = turn.assistant_text.clone();
    let projected_item_matches = turn.items.iter().any(|item| {
        item.get("id").and_then(Value::as_str) == Some(item_id.as_str())
            && item.get("text").and_then(Value::as_str) == Some(text)
    });
    if prior_text == text {
        if !projected_item_matches {
            upsert_agent_message_item(turn);
            if turn.assistant_completed {
                turn.assistant_completed = false;
            }
        }
        return None;
    }

    let delta = text.strip_prefix(&prior_text)?;
    if delta.is_empty() {
        return None;
    }
    turn.assistant_text = text.to_string();
    if !projected_item_matches {
        upsert_agent_message_item(turn);
    }
    if turn.assistant_completed {
        turn.assistant_completed = false;
    }
    Some((None, delta.to_string(), item_id))
}

pub(super) fn reconcile_turn_thinking_text(
    turn: &mut AppServerTurnState,
    text: &str,
) -> Option<(Option<Value>, String, String)> {
    if text.is_empty() {
        return None;
    }

    let item_id = turn.thinking_item_id.clone();
    if !turn.thinking_started {
        let started_item = agent_thinking_item_from_text(&item_id, "");
        turn.thinking_started = true;
        turn.thinking_completed = false;
        turn.thinking_text = text.to_string();
        upsert_agent_thinking_item(turn);
        return Some((Some(started_item), text.to_string(), item_id));
    }

    let prior_text = turn.thinking_text.clone();
    let projected_item_matches = turn.items.iter().any(|item| {
        item.get("id").and_then(Value::as_str) == Some(item_id.as_str())
            && item.get("text").and_then(Value::as_str) == Some(text)
    });
    if prior_text == text {
        if !projected_item_matches {
            upsert_agent_thinking_item(turn);
            if turn.thinking_completed {
                turn.thinking_completed = false;
            }
        }
        return None;
    }

    let delta = text.strip_prefix(&prior_text)?;
    if delta.is_empty() {
        return None;
    }
    turn.thinking_text = text.to_string();
    if !projected_item_matches {
        upsert_agent_thinking_item(turn);
    }
    if turn.thinking_completed {
        turn.thinking_completed = false;
    }
    Some((None, delta.to_string(), item_id))
}

pub(super) fn upsert_agent_message_item(turn: &mut AppServerTurnState) {
    let item_id = turn.assistant_item_id.clone();
    let item = agent_message_item(turn);
    if let Some(existing) = turn
        .items
        .iter_mut()
        .find(|item| item.get("id").and_then(Value::as_str) == Some(item_id.as_str()))
    {
        *existing = item;
    } else {
        turn.items.push(item);
    }
}

pub(super) fn upsert_agent_thinking_item(turn: &mut AppServerTurnState) {
    let item_id = turn.thinking_item_id.clone();
    let item = agent_thinking_item(turn);
    if let Some(existing) = turn
        .items
        .iter_mut()
        .find(|item| item.get("id").and_then(Value::as_str) == Some(item_id.as_str()))
    {
        *existing = item;
    } else {
        turn.items.push(item);
    }
}

pub(super) async fn current_turn_assistant_content(
    app: &CooldisAppServer,
    thread_id: &str,
    turn_id: &str,
) -> Option<AssistantContentProjection> {
    let user_text = {
        let state = app.inner.state.read().await;
        let thread = state.threads.get(thread_id)?;
        if thread.active_turn_id.as_deref() != Some(turn_id) {
            return None;
        }
        turn_user_text(thread.turns.get(turn_id)?)?
    };
    let parsed = ThreadId::parse_str(thread_id).ok()?;
    let handle = app
        .inner
        .supervisor
        .get_thread(&app.inner.tenant_id, parsed)
        .await
        .ok()?;
    let context = handle.session_context().await.ok()?;
    assistant_content_after_latest_user(&context.messages, &user_text)
}

pub(super) fn turn_user_text(turn: &AppServerTurnState) -> Option<String> {
    turn.items
        .iter()
        .find(|item| item.get("type").and_then(Value::as_str) == Some("userMessage"))
        .and_then(|item| item.get("content").and_then(Value::as_array))
        .map(|content| user_input_preview(content))
        .filter(|text| !text.is_empty())
}

pub(super) fn assistant_content_after_latest_user(
    messages: &[CanonicalMessage],
    user_text: &str,
) -> Option<AssistantContentProjection> {
    let mut after_latest_user = false;
    let mut assistant_content = None;
    for message in messages {
        match message {
            CanonicalMessage::User { content, .. } => {
                after_latest_user = text_from_canonical_content(content) == user_text;
                if after_latest_user {
                    assistant_content = None;
                }
            }
            CanonicalMessage::Assistant { content, .. } if after_latest_user => {
                let content = AssistantContentProjection {
                    text: text_from_canonical_content(content),
                    thinking: thinking_text_from_canonical_content(content),
                };
                if !content.is_empty() {
                    assistant_content = Some(content);
                }
            }
            CanonicalMessage::Assistant { .. } | CanonicalMessage::ToolResult { .. } => {}
        }
    }
    assistant_content
}

#[cfg(test)]
pub(super) async fn latest_assistant_text(
    app: &CooldisAppServer,
    thread_id: &str,
) -> Option<String> {
    let parsed = ThreadId::parse_str(thread_id).ok()?;
    let handle = app
        .inner
        .supervisor
        .get_thread(&app.inner.tenant_id, parsed)
        .await
        .ok()?;
    let context = handle.session_context().await.ok()?;
    context
        .messages
        .iter()
        .rev()
        .find_map(|message| match message {
            CanonicalMessage::Assistant { content, .. } => {
                let text = text_from_canonical_content(content);
                (!text.is_empty()).then_some(text)
            }
            CanonicalMessage::User { .. } | CanonicalMessage::ToolResult { .. } => None,
        })
}

pub(super) async fn complete_shell_command(
    connection: ConnectionState,
    thread_id: String,
    turn_id: String,
    item_id: String,
    command: String,
    cwd: PathBuf,
) {
    let started_at = now_ms();
    let (exit_code, stdout, stderr) = run_shell_command(cwd.clone(), command.clone()).await;
    let output = format!("{stdout}{stderr}");
    let duration_ms = now_ms().saturating_sub(started_at);
    let completed_item = command_execution_item(
        &item_id,
        &command,
        &cwd,
        "completed",
        Some(output.clone()),
        Some(exit_code),
        Some(duration_ms),
    );
    let completed_turn = {
        let mut state = connection.app.inner.state.write().await;
        let Some(thread) = state.threads.get_mut(&thread_id) else {
            return;
        };
        thread.updated_at_ms = now_ms();
        if thread.active_turn_id.as_deref() == Some(turn_id.as_str()) {
            thread.active_turn_id.take();
        }
        let Some(turn) = thread.turns.get_mut(&turn_id) else {
            return;
        };
        for item in &mut turn.items {
            if item.get("id").and_then(Value::as_str) == Some(item_id.as_str()) {
                *item = completed_item.clone();
                break;
            }
        }
        turn.status = AppServerTurnStatus::Completed;
        turn.completed_at_ms = Some(now_ms());
        turn_json(turn)
    };

    if !output.is_empty() {
        connection
            .notify(
                "item/commandExecution/outputDelta",
                json!({
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "itemId": item_id,
                    "delta": output,
                }),
            )
            .await;
    }
    connection
        .notify(
            "item/completed",
            json!({
                "item": completed_item,
                "threadId": thread_id,
                "turnId": turn_id,
                "completedAtMs": now_ms(),
            }),
        )
        .await;
    connection
        .notify(
            "turn/completed",
            json!({ "threadId": thread_id, "turn": completed_turn }),
        )
        .await;
}

pub(super) async fn run_shell_command(cwd: PathBuf, command: String) -> (i32, String, String) {
    let mut process = Command::new("/bin/sh");
    process.arg("-c").arg(command).current_dir(cwd);
    process.kill_on_drop(true);
    match process.output().await {
        Ok(output) => (
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stdout).into_owned(),
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ),
        Err(err) => (
            -1,
            String::new(),
            format!("failed to run shell command: {err}"),
        ),
    }
}

pub(super) async fn handle_thread_event(
    app: &CooldisAppServer,
    thread_id: &str,
    event: ThreadEvent,
) {
    match event {
        ThreadEvent::Runtime { event, .. } => {
            handle_runtime_event(app, thread_id, event.kind).await;
        }
        ThreadEvent::Failed { message, .. } => {
            fail_active_turn(app, thread_id, "runtime_failed", message).await;
        }
        ThreadEvent::Cancelled { reason, .. } => {
            interrupt_active_turn(app, thread_id, reason).await;
        }
        ThreadEvent::Started { .. }
        | ThreadEvent::CanonicalMirror { .. }
        | ThreadEvent::Output { .. }
        | ThreadEvent::Signal { .. }
        | ThreadEvent::Stopped { .. } => {}
    }
}

pub(super) async fn handle_runtime_event(
    app: &CooldisAppServer,
    thread_id: &str,
    event: RuntimeEventKind,
) {
    match event {
        RuntimeEventKind::TextDelta { text } => {
            append_agent_delta(app, thread_id, &text).await;
        }
        RuntimeEventKind::ThinkingDelta { text } => {
            append_agent_thinking_delta(app, thread_id, &text).await;
        }
        RuntimeEventKind::ToolCallStarted {
            call_id,
            name,
            input,
        } => {
            let item = json!({
                "type": "dynamicToolCall",
                "id": call_id,
                "namespace": null,
                "tool": name,
                "arguments": input,
                "status": "inProgress",
                "contentItems": null,
                "success": null,
                "durationMs": null,
            });
            let turn_id = {
                let mut state = app.inner.state.write().await;
                let Some(thread) = state.threads.get_mut(thread_id) else {
                    return;
                };
                let Some(turn_id) = thread.active_turn_id.clone() else {
                    return;
                };
                if let Some(turn) = thread.turns.get_mut(&turn_id) {
                    turn.items.push(item.clone());
                }
                turn_id
            };
            app.notify_thread_subscribers(
                thread_id,
                "item/started",
                json!({
                    "item": item,
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "startedAtMs": now_ms(),
                }),
            )
            .await;
        }
        RuntimeEventKind::ToolCallResult {
            call_id,
            output,
            success,
            duration_ms,
        } => {
            let completed =
                complete_dynamic_tool(app, thread_id, &call_id, output, success, duration_ms).await;
            if let Some((turn_id, item)) = completed {
                app.notify_thread_subscribers(
                    thread_id,
                    "item/completed",
                    json!({
                        "item": item,
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "completedAtMs": now_ms(),
                    }),
                )
                .await;
            }
        }
        RuntimeEventKind::Usage { usage } => {
            let turn_id = {
                let state = app.inner.state.read().await;
                state
                    .threads
                    .get(thread_id)
                    .and_then(|thread| thread.active_turn_id.clone())
            };
            if let Some(turn_id) = turn_id {
                app.notify_thread_subscribers(
                    thread_id,
                    "turn/usage",
                    json!({
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "usage": {
                            "inputTokens": usage.input_tokens,
                            "outputTokens": usage.output_tokens,
                            "cacheCreationInputTokens": usage.cache_creation_input_tokens,
                            "cacheReadInputTokens": usage.cache_read_input_tokens,
                        },
                    }),
                )
                .await;
            }
        }
        RuntimeEventKind::Cancelled { reason } => {
            interrupt_active_turn(app, thread_id, reason).await;
        }
        RuntimeEventKind::Failed { code, message } => {
            fail_active_turn(app, thread_id, code, message).await;
        }
        RuntimeEventKind::Terminal {
            state: RuntimeTerminalState::Cancelled,
        } => {
            interrupt_active_turn(app, thread_id, "turn cancelled".to_string()).await;
        }
        RuntimeEventKind::Terminal { .. }
        | RuntimeEventKind::ThreadStarted { .. }
        | RuntimeEventKind::ThreadInteraction { .. }
        | RuntimeEventKind::ToolLog { .. }
        | RuntimeEventKind::HookStarted { .. }
        | RuntimeEventKind::HookCompleted { .. }
        | RuntimeEventKind::ApprovalRequested { .. }
        | RuntimeEventKind::ApprovalResolved { .. }
        | RuntimeEventKind::PermissionDecision { .. }
        | RuntimeEventKind::ContextCompiled { .. }
        | RuntimeEventKind::ModelRequestStarted { .. }
        | RuntimeEventKind::ModelRequestRetryScheduled { .. }
        | RuntimeEventKind::ModelRequestFallbackSelected { .. }
        | RuntimeEventKind::ModelRequestCompleted { .. }
        | RuntimeEventKind::ModelRequestFailed { .. }
        | RuntimeEventKind::Timeout { .. }
        | RuntimeEventKind::PolicyRejected { .. }
        | RuntimeEventKind::Recovery { .. }
        | RuntimeEventKind::SubthreadStarted { .. }
        | RuntimeEventKind::SubthreadFinished { .. }
        | RuntimeEventKind::Checkpoint { .. }
        | RuntimeEventKind::Compaction { .. } => {}
    }
}

pub(super) async fn append_agent_delta(app: &CooldisAppServer, thread_id: &str, delta: &str) {
    let appended = {
        let mut state = app.inner.state.write().await;
        let Some(thread) = state.threads.get_mut(thread_id) else {
            return;
        };
        let Some(turn_id) = thread.active_turn_id.clone() else {
            return;
        };
        let Some(turn) = thread.turns.get_mut(&turn_id) else {
            return;
        };
        if turn.assistant_completed {
            return;
        }
        let item = if !turn.assistant_started {
            turn.assistant_started = true;
            let item = agent_message_item(turn);
            turn.items.push(item.clone());
            Some((turn_id.clone(), item))
        } else {
            None
        };
        turn.assistant_text.push_str(delta);
        thread.updated_at_ms = now_ms();
        (turn_id, turn.assistant_item_id.clone(), item)
    };

    let (turn_id, item_id, started_item) = appended;
    if let Some((turn_id, item)) = started_item {
        app.notify_thread_subscribers(
            thread_id,
            "item/started",
            json!({
                "item": item,
                "threadId": thread_id,
                "turnId": turn_id,
                "startedAtMs": now_ms(),
            }),
        )
        .await;
    }

    app.notify_thread_subscribers(
        thread_id,
        "item/agentMessage/delta",
        json!({
            "threadId": thread_id,
            "turnId": turn_id,
            "itemId": item_id,
            "delta": delta,
        }),
    )
    .await;
}

pub(super) async fn append_agent_thinking_delta(
    app: &CooldisAppServer,
    thread_id: &str,
    delta: &str,
) {
    let appended = {
        let mut state = app.inner.state.write().await;
        let Some(thread) = state.threads.get_mut(thread_id) else {
            return;
        };
        let Some(turn_id) = thread.active_turn_id.clone() else {
            return;
        };
        let Some(turn) = thread.turns.get_mut(&turn_id) else {
            return;
        };
        if turn.thinking_completed {
            return;
        }
        let item = if !turn.thinking_started {
            turn.thinking_started = true;
            let item = agent_thinking_item(turn);
            turn.items.push(item.clone());
            Some((turn_id.clone(), item))
        } else {
            None
        };
        turn.thinking_text.push_str(delta);
        thread.updated_at_ms = now_ms();
        (turn_id, turn.thinking_item_id.clone(), item)
    };

    let (turn_id, item_id, started_item) = appended;
    if let Some((turn_id, item)) = started_item {
        app.notify_thread_subscribers(
            thread_id,
            "item/started",
            json!({
                "item": item,
                "threadId": thread_id,
                "turnId": turn_id,
                "startedAtMs": now_ms(),
            }),
        )
        .await;
    }

    app.notify_thread_subscribers(
        thread_id,
        "item/agentThinking/delta",
        json!({
            "threadId": thread_id,
            "turnId": turn_id,
            "itemId": item_id,
            "delta": delta,
        }),
    )
    .await;
}

pub(super) async fn complete_dynamic_tool(
    app: &CooldisAppServer,
    thread_id: &str,
    call_id: &str,
    output: String,
    success: bool,
    duration_ms: Option<u64>,
) -> Option<(String, Value)> {
    let mut state = app.inner.state.write().await;
    let thread = state.threads.get_mut(thread_id)?;
    let turn_id = thread.active_turn_id.clone()?;
    let turn = thread.turns.get_mut(&turn_id)?;
    for item in &mut turn.items {
        if item.get("id").and_then(Value::as_str) == Some(call_id) {
            item["status"] =
                Value::String(if success { "completed" } else { "failed" }.to_string());
            item["success"] = Value::Bool(success);
            item["durationMs"] = duration_ms.map(Value::from).unwrap_or(Value::Null);
            item["contentItems"] = json!([{ "type": "inputText", "text": output }]);
            return Some((turn_id, item.clone()));
        }
    }
    None
}

pub(super) async fn interrupt_active_turn(app: &CooldisAppServer, thread_id: &str, reason: String) {
    let completed = {
        let mut state = app.inner.state.write().await;
        let Some(thread) = state.threads.get_mut(thread_id) else {
            return;
        };
        let Some(turn_id) = thread.active_turn_id.take() else {
            return;
        };
        thread.turns.get_mut(&turn_id).map(|turn| {
            turn.status = AppServerTurnStatus::Interrupted;
            turn.completed_at_ms = Some(now_ms());
            turn.error = Some(turn_error(reason, None));
            let (turn, completed_items) = finalize_turn_payload(turn);
            (turn_id, turn, completed_items)
        })
    };
    if let Some((turn_id, turn, completed_items)) = completed {
        for item in completed_items {
            app.notify_thread_subscribers(
                thread_id,
                "item/completed",
                json!({
                    "item": item,
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "completedAtMs": now_ms(),
                }),
            )
            .await;
        }
        app.notify_thread_subscribers(
            thread_id,
            "turn/completed",
            json!({ "threadId": thread_id, "turn": turn }),
        )
        .await;
    }
}

pub(super) async fn fail_active_turn(
    app: &CooldisAppServer,
    thread_id: &str,
    code: impl Into<String>,
    message: String,
) {
    let code = code.into();
    let completed = {
        let mut state = app.inner.state.write().await;
        let Some(thread) = state.threads.get_mut(thread_id) else {
            return;
        };
        let Some(turn_id) = thread.active_turn_id.take() else {
            return;
        };
        thread.turns.get_mut(&turn_id).map(|turn| {
            turn.status = AppServerTurnStatus::Failed;
            turn.completed_at_ms = Some(now_ms());
            turn.error = Some(turn_error(message.clone(), Some(code.clone())));
            let (turn_json, completed_items) = finalize_turn_payload(turn);
            (
                turn_id,
                turn_json,
                completed_items,
                turn.error.clone().unwrap_or(Value::Null),
            )
        })
    };
    if let Some((turn_id, turn, completed_items, error)) = completed {
        app.notify_thread_subscribers(
            thread_id,
            "error",
            json!({
                "error": error,
                "willRetry": false,
                "threadId": thread_id,
                "turnId": turn_id,
            }),
        )
        .await;
        for item in completed_items {
            app.notify_thread_subscribers(
                thread_id,
                "item/completed",
                json!({
                    "item": item,
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "completedAtMs": now_ms(),
                }),
            )
            .await;
        }
        app.notify_thread_subscribers(
            thread_id,
            "turn/completed",
            json!({ "threadId": thread_id, "turn": turn }),
        )
        .await;
    }
}
