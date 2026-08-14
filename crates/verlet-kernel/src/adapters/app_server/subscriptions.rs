const INITIAL_THREAD_STATUS_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
const FAILED_THREAD_EVENT_GRACE: std::time::Duration = std::time::Duration::from_millis(20);

#[derive(Default)]
pub(crate) struct AppServerSubscriptions {
    pub(crate) next_subscriber_id: u64,
    pub(crate) next_watcher_id: u64,
    pub(crate) subscribers:
        std::collections::HashMap<String, std::collections::BTreeMap<u64, AppServerSubscriber>>,
    pub(crate) watchers: std::collections::HashMap<String, AppServerThreadWatcher>,
}

#[derive(Clone)]
pub(crate) struct AppServerSubscriber {
    pub(crate) outbound:
        tokio::sync::mpsc::UnboundedSender<crate::adapters::app_server::connection::JsonRpcMessage>,
    pub(crate) opt_out_notifications:
        std::sync::Arc<tokio::sync::RwLock<std::collections::HashSet<String>>>,
}

pub(crate) struct AppServerThreadWatcher {
    pub(crate) id: u64,
    pub(crate) cancellation: tokio_util::sync::CancellationToken,
}

enum ResyncedTurnItem {
    AgentMessage,
    AgentThinking,
    DynamicTool(serde_json::Value),
}

struct ResyncedTurnProjection {
    assistant_text: String,
    thinking_text: String,
    items: Vec<ResyncedTurnItem>,
}

struct ResyncedTurnFacts {
    entry_id: String,
    completions: std::collections::HashMap<
        String,
        crate::kernel::control_decision::ToolCallCompletedPayload,
    >,
    result_entry_ids: std::collections::HashMap<String, String>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct ThreadResyncTestGate {
    entered: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
}

#[cfg(test)]
impl ThreadResyncTestGate {
    pub(crate) async fn wait_until_entered(&self) {
        self.entered.notified().await;
    }

    pub(crate) fn release(&self) {
        self.release.notify_one();
    }
}

#[cfg(test)]
pub(crate) fn install_thread_resync_test_gate(thread_id: &str) -> ThreadResyncTestGate {
    let gate = ThreadResyncTestGate {
        entered: std::sync::Arc::new(tokio::sync::Notify::new()),
        release: std::sync::Arc::new(tokio::sync::Notify::new()),
    };
    thread_resync_test_gates()
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .insert(thread_id.to_string(), gate.clone());
    gate
}

#[cfg(test)]
fn thread_resync_test_gates()
-> &'static std::sync::Mutex<std::collections::HashMap<String, ThreadResyncTestGate>> {
    static GATES: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, ThreadResyncTestGate>>,
    > = std::sync::OnceLock::new();
    GATES.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
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

impl crate::adapters::app_server::VerletAppServer {
    pub(crate) async fn subscribe_thread_connection(
        &self,
        handle: crate::kernel::runtime_host::RuntimeThreadHandle,
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
            .is_none_or(|watcher| watcher.cancellation.is_cancelled());
        if should_spawn {
            subscriptions.watchers.remove(&thread_id);
            let watcher_id = subscriptions.next_watcher_id;
            subscriptions.next_watcher_id = subscriptions.next_watcher_id.saturating_add(1);
            let app = self.clone();
            let watcher_thread_id = thread_id.clone();
            let watcher_cancellation = self.inner.tasks.cancellation();
            let task_cancellation = watcher_cancellation.clone();
            if self.inner.tasks.spawn(async move {
                watch_thread(app.clone(), handle, task_cancellation).await;
                app.finish_thread_watcher(&watcher_thread_id, watcher_id)
                    .await;
            }) {
                subscriptions.watchers.insert(
                    thread_id,
                    AppServerThreadWatcher {
                        id: watcher_id,
                        cancellation: watcher_cancellation,
                    },
                );
            }
        }
        subscriber_id
    }

    pub(crate) async fn unsubscribe_thread_connection(&self, thread_id: &str, subscriber_id: u64) {
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
            watcher.cancellation.cancel();
        }
    }

    pub(crate) async fn finish_thread_watcher(&self, thread_id: &str, watcher_id: u64) {
        let mut subscriptions = self.inner.subscriptions.lock().await;
        if subscriptions
            .watchers
            .get(thread_id)
            .is_some_and(|watcher| watcher.id == watcher_id)
        {
            subscriptions.watchers.remove(thread_id);
        }
    }

    pub(crate) async fn thread_has_active_turn(&self, thread_id: &str) -> bool {
        let state = self.inner.state.read().await;
        state
            .threads
            .get(thread_id)
            .is_some_and(|thread| thread.active_turn_id.is_some())
    }

    pub(crate) async fn abort_thread_watcher_if_idle_and_unsubscribed(&self, thread_id: &str) {
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
            watcher.cancellation.cancel();
        }
    }

    pub(crate) async fn notify_thread_subscribers(
        &self,
        thread_id: &str,
        method: &str,
        params: serde_json::Value,
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
    pub(crate) async fn notify(&self, method: &str, params: serde_json::Value) {
        if self.opt_out_notifications.read().await.contains(method) {
            return;
        }
        let _ = self.outbound.send(
            crate::adapters::app_server::connection::JsonRpcMessage::Notification(
                crate::adapters::app_server::connection::JsonRpcNotification {
                    method: method.to_string(),
                    params: Some(params),
                },
            ),
        );
    }
}

pub(crate) async fn watch_thread(
    app: crate::adapters::app_server::VerletAppServer,
    handle: crate::kernel::runtime_host::RuntimeThreadHandle,
    cancellation: tokio_util::sync::CancellationToken,
) {
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
            _ = cancellation.cancelled() => break,
            event = events.recv() => {
                match event {
                    Ok(event) => {
                        handle_thread_event(&app, &thread_id, event).await;
                        let mut status_value = handle.status();
                        if lagged_events > 0
                            && events.len() == 0
                            && matches!(
                                status_value,
                                verlet_runtime_contracts::ThreadStatus::Idle | verlet_runtime_contracts::ThreadStatus::Stopped | verlet_runtime_contracts::ThreadStatus::Failed
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
                        if matches!(status_value, verlet_runtime_contracts::ThreadStatus::Stopped | verlet_runtime_contracts::ThreadStatus::Failed)
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
                                serde_json::json!({
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
                        verlet_runtime_contracts::ThreadStatus::Idle | verlet_runtime_contracts::ThreadStatus::Stopped | verlet_runtime_contracts::ThreadStatus::Failed
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
                if status_value == verlet_runtime_contracts::ThreadStatus::Failed {
                    handle_failed_thread_status(&app, &thread_id, &mut events).await;
                    break;
                }
                if status_value == verlet_runtime_contracts::ThreadStatus::Stopped {
                    break;
                }
            }
        }
    }
}

pub(crate) async fn resynchronize_thread_after_lag(
    app: &crate::adapters::app_server::VerletAppServer,
    handle: &crate::kernel::runtime_host::RuntimeThreadHandle,
    thread_id: &str,
    lagged_events: u64,
    lagged_turn: Option<&str>,
) -> bool {
    #[cfg(test)]
    pause_thread_resync_for_test(thread_id).await;

    let projection = if let Some(turn_id) = lagged_turn {
        let events = match handle.read_thread_events(None).await {
            Ok(events) => events,
            Err(err) => {
                notify_thread_resync_failed(app, thread_id, lagged_events, err.to_string()).await;
                return false;
            }
        };
        let facts = match resynced_turn_facts(&events, turn_id) {
            Ok(facts) => facts,
            Err(message) => {
                notify_thread_resync_failed(app, thread_id, lagged_events, message).await;
                return false;
            }
        };
        if let Some(facts) = facts {
            let context = match handle.session_context().await {
                Ok(context) => context,
                Err(err) => {
                    notify_thread_resync_failed(app, thread_id, lagged_events, err.to_string())
                        .await;
                    return false;
                }
            };
            let Some(projection) = resynced_turn_projection(
                &context.entries,
                &facts.entry_id,
                &facts.completions,
                &facts.result_entry_ids,
            ) else {
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
        }
    } else {
        None
    };

    let thread = {
        let mut state = app.inner.state.write().await;
        state.threads.get_mut(thread_id).map(|thread| {
            thread.status = handle.status();
            thread.updated_at_ms = crate::adapters::app_server::connection::now_ms();
            if let (Some(turn_id), Some(projection)) = (lagged_turn, projection)
                && let Some(turn) = thread.turns.get_mut(turn_id)
            {
                apply_resynced_turn_projection(turn, projection);
            }
            crate::adapters::app_server::threads::thread_json(thread, true)
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
        serde_json::json!({
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
    app: &crate::adapters::app_server::VerletAppServer,
    thread_id: &str,
    lagged_events: u64,
    message: String,
) {
    app.notify_thread_subscribers(
        thread_id,
        "thread/resync/failed",
        serde_json::json!({
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
    entries: &[verlet_history::SessionEntry],
    entry_id: &str,
    completions: &std::collections::HashMap<
        String,
        crate::kernel::control_decision::ToolCallCompletedPayload,
    >,
    result_entry_ids: &std::collections::HashMap<String, String>,
) -> Option<ResyncedTurnProjection> {
    let user_index = entries.iter().position(|entry| {
        entry.entry_id.to_string() == entry_id
            && matches!(
                &entry.kind,
                verlet_history::SessionEntryKind::Message {
                    message: verlet_history::CanonicalMessage::User { .. }
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
        let verlet_history::SessionEntryKind::Message { message } = &entry.kind else {
            continue;
        };
        match message {
            verlet_history::CanonicalMessage::User { .. } => break,
            verlet_history::CanonicalMessage::Assistant { content, .. } => {
                for content in content {
                    match content {
                        verlet_history::CanonicalContent::Text { text, .. } => {
                            if !text.is_empty() && !saw_agent_message {
                                projection.items.push(ResyncedTurnItem::AgentMessage);
                                saw_agent_message = true;
                            }
                            projection.assistant_text.push_str(text);
                        }
                        verlet_history::CanonicalContent::Thinking { text, .. } => {
                            if !text.is_empty() && !saw_agent_thinking {
                                projection.items.push(ResyncedTurnItem::AgentThinking);
                                saw_agent_thinking = true;
                            }
                            projection.thinking_text.push_str(text);
                        }
                        verlet_history::CanonicalContent::ToolCall {
                            id,
                            name,
                            arguments,
                        } => {
                            projection.items.push(ResyncedTurnItem::DynamicTool(
                                serde_json::json!({
                                    "type": "dynamicToolCall",
                                    "id": id,
                                    "namespace": null,
                                    "tool": name,
                                    "arguments": arguments,
                                    "status": "inProgress",
                                    "contentItems": null,
                                    "success": null,
                                    "durationMs": null,
                                }),
                            ));
                        }
                        verlet_history::CanonicalContent::Image { .. } => {}
                    }
                }
            }
            verlet_history::CanonicalMessage::ToolResult {
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
                                if value.get("id").and_then(serde_json::Value::as_str)
                                    == Some(tool_call_id.as_str())
                        )
                    })
                {
                    item["status"] = serde_json::Value::String(
                        if *is_error { "failed" } else { "completed" }.to_string(),
                    );
                    item["success"] = serde_json::Value::Bool(!is_error);
                    item["contentItems"] = serde_json::json!([{
                        "type": "inputText",
                        "text": crate::adapters::app_server::text_from_canonical_content(content),
                    }]);
                    if let Some(completion) = completions.get(tool_call_id) {
                        item["status"] = serde_json::Value::String(
                            if completion.success {
                                "completed"
                            } else {
                                "failed"
                            }
                            .to_string(),
                        );
                        item["success"] = serde_json::Value::Bool(completion.success);
                        item["durationMs"] = completion
                            .duration_ms
                            .map(serde_json::Value::from)
                            .unwrap_or(serde_json::Value::Null);
                    }
                }
            }
        }
    }
    // Detached completions may be appended after the next user entry. Apply
    // full-stream completion facts and their canonical result content instead
    // of leaving the interrupted turn's dynamic tool stuck in progress.
    for item in &mut projection.items {
        let ResyncedTurnItem::DynamicTool(item) = item else {
            continue;
        };
        let Some(call_id) = item
            .get("id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
        else {
            continue;
        };
        let Some(completion) = completions.get(&call_id) else {
            continue;
        };
        item["status"] = serde_json::Value::String(
            if completion.success {
                "completed"
            } else {
                "failed"
            }
            .to_string(),
        );
        item["success"] = serde_json::Value::Bool(completion.success);
        item["durationMs"] = completion
            .duration_ms
            .map(serde_json::Value::from)
            .unwrap_or(serde_json::Value::Null);
        let result_entry_id = result_entry_ids.get(&call_id);
        if let Some(content) = entries.iter().find_map(|entry| match &entry.kind {
            verlet_history::SessionEntryKind::Message {
                message:
                    verlet_history::CanonicalMessage::ToolResult {
                        tool_call_id,
                        content,
                        ..
                    },
            } if tool_call_id == &call_id
                && result_entry_id
                    .is_some_and(|entry_id| entry.entry_id.to_string() == entry_id.as_str()) =>
            {
                Some(content)
            }
            _ => None,
        }) {
            item["contentItems"] = serde_json::json!([{
                "type": "inputText",
                "text": crate::adapters::app_server::text_from_canonical_content(content),
            }]);
        }
    }
    Some(projection)
}

fn resynced_turn_facts(
    events: &[verlet_history::EventRecord],
    turn_id: &str,
) -> Result<Option<ResyncedTurnFacts>, String> {
    let submitted = events
        .iter()
        .filter(|event| {
            event.kind == verlet_history::EventKind::TurnSubmitted
                && event
                    .payload
                    .get("turn_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(turn_id)
        })
        .filter_map(|event| {
            event
                .payload
                .get("entry_id")
                .and_then(serde_json::Value::as_str)
                .map(|entry_id| (event.sequence.get(), entry_id.to_string()))
        })
        .max_by_key(|(sequence, _)| *sequence);
    let Some((_, entry_id)) = submitted else {
        return Ok(None);
    };

    let mut completions = std::collections::HashMap::new();
    let mut request_calls = std::collections::HashMap::new();
    for event in events {
        match event.kind {
            verlet_history::EventKind::ToolCallRequested
                if event
                    .payload
                    .get("subject")
                    .and_then(|subject| subject.get("turn_id"))
                    .and_then(serde_json::Value::as_str)
                    == Some(turn_id) =>
            {
                let call_id = event
                    .payload
                    .get("subject")
                    .and_then(|subject| subject.get("call_id"))
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| format!("tool.call.requested {} has no call id", event.id))?;
                request_calls.insert(event.id, call_id.to_string());
            }
            verlet_history::EventKind::ToolCallCompleted
                if event
                    .payload
                    .get("subject")
                    .and_then(|subject| subject.get("turn_id"))
                    .and_then(serde_json::Value::as_str)
                    == Some(turn_id) =>
            {
                let payload = serde_json::from_value::<
                    crate::kernel::control_decision::ToolCallCompletedPayload,
                >(event.payload.clone())
                .map_err(|err| format!("tool.call.completed payload is invalid: {err}"))?;
                completions.insert(payload.subject.call_id.clone(), payload);
            }
            _ => {}
        }
    }
    let mut result_entry_ids = std::collections::HashMap::new();
    for event in events
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::SessionEntryAppended)
    {
        let Some(call_id) = event
            .provenance
            .source_event_ids
            .iter()
            .find_map(|source| request_calls.get(source))
        else {
            continue;
        };
        let Some(result_entry_id) = event
            .payload
            .get("entry_id")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        result_entry_ids.insert(call_id.clone(), result_entry_id.to_string());
    }
    Ok(Some(ResyncedTurnFacts {
        entry_id,
        completions,
        result_entry_ids,
    }))
}

fn apply_resynced_turn_projection(
    turn: &mut crate::adapters::app_server::threads::AppServerTurnState,
    projection: ResyncedTurnProjection,
) {
    turn.items.retain(|item| {
        !matches!(
            item.get("type").and_then(serde_json::Value::as_str),
            Some("agentMessage" | "agentThinking" | "dynamicToolCall")
        )
    });
    for item in projection.items {
        turn.items.push(match item {
            ResyncedTurnItem::AgentMessage => {
                crate::adapters::app_server::threads::agent_message_item_from_text(
                    &turn.assistant_item_id,
                    &projection.assistant_text,
                )
            }
            ResyncedTurnItem::AgentThinking => {
                crate::adapters::app_server::threads::agent_thinking_item_from_text(
                    &turn.thinking_item_id,
                    &projection.thinking_text,
                )
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

pub(crate) async fn wait_for_initial_thread_status(
    handle: &crate::kernel::runtime_host::RuntimeThreadHandle,
) {
    if handle.status() != verlet_runtime_contracts::ThreadStatus::Starting {
        return;
    }
    let mut status = handle.subscribe_status();
    let _ = tokio::time::timeout(INITIAL_THREAD_STATUS_WAIT_TIMEOUT, async {
        loop {
            if *status.borrow() != verlet_runtime_contracts::ThreadStatus::Starting {
                return;
            }
            if status.changed().await.is_err() {
                return;
            }
        }
    })
    .await;
}

pub(crate) async fn handle_thread_status(
    app: &crate::adapters::app_server::VerletAppServer,
    thread_id: &str,
    status: verlet_runtime_contracts::ThreadStatus,
) {
    let completion_to_schedule = {
        let mut state = app.inner.state.write().await;
        let Some(thread) = state.threads.get_mut(thread_id) else {
            return;
        };
        thread.status = status;
        thread.updated_at_ms = crate::adapters::app_server::connection::now_ms();
        if matches!(
            status,
            verlet_runtime_contracts::ThreadStatus::Running
                | verlet_runtime_contracts::ThreadStatus::Cancelling
        ) && let Some(turn_id) = thread.active_turn_id.clone()
            && let Some(turn) = thread.turns.get_mut(&turn_id)
        {
            turn.observed_running = true;
        }
        if status == verlet_runtime_contracts::ThreadStatus::Idle {
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
        serde_json::json!({
            "threadId": thread_id,
            "status": crate::adapters::app_server::threads::thread_status_json(status),
        }),
    )
    .await;

    if let Some(turn_id) = completion_to_schedule {
        let app = app.clone();
        let tasks = std::sync::Arc::clone(&app.inner.tasks);
        let thread_id = thread_id.to_string();
        tasks.spawn_cancellable(complete_turn_after_settle(app, thread_id, turn_id));
    }
    if matches!(
        status,
        verlet_runtime_contracts::ThreadStatus::Stopped
            | verlet_runtime_contracts::ThreadStatus::Failed
    ) {
        app.notify_thread_subscribers(
            thread_id,
            "thread/closed",
            serde_json::json!({ "threadId": thread_id }),
        )
        .await;
    }
}

/// Lets queued runtime failure events provide their code and message before a
/// failed thread status falls back to a generic failed-turn projection.
pub(crate) async fn handle_failed_thread_status(
    app: &crate::adapters::app_server::VerletAppServer,
    thread_id: &str,
    events: &mut tokio::sync::broadcast::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
) {
    if !app.thread_has_active_turn(thread_id).await {
        return;
    }
    if let Ok(Ok(event)) = tokio::time::timeout(FAILED_THREAD_EVENT_GRACE, events.recv()).await {
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

pub(crate) async fn complete_turn_after_settle(
    app: crate::adapters::app_server::VerletAppServer,
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
            if turn.status == crate::adapters::app_server::threads::AppServerTurnStatus::InProgress
            {
                turn.status = crate::adapters::app_server::threads::AppServerTurnStatus::Completed;
            }
            turn.completed_at_ms = Some(crate::adapters::app_server::connection::now_ms());
            let synthesized_message = current_turn_agent_content
                .as_ref()
                .map(|content| content.text.as_str())
                .and_then(|text| reconcile_turn_assistant_text(turn, text));
            let synthesized_thinking = current_turn_agent_content
                .as_ref()
                .map(|content| content.thinking.as_str())
                .and_then(|text| reconcile_turn_thinking_text(turn, text));
            let (turn, completed_items) =
                crate::adapters::app_server::threads::finalize_turn_payload(turn);
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
                serde_json::json!({
                    "item": item,
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "startedAtMs": crate::adapters::app_server::connection::now_ms(),
                }),
            )
            .await;
        }
        app.notify_thread_subscribers(
            &thread_id,
            "item/agentMessage/delta",
            serde_json::json!({
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
                serde_json::json!({
                    "item": item,
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "startedAtMs": crate::adapters::app_server::connection::now_ms(),
                }),
            )
            .await;
        }
        app.notify_thread_subscribers(
            &thread_id,
            "item/agentThinking/delta",
            serde_json::json!({
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
            serde_json::json!({
                "item": item,
                "threadId": thread_id,
                "turnId": turn_id,
                "completedAtMs": crate::adapters::app_server::connection::now_ms(),
            }),
        )
        .await;
    }
    app.notify_thread_subscribers(
        &thread_id,
        "turn/completed",
        serde_json::json!({ "threadId": thread_id, "turn": turn }),
    )
    .await;
    app.abort_thread_watcher_if_idle_and_unsubscribed(&thread_id)
        .await;
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct AssistantContentProjection {
    pub(crate) text: String,
    pub(crate) thinking: String,
}

impl AssistantContentProjection {
    fn is_empty(&self) -> bool {
        self.text.is_empty() && self.thinking.is_empty()
    }
}

pub(crate) async fn wait_for_current_turn_assistant_content(
    app: &crate::adapters::app_server::VerletAppServer,
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

pub(crate) async fn turn_is_active(
    app: &crate::adapters::app_server::VerletAppServer,
    thread_id: &str,
    turn_id: &str,
) -> bool {
    let state = app.inner.state.read().await;
    let Some(thread) = state.threads.get(thread_id) else {
        return false;
    };
    thread.active_turn_id.as_deref() == Some(turn_id) && thread.turns.contains_key(turn_id)
}

/// Reconciles completion payloads from saved session text after fast runtime
/// turns, because status notifications can outrun queued text notifications.
pub(crate) fn reconcile_turn_assistant_text(
    turn: &mut crate::adapters::app_server::threads::AppServerTurnState,
    text: &str,
) -> Option<(Option<serde_json::Value>, String, String)> {
    if text.is_empty() {
        return None;
    }

    let item_id = turn.assistant_item_id.clone();
    if !turn.assistant_started {
        let started_item =
            crate::adapters::app_server::threads::agent_message_item_from_text(&item_id, "");
        turn.assistant_started = true;
        turn.assistant_completed = false;
        turn.assistant_text = text.to_string();
        upsert_agent_message_item(turn);
        return Some((Some(started_item), text.to_string(), item_id));
    }

    let prior_text = turn.assistant_text.clone();
    let projected_item_matches = turn.items.iter().any(|item| {
        item.get("id").and_then(serde_json::Value::as_str) == Some(item_id.as_str())
            && item.get("text").and_then(serde_json::Value::as_str) == Some(text)
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

pub(crate) fn reconcile_turn_thinking_text(
    turn: &mut crate::adapters::app_server::threads::AppServerTurnState,
    text: &str,
) -> Option<(Option<serde_json::Value>, String, String)> {
    if text.is_empty() {
        return None;
    }

    let item_id = turn.thinking_item_id.clone();
    if !turn.thinking_started {
        let started_item =
            crate::adapters::app_server::threads::agent_thinking_item_from_text(&item_id, "");
        turn.thinking_started = true;
        turn.thinking_completed = false;
        turn.thinking_text = text.to_string();
        upsert_agent_thinking_item(turn);
        return Some((Some(started_item), text.to_string(), item_id));
    }

    let prior_text = turn.thinking_text.clone();
    let projected_item_matches = turn.items.iter().any(|item| {
        item.get("id").and_then(serde_json::Value::as_str) == Some(item_id.as_str())
            && item.get("text").and_then(serde_json::Value::as_str) == Some(text)
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

pub(crate) fn upsert_agent_message_item(
    turn: &mut crate::adapters::app_server::threads::AppServerTurnState,
) {
    let item_id = turn.assistant_item_id.clone();
    let item = crate::adapters::app_server::threads::agent_message_item(turn);
    if let Some(existing) = turn
        .items
        .iter_mut()
        .find(|item| item.get("id").and_then(serde_json::Value::as_str) == Some(item_id.as_str()))
    {
        *existing = item;
    } else {
        turn.items.push(item);
    }
}

pub(crate) fn upsert_agent_thinking_item(
    turn: &mut crate::adapters::app_server::threads::AppServerTurnState,
) {
    let item_id = turn.thinking_item_id.clone();
    let item = crate::adapters::app_server::threads::agent_thinking_item(turn);
    if let Some(existing) = turn
        .items
        .iter_mut()
        .find(|item| item.get("id").and_then(serde_json::Value::as_str) == Some(item_id.as_str()))
    {
        *existing = item;
    } else {
        turn.items.push(item);
    }
}

pub(crate) async fn current_turn_assistant_content(
    app: &crate::adapters::app_server::VerletAppServer,
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
    let parsed = verlet_runtime_contracts::ThreadId::parse_str(thread_id).ok()?;
    let handle = app
        .inner
        .supervisor
        .get_thread(&app.inner.tenant_id, parsed)
        .await
        .ok()?;
    let context = handle.session_context().await.ok()?;
    assistant_content_after_latest_user(&context.messages, &user_text)
}

pub(crate) fn turn_user_text(
    turn: &crate::adapters::app_server::threads::AppServerTurnState,
) -> Option<String> {
    turn.items
        .iter()
        .find(|item| item.get("type").and_then(serde_json::Value::as_str) == Some("userMessage"))
        .and_then(|item| item.get("content").and_then(serde_json::Value::as_array))
        .map(|content| crate::adapters::app_server::threads::user_input_preview(content))
        .filter(|text| !text.is_empty())
}

pub(crate) fn assistant_content_after_latest_user(
    messages: &[verlet_history::CanonicalMessage],
    user_text: &str,
) -> Option<AssistantContentProjection> {
    let mut after_latest_user = false;
    let mut assistant_content = None;
    for message in messages {
        match message {
            verlet_history::CanonicalMessage::User { content, .. } => {
                after_latest_user =
                    crate::adapters::app_server::text_from_canonical_content(content) == user_text;
                if after_latest_user {
                    assistant_content = None;
                }
            }
            verlet_history::CanonicalMessage::Assistant { content, .. } if after_latest_user => {
                let content = AssistantContentProjection {
                    text: crate::adapters::app_server::text_from_canonical_content(content),
                    thinking: crate::adapters::app_server::thinking_text_from_canonical_content(
                        content,
                    ),
                };
                if !content.is_empty() {
                    assistant_content = Some(content);
                }
            }
            verlet_history::CanonicalMessage::Assistant { .. }
            | verlet_history::CanonicalMessage::ToolResult { .. } => {}
        }
    }
    assistant_content
}

#[cfg(test)]
pub(crate) async fn latest_assistant_text(
    app: &crate::adapters::app_server::VerletAppServer,
    thread_id: &str,
) -> Option<String> {
    let parsed = verlet_runtime_contracts::ThreadId::parse_str(thread_id).ok()?;
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
            verlet_history::CanonicalMessage::Assistant { content, .. } => {
                let text = crate::adapters::app_server::text_from_canonical_content(content);
                (!text.is_empty()).then_some(text)
            }
            verlet_history::CanonicalMessage::User { .. }
            | verlet_history::CanonicalMessage::ToolResult { .. } => None,
        })
}

pub(crate) async fn complete_shell_command(
    connection: crate::adapters::app_server::connection::ConnectionState,
    thread_id: String,
    turn_id: String,
    item_id: String,
    command: String,
    cwd: std::path::PathBuf,
) {
    let started_at = crate::adapters::app_server::connection::now_ms();
    let (exit_code, stdout, stderr) = run_shell_command(cwd.clone(), command.clone()).await;
    let output = format!("{stdout}{stderr}");
    let duration_ms = crate::adapters::app_server::connection::now_ms().saturating_sub(started_at);
    let completed_item = crate::adapters::app_server::threads::command_execution_item(
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
        thread.updated_at_ms = crate::adapters::app_server::connection::now_ms();
        if thread.active_turn_id.as_deref() == Some(turn_id.as_str()) {
            thread.active_turn_id.take();
        }
        let Some(turn) = thread.turns.get_mut(&turn_id) else {
            return;
        };
        for item in &mut turn.items {
            if item.get("id").and_then(serde_json::Value::as_str) == Some(item_id.as_str()) {
                *item = completed_item.clone();
                break;
            }
        }
        turn.status = crate::adapters::app_server::threads::AppServerTurnStatus::Completed;
        turn.completed_at_ms = Some(crate::adapters::app_server::connection::now_ms());
        crate::adapters::app_server::threads::turn_json(turn)
    };

    if !output.is_empty() {
        connection
            .notify(
                "item/commandExecution/outputDelta",
                serde_json::json!({
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
            serde_json::json!({
                "item": completed_item,
                "threadId": thread_id,
                "turnId": turn_id,
                "completedAtMs": crate::adapters::app_server::connection::now_ms(),
            }),
        )
        .await;
    connection
        .notify(
            "turn/completed",
            serde_json::json!({ "threadId": thread_id, "turn": completed_turn }),
        )
        .await;
}

pub(crate) async fn run_shell_command(
    cwd: std::path::PathBuf,
    command: String,
) -> (i32, String, String) {
    let mut process = tokio::process::Command::new("/bin/sh");
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

pub(crate) async fn handle_thread_event(
    app: &crate::adapters::app_server::VerletAppServer,
    thread_id: &str,
    event: crate::kernel::runtime_host::runtime_api::ThreadEvent,
) {
    match event {
        crate::kernel::runtime_host::runtime_api::ThreadEvent::Runtime { event, .. } => {
            handle_runtime_event(app, thread_id, event.kind).await;
        }
        crate::kernel::runtime_host::runtime_api::ThreadEvent::Failed { message, .. } => {
            fail_active_turn(app, thread_id, "runtime_failed", message).await;
        }
        crate::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled { reason, .. } => {
            interrupt_active_turn(app, thread_id, reason).await;
        }
        crate::kernel::runtime_host::runtime_api::ThreadEvent::Started { .. }
        | crate::kernel::runtime_host::runtime_api::ThreadEvent::CanonicalMirror { .. }
        | crate::kernel::runtime_host::runtime_api::ThreadEvent::Output { .. }
        | crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal { .. }
        | crate::kernel::runtime_host::runtime_api::ThreadEvent::Stopped { .. } => {}
    }
}

pub(crate) async fn handle_runtime_event(
    app: &crate::adapters::app_server::VerletAppServer,
    thread_id: &str,
    event: crate::kernel::runtime_host::runtime_events::RuntimeEventKind,
) {
    match event {
        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::TextDelta { text } => {
            append_agent_delta(app, thread_id, &text).await;
        }
        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ThinkingDelta { text } => {
            append_agent_thinking_delta(app, thread_id, &text).await;
        }
        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallStarted {
            call_id,
            name,
            input,
        } => {
            let item = serde_json::json!({
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
                serde_json::json!({
                    "item": item,
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "startedAtMs": crate::adapters::app_server::connection::now_ms(),
                }),
            )
            .await;
        }
        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallResult {
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
                    serde_json::json!({
                        "item": item,
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "completedAtMs": crate::adapters::app_server::connection::now_ms(),
                    }),
                )
                .await;
            }
        }
        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Usage { usage } => {
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
                    serde_json::json!({
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
        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Cancelled { reason } => {
            interrupt_active_turn(app, thread_id, reason).await;
        }
        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Failed { code, message } => {
            fail_active_turn(app, thread_id, code, message).await;
        }
        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
            state: verlet_runtime_contracts::RuntimeTerminalState::Cancelled,
        } => {
            interrupt_active_turn(app, thread_id, "turn cancelled".to_string()).await;
        }
        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ThreadStarted { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ThreadInteraction { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolLog { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::HookStarted { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::HookCompleted { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ApprovalRequested { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ApprovalResolved { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::PermissionDecision { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ContextCompiled { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestStarted { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestRetryScheduled { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestFallbackSelected { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestCompleted { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestFailed { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Timeout { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::PolicyRejected { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Recovery { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::SubthreadStarted { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::SubthreadFinished { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Checkpoint { .. }
        | crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Compaction { .. } => {}
    }
}

pub(crate) async fn append_agent_delta(
    app: &crate::adapters::app_server::VerletAppServer,
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
        if turn.assistant_completed {
            return;
        }
        let item = if !turn.assistant_started {
            turn.assistant_started = true;
            let item = crate::adapters::app_server::threads::agent_message_item(turn);
            turn.items.push(item.clone());
            Some((turn_id.clone(), item))
        } else {
            None
        };
        turn.assistant_text.push_str(delta);
        thread.updated_at_ms = crate::adapters::app_server::connection::now_ms();
        (turn_id, turn.assistant_item_id.clone(), item)
    };

    let (turn_id, item_id, started_item) = appended;
    if let Some((turn_id, item)) = started_item {
        app.notify_thread_subscribers(
            thread_id,
            "item/started",
            serde_json::json!({
                "item": item,
                "threadId": thread_id,
                "turnId": turn_id,
                "startedAtMs": crate::adapters::app_server::connection::now_ms(),
            }),
        )
        .await;
    }

    app.notify_thread_subscribers(
        thread_id,
        "item/agentMessage/delta",
        serde_json::json!({
            "threadId": thread_id,
            "turnId": turn_id,
            "itemId": item_id,
            "delta": delta,
        }),
    )
    .await;
}

pub(crate) async fn append_agent_thinking_delta(
    app: &crate::adapters::app_server::VerletAppServer,
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
            let item = crate::adapters::app_server::threads::agent_thinking_item(turn);
            turn.items.push(item.clone());
            Some((turn_id.clone(), item))
        } else {
            None
        };
        turn.thinking_text.push_str(delta);
        thread.updated_at_ms = crate::adapters::app_server::connection::now_ms();
        (turn_id, turn.thinking_item_id.clone(), item)
    };

    let (turn_id, item_id, started_item) = appended;
    if let Some((turn_id, item)) = started_item {
        app.notify_thread_subscribers(
            thread_id,
            "item/started",
            serde_json::json!({
                "item": item,
                "threadId": thread_id,
                "turnId": turn_id,
                "startedAtMs": crate::adapters::app_server::connection::now_ms(),
            }),
        )
        .await;
    }

    app.notify_thread_subscribers(
        thread_id,
        "item/agentThinking/delta",
        serde_json::json!({
            "threadId": thread_id,
            "turnId": turn_id,
            "itemId": item_id,
            "delta": delta,
        }),
    )
    .await;
}

pub(crate) async fn complete_dynamic_tool(
    app: &crate::adapters::app_server::VerletAppServer,
    thread_id: &str,
    call_id: &str,
    output: String,
    success: bool,
    duration_ms: Option<u64>,
) -> Option<(String, serde_json::Value)> {
    let mut state = app.inner.state.write().await;
    let thread = state.threads.get_mut(thread_id)?;
    let turn_id = thread.active_turn_id.clone()?;
    let turn = thread.turns.get_mut(&turn_id)?;
    for item in &mut turn.items {
        if item.get("id").and_then(serde_json::Value::as_str) == Some(call_id) {
            item["status"] =
                serde_json::Value::String(if success { "completed" } else { "failed" }.to_string());
            item["success"] = serde_json::Value::Bool(success);
            item["durationMs"] = duration_ms
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null);
            item["contentItems"] = serde_json::json!([{ "type": "inputText", "text": output }]);
            return Some((turn_id, item.clone()));
        }
    }
    None
}

pub(crate) async fn interrupt_active_turn(
    app: &crate::adapters::app_server::VerletAppServer,
    thread_id: &str,
    reason: String,
) {
    let completed = {
        let mut state = app.inner.state.write().await;
        let Some(thread) = state.threads.get_mut(thread_id) else {
            return;
        };
        let Some(turn_id) = thread.active_turn_id.take() else {
            return;
        };
        thread.turns.get_mut(&turn_id).map(|turn| {
            turn.status = crate::adapters::app_server::threads::AppServerTurnStatus::Interrupted;
            turn.completed_at_ms = Some(crate::adapters::app_server::connection::now_ms());
            turn.error = Some(crate::adapters::app_server::connection::turn_error(
                reason, None,
            ));
            let (turn, completed_items) =
                crate::adapters::app_server::threads::finalize_turn_payload(turn);
            (turn_id, turn, completed_items)
        })
    };
    if let Some((turn_id, turn, completed_items)) = completed {
        for item in completed_items {
            app.notify_thread_subscribers(
                thread_id,
                "item/completed",
                serde_json::json!({
                    "item": item,
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "completedAtMs": crate::adapters::app_server::connection::now_ms(),
                }),
            )
            .await;
        }
        app.notify_thread_subscribers(
            thread_id,
            "turn/completed",
            serde_json::json!({ "threadId": thread_id, "turn": turn }),
        )
        .await;
    }
}

pub(crate) async fn fail_active_turn(
    app: &crate::adapters::app_server::VerletAppServer,
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
            turn.status = crate::adapters::app_server::threads::AppServerTurnStatus::Failed;
            turn.completed_at_ms = Some(crate::adapters::app_server::connection::now_ms());
            turn.error = Some(crate::adapters::app_server::connection::turn_error(
                message.clone(),
                Some(code.clone()),
            ));
            let (turn_json, completed_items) =
                crate::adapters::app_server::threads::finalize_turn_payload(turn);
            (
                turn_id,
                turn_json,
                completed_items,
                turn.error.clone().unwrap_or(serde_json::Value::Null),
            )
        })
    };
    if let Some((turn_id, turn, completed_items, error)) = completed {
        app.notify_thread_subscribers(
            thread_id,
            "error",
            serde_json::json!({
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
                serde_json::json!({
                    "item": item,
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "completedAtMs": crate::adapters::app_server::connection::now_ms(),
                }),
            )
            .await;
        }
        app.notify_thread_subscribers(
            thread_id,
            "turn/completed",
            serde_json::json!({ "threadId": thread_id, "turn": turn }),
        )
        .await;
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn resync_projects_a_detached_completion_appended_after_the_next_user_entry() {
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "late-completion");
        let old_user = verlet_history::SessionEntry::new(
            coordinates.clone(),
            None,
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::user_text("older turn"),
            },
        );
        let old_result = verlet_history::SessionEntry::new(
            coordinates.clone(),
            Some(old_user.entry_id),
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::tool_result(
                    "call-late",
                    "bash",
                    "older result with a reused call id",
                    false,
                ),
            },
        );
        let user = verlet_history::SessionEntry::new(
            coordinates.clone(),
            Some(old_result.entry_id),
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::user_text("first turn"),
            },
        );
        let assistant = verlet_history::SessionEntry::new(
            coordinates.clone(),
            Some(user.entry_id),
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::assistant(
                    "test",
                    verlet_history::ProviderApi::OpenAIResponses,
                    "model",
                    vec![verlet_history::CanonicalContent::tool_call(
                        "call-late",
                        "bash",
                        serde_json::json!({"command": "slow"}),
                    )],
                    verlet_history::CanonicalStopReason::ToolUse,
                ),
            },
        );
        let next_user = verlet_history::SessionEntry::new(
            coordinates.clone(),
            Some(assistant.entry_id),
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::user_text("replacement turn"),
            },
        );
        let late_result = verlet_history::SessionEntry::new(
            coordinates,
            Some(next_user.entry_id),
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::tool_result(
                    "call-late",
                    "bash",
                    "cancelled after grace",
                    true,
                ),
            },
        );
        let late_result_entry_id = late_result.entry_id.to_string();
        let completion: crate::kernel::control_decision::ToolCallCompletedPayload = serde_json::from_value(
            serde_json::to_value(crate::kernel::control_decision::ToolCallCompletedPayload {
                subject: crate::kernel::control_decision::ToolCallSubject {
                    turn_id: "turn-first".to_string(),
                    call_id: "call-late".to_string(),
                },
                snapshot_id: "snapshot".to_string(),
                tool_name: "bash".to_string(),
                success: false,
                args_fingerprint: None,
                duration_ms: Some(12),
                finish_order: Some(0),
                cancellation: Some(crate::kernel::control_decision::ToolCallCancellation::CancelledExceededGrace),
            })
            .unwrap(),
        )
        .unwrap();
        let projection = crate::adapters::app_server::subscriptions::resynced_turn_projection(
            &[
                old_user,
                old_result,
                user.clone(),
                assistant,
                next_user,
                late_result,
            ],
            &user.entry_id.to_string(),
            &std::collections::HashMap::from([("call-late".to_string(), completion)]),
            &std::collections::HashMap::from([("call-late".to_string(), late_result_entry_id)]),
        )
        .unwrap();
        let tool = projection
            .items
            .iter()
            .find_map(|item| match item {
                crate::adapters::app_server::subscriptions::ResyncedTurnItem::DynamicTool(tool) => {
                    Some(tool)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(tool["status"], "failed");
        assert_eq!(tool["success"], false);
        assert_eq!(tool["durationMs"], 12);
        assert_eq!(tool["contentItems"][0]["text"], "cancelled after grace");
    }
}
