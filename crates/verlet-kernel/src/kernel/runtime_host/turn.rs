use crate::ThinkingConfig;
use crate::kernel::history::CanonicalContent;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{OwnedSemaphorePermit, watch};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use verlet_runtime_contracts::{
    ThreadContext, ThreadCoordinates, ThreadId, ThreadTopology, TurnBudget,
};

#[derive(Clone, Debug)]
pub struct TurnContext {
    pub turn_id: String,
    pub trace_id: String,
    pub thread: ThreadContext,
    pub cwd: Option<PathBuf>,
    pub workspace_roots: Vec<PathBuf>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub thinking: Option<ThinkingConfig>,
    pub permission_profile: Option<String>,
    pub provider_metadata: BTreeMap<String, String>,
    pub metadata: BTreeMap<String, String>,
    pub environment: BTreeMap<String, String>,
    pub model_visible_context: Vec<String>,
    pub budget: TurnBudget,
    pub cancellation: CancellationToken,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TurnContextSnapshot {
    pub turn_id: String,
    pub trace_id: String,
    pub coordinates: ThreadCoordinates,
    pub parent_thread_id: Option<ThreadId>,
    #[serde(default)]
    pub topology: ThreadTopology,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub workspace_roots: Vec<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<ThinkingConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_profile: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub provider_metadata: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_visible_context: Vec<String>,
    #[serde(default, skip_serializing_if = "TurnBudget::is_empty")]
    pub budget: TurnBudget,
    #[serde(default)]
    pub cancellation_requested: bool,
}

impl TurnContext {
    pub fn new(
        thread: ThreadContext,
        turn_id: impl Into<String>,
        input: &TurnInput,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            turn_id: turn_id.into(),
            trace_id: Uuid::now_v7().to_string(),
            thread,
            cwd: input.cwd.clone(),
            workspace_roots: input.workspace_roots.clone(),
            model: input.model.clone(),
            provider: input.provider.clone(),
            thinking: input.thinking.clone(),
            permission_profile: input.permission_profile.clone(),
            provider_metadata: input.provider_metadata.clone(),
            metadata: input.metadata.clone(),
            environment: BTreeMap::new(),
            model_visible_context: Vec::new(),
            budget: TurnBudget::default(),
            cancellation,
        }
    }

    pub fn coordinates(&self) -> &ThreadCoordinates {
        &self.thread.coordinates
    }

    pub fn parent_thread_id(&self) -> Option<ThreadId> {
        self.thread.parent_thread_id
    }

    pub fn with_effective_model_provider(
        mut self,
        provider: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        if self.provider.is_none() {
            self.provider = Some(provider.into());
        }
        if self.model.is_none() {
            self.model = Some(model.into());
        }
        self
    }

    pub fn with_budget(mut self, budget: TurnBudget) -> Self {
        self.budget = budget;
        self
    }

    pub fn with_environment(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.environment.insert(key.into(), value.into());
        self
    }

    pub fn add_model_visible_context(mut self, context: impl Into<String>) -> Self {
        self.model_visible_context.push(context.into());
        self
    }

    pub fn snapshot(&self) -> TurnContextSnapshot {
        TurnContextSnapshot {
            turn_id: self.turn_id.clone(),
            trace_id: self.trace_id.clone(),
            coordinates: self.thread.coordinates.clone(),
            parent_thread_id: self.thread.parent_thread_id,
            topology: self.thread.topology.clone(),
            cwd: self.cwd.clone(),
            workspace_roots: self.workspace_roots.clone(),
            model: self.model.clone(),
            provider: self.provider.clone(),
            thinking: self.thinking.clone(),
            permission_profile: self.permission_profile.clone(),
            provider_metadata: self.provider_metadata.clone(),
            metadata: self.metadata.clone(),
            environment: self.environment.clone(),
            model_visible_context: self.model_visible_context.clone(),
            budget: self.budget.clone(),
            cancellation_requested: self.cancellation.is_cancelled(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum TurnWatchdogPhase {
    Pending,
    Active,
    TimedOut,
    Finished,
}

/// Identifies one submitted turn and tracks only that turn's execution phase.
///
/// Enqueueing another turn cannot start, replace, or retire this token. The
/// token becomes active at the canonical runtime turn-entry boundary and
/// finishes when the corresponding input leaves the runtime.
pub(super) struct TurnWatchdogToken {
    id: u64,
    lease: Arc<TurnWatchdogLease>,
}

struct TurnWatchdogLease {
    state: Arc<TurnWatchdogState>,
}

struct TurnWatchdogState {
    phase: AtomicU8,
    updates: watch::Sender<TurnWatchdogPhase>,
}

impl TurnWatchdogToken {
    pub(super) fn new(id: u64) -> (Self, TurnWatchdogHandle) {
        let (phase, phase_rx) = watch::channel(TurnWatchdogPhase::Pending);
        let state = Arc::new(TurnWatchdogState {
            phase: AtomicU8::new(TurnWatchdogPhase::Pending as u8),
            updates: phase,
        });
        (
            Self {
                id,
                lease: Arc::new(TurnWatchdogLease {
                    state: Arc::clone(&state),
                }),
            },
            TurnWatchdogHandle {
                id,
                state,
                phase: phase_rx,
            },
        )
    }

    fn start(&self) {
        if self
            .lease
            .state
            .phase
            .compare_exchange(
                TurnWatchdogPhase::Pending as u8,
                TurnWatchdogPhase::Active as u8,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
        {
            self.lease
                .state
                .updates
                .send_replace(TurnWatchdogPhase::Active);
        }
    }
}

impl Clone for TurnWatchdogToken {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            lease: Arc::clone(&self.lease),
        }
    }
}

impl fmt::Debug for TurnWatchdogToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TurnWatchdogToken")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl PartialEq for TurnWatchdogToken {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for TurnWatchdogToken {}

impl Drop for TurnWatchdogLease {
    fn drop(&mut self) {
        self.state
            .phase
            .store(TurnWatchdogPhase::Finished as u8, Ordering::SeqCst);
        self.state.updates.send_replace(TurnWatchdogPhase::Finished);
    }
}

pub(super) struct TurnWatchdogHandle {
    id: u64,
    state: Arc<TurnWatchdogState>,
    phase: watch::Receiver<TurnWatchdogPhase>,
}

impl TurnWatchdogHandle {
    pub(super) fn id(&self) -> u64 {
        self.id
    }

    pub(super) async fn wait_until_started(&mut self) -> bool {
        loop {
            match *self.phase.borrow() {
                TurnWatchdogPhase::Pending => {}
                TurnWatchdogPhase::Active | TurnWatchdogPhase::TimedOut => return true,
                TurnWatchdogPhase::Finished => return false,
            }
            if self.phase.changed().await.is_err() {
                return false;
            }
        }
    }

    pub(super) fn try_timeout(&self) -> bool {
        if self
            .state
            .phase
            .compare_exchange(
                TurnWatchdogPhase::Active as u8,
                TurnWatchdogPhase::TimedOut as u8,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
        {
            self.state.updates.send_replace(TurnWatchdogPhase::TimedOut);
            true
        } else {
            false
        }
    }

    pub(super) fn is_timed_out(&self) -> bool {
        self.state.phase.load(Ordering::SeqCst) == TurnWatchdogPhase::TimedOut as u8
    }
}

struct PendingInputPermitToken {
    lease: Arc<PendingInputPermitLease>,
}

struct PendingInputPermitLease {
    permit: StdMutex<Option<OwnedSemaphorePermit>>,
}

impl PendingInputPermitToken {
    fn new(permit: OwnedSemaphorePermit) -> Self {
        Self {
            lease: Arc::new(PendingInputPermitLease {
                permit: StdMutex::new(Some(permit)),
            }),
        }
    }

    fn start(&self) {
        self.lease
            .permit
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .take();
    }
}

impl Clone for PendingInputPermitToken {
    fn clone(&self) -> Self {
        Self {
            lease: Arc::clone(&self.lease),
        }
    }
}

impl fmt::Debug for PendingInputPermitToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingInputPermitToken")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Default)]
struct TurnRuntimeState {
    turn_watchdog: Option<TurnWatchdogToken>,
    pending_input_permit: Option<PendingInputPermitToken>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TurnInput {
    pub content: Vec<TurnContent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub workspace_roots: Vec<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<ThinkingConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_profile: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub provider_metadata: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    #[serde(skip)]
    runtime_state: Option<Box<TurnRuntimeState>>,
}

impl PartialEq for TurnInput {
    fn eq(&self, other: &Self) -> bool {
        self.content == other.content
            && self.cwd == other.cwd
            && self.workspace_roots == other.workspace_roots
            && self.model == other.model
            && self.provider == other.provider
            && self.thinking == other.thinking
            && self.permission_profile == other.permission_profile
            && self.provider_metadata == other.provider_metadata
            && self.metadata == other.metadata
    }
}

impl Eq for TurnInput {}

impl TurnInput {
    pub fn new(content: impl IntoIterator<Item = TurnContent>) -> Self {
        Self {
            content: content.into_iter().collect(),
            cwd: None,
            workspace_roots: Vec::new(),
            model: None,
            provider: None,
            thinking: None,
            permission_profile: None,
            provider_metadata: BTreeMap::new(),
            metadata: BTreeMap::new(),
            runtime_state: None,
        }
    }

    pub(super) fn set_turn_watchdog(&mut self, turn_watchdog: TurnWatchdogToken) {
        self.runtime_state
            .get_or_insert_with(Default::default)
            .turn_watchdog = Some(turn_watchdog);
    }

    pub(super) fn set_pending_input_permit(&mut self, permit: OwnedSemaphorePermit) {
        self.runtime_state
            .get_or_insert_with(Default::default)
            .pending_input_permit = Some(PendingInputPermitToken::new(permit));
    }

    pub(crate) fn turn_watchdog_id(&self) -> Option<u64> {
        self.runtime_state
            .as_ref()
            .and_then(|state| state.turn_watchdog.as_ref())
            .map(|watchdog| watchdog.id)
    }

    pub(super) fn start_turn_watchdog(&self) {
        let Some(runtime_state) = &self.runtime_state else {
            return;
        };
        if let Some(pending_input_permit) = &runtime_state.pending_input_permit {
            pending_input_permit.start();
        }
        if let Some(turn_watchdog) = &runtime_state.turn_watchdog {
            turn_watchdog.start();
        }
    }

    pub fn text(text: impl Into<String>) -> Self {
        Self::new([TurnContent::text(text)])
    }

    pub fn with_cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    pub fn with_workspace_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.workspace_roots.push(root.into());
        self
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    pub fn with_provider(mut self, provider: impl Into<String>) -> Self {
        self.provider = Some(provider.into());
        self
    }

    pub fn with_thinking(mut self, thinking: ThinkingConfig) -> Self {
        self.thinking = Some(thinking);
        self
    }

    pub fn with_permission_profile(mut self, permission_profile: impl Into<String>) -> Self {
        self.permission_profile = Some(permission_profile.into());
        self
    }

    pub fn with_provider_metadata(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.provider_metadata.insert(key.into(), value.into());
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn text_projection(&self) -> String {
        self.start_turn_watchdog();
        self.content
            .iter()
            .filter_map(|content| match content {
                TurnContent::Text { text } => Some(text.as_str()),
                TurnContent::Image { .. } | TurnContent::FileRef { .. } => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub fn canonical_content(&self) -> Vec<CanonicalContent> {
        self.content
            .iter()
            .filter_map(|content| match content {
                TurnContent::Text { text } => Some(CanonicalContent::text(text.clone())),
                TurnContent::Image { data, mime_type } => Some(CanonicalContent::Image {
                    data: data.clone(),
                    mime_type: mime_type.clone(),
                }),
                TurnContent::FileRef { .. } => None,
            })
            .collect()
    }
}

impl From<String> for TurnInput {
    fn from(value: String) -> Self {
        Self::text(value)
    }
}

impl From<&str> for TurnInput {
    fn from(value: &str) -> Self {
        Self::text(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TurnContent {
    Text {
        text: String,
    },
    Image {
        data: String,
        mime_type: String,
    },
    FileRef {
        path: PathBuf,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mime_type: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        size_bytes: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sha256: Option<String>,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        metadata: BTreeMap<String, String>,
    },
}

impl TurnContent {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }

    pub fn image(data: impl Into<String>, mime_type: impl Into<String>) -> Self {
        Self::Image {
            data: data.into(),
            mime_type: mime_type.into(),
        }
    }

    pub fn file_ref(path: impl Into<PathBuf>) -> Self {
        Self::FileRef {
            path: path.into(),
            mime_type: None,
            size_bytes: None,
            sha256: None,
            metadata: BTreeMap::new(),
        }
    }

    pub fn with_mime_type(mut self, mime_type: impl Into<String>) -> Self {
        if let Self::FileRef {
            mime_type: slot, ..
        } = &mut self
        {
            *slot = Some(mime_type.into());
        }
        self
    }

    pub fn with_size_bytes(mut self, size_bytes: u64) -> Self {
        if let Self::FileRef {
            size_bytes: slot, ..
        } = &mut self
        {
            *slot = Some(size_bytes);
        }
        self
    }

    pub fn with_sha256(mut self, sha256: impl Into<String>) -> Self {
        if let Self::FileRef { sha256: slot, .. } = &mut self {
            *slot = Some(sha256.into());
        }
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        if let Self::FileRef { metadata, .. } = &mut self {
            metadata.insert(key.into(), value.into());
        }
        self
    }
}
