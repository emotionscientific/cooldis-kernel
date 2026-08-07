//! The frozen guest authoring contract (ADR 0002).
//!
//! Everything in this module is the user-facing surface that the
//! `#[operation]` / `#[coupling]` attribute macros wrap: a plain typed Rust
//! function, serde-derivable input/output types, and one error enum. The
//! wire encoding stays behind the macro expansion; nothing here names linear
//! memory, exports, or envelopes.

/// The one error type of the guest contract.
///
/// Guest functions return `Result<_, GuestError>`; the macro expansion owns
/// the mapping onto the wire error envelope, so adding a mapping never
/// changes guest source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GuestError {
    /// Input failed to deserialize or validate.
    BadInput(String),
    /// The request names something this guest does not provide.
    Unsupported(String),
    /// A host power failed (HTTP, source/sink IO, cancellation).
    Host(crate::StatusCode),
    /// Guest logic failed.
    Internal(String),
}

impl std::fmt::Display for GuestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadInput(msg) => write!(f, "bad input: {msg}"),
            Self::Unsupported(msg) => write!(f, "unsupported: {msg}"),
            Self::Host(status) => write!(f, "host call failed: {status:?}"),
            Self::Internal(msg) => write!(f, "internal: {msg}"),
        }
    }
}

impl std::error::Error for GuestError {}

impl From<serde_json::Error> for GuestError {
    fn from(err: serde_json::Error) -> Self {
        Self::BadInput(err.to_string())
    }
}

impl From<crate::StatusCode> for GuestError {
    fn from(status: crate::StatusCode) -> Self {
        Self::Host(status)
    }
}

/// Typed view over a `cooldis.coupling.invocation/0.1` envelope.
///
/// This is the single argument of a `#[coupling]` function. Couplings are
/// pure compute: the context exposes recorded events and config, never host
/// powers.
#[derive(Clone, Debug)]
pub struct CouplingContext {
    invocation: crate::CouplingInvocation,
}

impl CouplingContext {
    /// Build a context from the raw invocation envelope.
    pub fn from_invocation(invocation: crate::CouplingInvocation) -> Self {
        Self { invocation }
    }

    /// The event that fired the trigger.
    pub fn trigger(&self) -> &crate::CouplingInvocationEvent {
        &self.invocation.trigger_event
    }

    /// The events selected by the coupling's source selectors, in record
    /// order.
    pub fn sources(&self) -> &[crate::CouplingInvocationEvent] {
        &self.invocation.selected_events
    }

    /// Deserialize the manifest `config` block into a typed struct.
    pub fn config<C: serde::de::DeserializeOwned>(&self) -> Result<C, GuestError> {
        serde_json::from_value(self.invocation.config.clone())
            .map_err(|err| GuestError::BadInput(format!("coupling config: {err}")))
    }

    /// Metadata about the coupling invocation.
    pub fn meta(&self) -> &crate::CouplingInvocationMeta {
        &self.invocation.invocation_meta
    }

    /// Escape hatch to the raw envelope; prefer the typed accessors.
    pub fn invocation(&self) -> &crate::CouplingInvocation {
        &self.invocation
    }
}

/// Builder for the proposed events of a coupling discharge.
///
/// The kernel, not the guest, stamps origin and provenance and enforces the
/// sink grant; a discharge only ever proposes.
#[derive(Clone, Debug, Default)]
pub struct Discharge {
    events: Vec<crate::CouplingDischargeEvent>,
}

impl Discharge {
    /// A discharge with no proposed events — the legal "nothing to do"
    /// outcome.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Propose one event onto `stream` with the given kind and payload.
    pub fn event(
        mut self,
        stream: impl Into<String>,
        kind: impl Into<String>,
        payload: impl serde::Serialize,
    ) -> Result<Self, GuestError> {
        let payload = serde_json::to_value(payload)
            .map_err(|err| GuestError::Internal(format!("discharge payload: {err}")))?;
        self.events.push(crate::CouplingDischargeEvent {
            stream: stream.into(),
            kind: kind.into(),
            payload,
            provenance: None,
        });
        Ok(self)
    }

    /// Propose one event with an already-built JSON payload.
    pub fn event_json(
        mut self,
        stream: impl Into<String>,
        kind: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        self.events.push(crate::CouplingDischargeEvent {
            stream: stream.into(),
            kind: kind.into(),
            payload,
            provenance: None,
        });
        self
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Convert this builder into the wire discharge envelope.
    pub fn into_coupling_discharge(self) -> crate::CouplingDischarge {
        crate::CouplingDischarge::new(self.events)
    }
}

/// Host powers handed to an effectful `#[operation]` function.
///
/// A pure operation takes only its input; an operation that needs HTTP,
/// event emission, or cancellation checks takes `&mut OperationContext` as
/// its first parameter. Every method is a granted, witnessed host call; the
/// context adds types, not authority.
#[derive(Debug)]
pub struct OperationContext {
    invocation: crate::Invocation,
    events: crate::EventSink,
}

impl OperationContext {
    /// Build an operation context from host invocation and event handles.
    pub fn new(invocation: crate::Invocation, events: crate::EventSink) -> Self {
        Self { invocation, events }
    }

    /// Perform a granted HTTP request; returns the response metadata and
    /// body bytes.
    pub fn http(
        &mut self,
        request: &crate::HttpRequest,
        body: &[u8],
    ) -> Result<(crate::HttpResponse, Vec<u8>), GuestError> {
        let request_bytes = request
            .to_json_vec()
            .map_err(|err| GuestError::Internal(format!("http request encode: {err}")))?;
        let sources = crate::http_request(self.invocation, &request_bytes, body, self.events)?;
        let metadata_bytes = crate::read_source_to_end(sources.metadata)?;
        let response: crate::HttpResponse = serde_json::from_slice(&metadata_bytes)
            .map_err(|err| GuestError::Internal(format!("http response decode: {err}")))?;
        let body = crate::read_source_to_end(sources.body)?;
        Ok((response, body))
    }

    /// Emit one progress event onto the operation's JSONL event port.
    pub fn emit(&mut self, event: &impl serde::Serialize) -> Result<(), GuestError> {
        let mut bytes = serde_json::to_vec(event)
            .map_err(|err| GuestError::Internal(format!("event encode: {err}")))?;
        bytes.push(b'\n');
        crate::emit_event(self.invocation, self.events, &bytes)?;
        Ok(())
    }

    /// Errors with `GuestError::Host(Cancelled)` once the host has cancelled
    /// this invocation; long loops should call it at safe points.
    pub fn check_cancelled(&self) -> Result<(), GuestError> {
        crate::check_cancelled(self.invocation).map_err(GuestError::Host)
    }
}
