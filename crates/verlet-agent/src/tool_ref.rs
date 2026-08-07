/// A pin: `mcptool://<server>/<tool>@sha256:<hash>` — the acceptance of a
/// witnessed protocol-tool contract as a content-addressed record.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedToolRef {
    /// Source record name (the `<server>` in `mcp://<server>`).
    pub server: String,
    pub tool_name: String,
    /// `sha256:<hex>` schema hash the pinned contract must match exactly.
    pub schema_hash: String,
}

impl PinnedToolRef {
    /// Parse `mcptool://<server>/<tool>@sha256:<hash>`, fail closed on any
    /// missing or malformed segment.
    pub fn parse(reference: &str) -> crate::VerletResult<Self> {
        let body = reference.strip_prefix("mcptool://").ok_or_else(|| {
            crate::VerletAgentError::RuntimeFactory(format!(
                "pin {reference:?} must start with mcptool://"
            ))
        })?;
        let (path, hash) = body.split_once("@sha256:").ok_or_else(|| {
            crate::VerletAgentError::RuntimeFactory(format!(
                "pin {reference:?} must be content-addressed with @sha256:<hash>"
            ))
        })?;
        if hash.len() != 64
            || !hash
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(crate::VerletAgentError::RuntimeFactory(format!(
                "pin {reference:?} has an invalid sha256 schema hash"
            )));
        }
        let (server, tool_name) = path.split_once('/').ok_or_else(|| {
            crate::VerletAgentError::RuntimeFactory(format!(
                "pin {reference:?} must name a server and a tool as <server>/<tool>"
            ))
        })?;
        if server.is_empty() || tool_name.is_empty() {
            return Err(crate::VerletAgentError::RuntimeFactory(format!(
                "pin {reference:?} must name a server and a tool as <server>/<tool>"
            )));
        }
        Ok(Self {
            server: server.to_string(),
            tool_name: tool_name.to_string(),
            schema_hash: format!("sha256:{hash}"),
        })
    }

    /// `mcp://<server>` — the source record this pin resolves against.
    pub fn server_ref(&self) -> String {
        format!("mcp://{}", self.server)
    }
}
