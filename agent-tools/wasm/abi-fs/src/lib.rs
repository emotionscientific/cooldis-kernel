//! `ToolFs` over the `cooldis_0.1` guest ABI.
//!
//! Adapts the agent-tools filesystem trait onto the host fs imports so the
//! tool cores run unchanged inside a wasm guest against the thread's
//! attached VFS. Confinement is the attachment itself: the host exposes only
//! the VFS world, so no path checks happen here (see the `ToolFs` trait
//! docs; the granted scope is the whole attached VFS). Mutating methods
//! additionally require the `fs.write` capability grant on the invocation;
//! without it the host denies the call and it surfaces as
//! [`verlet_tool_core::ToolFsError::Denied`].
//!
//! Native (non-wasm32) builds compile but every host call fails with a
//! transport error; the adapter is only meaningful inside a wasm guest.

/// [`verlet_tool_core::ToolFs`] backend over the guest ABI fs imports.
pub struct AbiFs {
    root: std::path::PathBuf,
}

impl AbiFs {
    /// `root` is the VFS directory that relative tool paths resolve
    /// against. The embedder passes it in the operation input; the runtime
    /// convention is `/workspace`.
    pub fn new(root: std::path::PathBuf) -> Self {
        Self { root }
    }

    /// Resolve a tool-supplied path onto the VFS: absolute paths pass
    /// through, relative paths join the root.
    fn resolve(&self, path: &std::path::Path) -> std::path::PathBuf {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.root.join(path)
        }
    }
}

impl verlet_tool_core::ToolFs for AbiFs {
    /// `open_file_read` + `read_file` chunks + `close_file`.
    /// Status mapping (all methods): `NotFound` ->
    /// [`verlet_tool_core::ToolFsError::NotFound`], `CapabilityDenied` ->
    /// `Denied`, everything else -> `Io` with the status in the message.
    /// A non-UTF-8 path is `Io` (host paths cross the ABI as UTF-8).
    fn read_file(&self, path: &std::path::Path) -> Result<Vec<u8>, verlet_tool_core::ToolFsError> {
        let _ = self.resolve(path);
        todo!("EMO-605: read via guest ABI")
    }

    /// `open_file_write` + `write_file` + `close_file`; the close is the
    /// commit point (whole-file replace). Parent directories are the tool
    /// core's job (`mkdir` first), matching the ABI contract.
    fn write_file(
        &self,
        path: &std::path::Path,
        content: &[u8],
    ) -> Result<(), verlet_tool_core::ToolFsError> {
        let _ = (self.resolve(path), content);
        todo!("EMO-605: write via guest ABI")
    }

    /// Guest `mkdir` with the same `recursive` semantics.
    fn mkdir(
        &self,
        path: &std::path::Path,
        recursive: bool,
    ) -> Result<(), verlet_tool_core::ToolFsError> {
        let _ = (self.resolve(path), recursive);
        todo!("EMO-605: mkdir via guest ABI")
    }

    /// `stat_path`: kind `Dir` -> `is_dir`, kind `File` -> `is_file`,
    /// `Other` -> neither; size passes through.
    fn stat(
        &self,
        path: &std::path::Path,
    ) -> Result<verlet_tool_core::FileStat, verlet_tool_core::ToolFsError> {
        let _ = self.resolve(path);
        todo!("EMO-605: stat via guest ABI")
    }

    /// `list_dir`: entries map field-for-field (already name-sorted by the
    /// host, matching the deterministic walk order the tools rely on).
    fn read_dir(
        &self,
        path: &std::path::Path,
    ) -> Result<Vec<verlet_tool_core::DirEntry>, verlet_tool_core::ToolFsError> {
        let _ = self.resolve(path);
        todo!("EMO-605: list via guest ABI")
    }

    /// `stat_path` with `NotFound` mapped to `Ok(false)`.
    fn exists(&self, path: &std::path::Path) -> Result<bool, verlet_tool_core::ToolFsError> {
        let _ = self.resolve(path);
        todo!("EMO-605: exists via guest ABI")
    }
}
