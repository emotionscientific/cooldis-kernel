use bashkit::FileSystem as _;
use futures_util::TryStreamExt as _;
use object_store::ObjectStore as _;
use object_store::ObjectStoreExt as _;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VfsMutationKind {
    Write,
    Append,
    Mkdir,
    Remove,
    Rename,
    Copy,
    Chmod,
    SetModifiedTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VfsMutation {
    pub kind: VfsMutationKind,
    pub path: std::path::PathBuf,
    pub target: Option<std::path::PathBuf>,
}

#[derive(Clone)]
pub enum ObjectStoreMountBackend {
    Shared(std::sync::Arc<dyn object_store::ObjectStore>),
    S3(S3ObjectStoreConfig),
}

impl std::fmt::Debug for ObjectStoreMountBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Shared(_) => f.write_str("Shared(<object_store>)"),
            Self::S3(config) => f.debug_tuple("S3").field(config).finish(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ObjectStoreMountConfig {
    pub backend: ObjectStoreMountBackend,
    pub prefix: String,
    pub cache_policy: ObjectStoreCachePolicy,
}

impl ObjectStoreMountConfig {
    pub fn in_memory(prefix: impl Into<String>) -> Self {
        Self {
            backend: ObjectStoreMountBackend::Shared(std::sync::Arc::new(
                object_store::memory::InMemory::new(),
            )),
            prefix: normalize_object_prefix(prefix.into()),
            cache_policy: ObjectStoreCachePolicy::default(),
        }
    }

    pub fn shared(
        store: std::sync::Arc<dyn object_store::ObjectStore>,
        prefix: impl Into<String>,
    ) -> Self {
        Self {
            backend: ObjectStoreMountBackend::Shared(store),
            prefix: normalize_object_prefix(prefix.into()),
            cache_policy: ObjectStoreCachePolicy::default(),
        }
    }

    pub fn s3(config: S3ObjectStoreConfig, prefix: impl Into<String>) -> Self {
        Self {
            backend: ObjectStoreMountBackend::S3(config),
            prefix: normalize_object_prefix(prefix.into()),
            cache_policy: ObjectStoreCachePolicy::default(),
        }
    }

    pub fn r2(config: R2ObjectStoreConfig, prefix: impl Into<String>) -> Self {
        Self::s3(config.into_s3_config(), prefix)
    }

    pub fn with_cache_policy(mut self, policy: ObjectStoreCachePolicy) -> Self {
        self.cache_policy = policy;
        self
    }

    fn build_store(&self) -> bashkit::Result<std::sync::Arc<dyn object_store::ObjectStore>> {
        match &self.backend {
            ObjectStoreMountBackend::Shared(store) => Ok(store.clone()),
            ObjectStoreMountBackend::S3(config) => config.build_store(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectStoreCachePolicy {
    pub max_clean_file_bytes: u64,
    pub max_clean_file_count: usize,
    pub max_single_file_bytes: u64,
}

impl ObjectStoreCachePolicy {
    pub const fn disabled() -> Self {
        Self {
            max_clean_file_bytes: 0,
            max_clean_file_count: 0,
            max_single_file_bytes: 0,
        }
    }

    pub const fn bounded(
        max_clean_file_bytes: u64,
        max_clean_file_count: usize,
        max_single_file_bytes: u64,
    ) -> Self {
        Self {
            max_clean_file_bytes,
            max_clean_file_count,
            max_single_file_bytes,
        }
    }

    fn should_track(&self, size: u64) -> bool {
        self.max_clean_file_bytes > 0
            && self.max_clean_file_count > 0
            && size <= self.max_single_file_bytes
    }
}

impl Default for ObjectStoreCachePolicy {
    fn default() -> Self {
        Self {
            max_clean_file_bytes: 32 * 1024 * 1024,
            max_clean_file_count: 1024,
            max_single_file_bytes: 1024 * 1024,
        }
    }
}

#[derive(Clone)]
pub struct S3ObjectStoreConfig {
    pub bucket: String,
    pub endpoint: Option<String>,
    pub region: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub virtual_hosted_style_request: bool,
    pub allow_http: bool,
}

impl std::fmt::Debug for S3ObjectStoreConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3ObjectStoreConfig")
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field(
                "access_key_id",
                &self.access_key_id.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "virtual_hosted_style_request",
                &self.virtual_hosted_style_request,
            )
            .field("allow_http", &self.allow_http)
            .finish()
    }
}

impl S3ObjectStoreConfig {
    pub fn new(bucket: impl Into<String>, region: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            endpoint: None,
            region: region.into(),
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            virtual_hosted_style_request: false,
            allow_http: false,
        }
    }

    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    pub fn with_credentials(
        mut self,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        self.access_key_id = Some(access_key_id.into());
        self.secret_access_key = Some(secret_access_key.into());
        self
    }

    pub fn with_session_token(mut self, token: impl Into<String>) -> Self {
        self.session_token = Some(token.into());
        self
    }

    pub fn with_allow_http(mut self, allow_http: bool) -> Self {
        self.allow_http = allow_http;
        self
    }

    pub fn with_virtual_hosted_style_request(mut self, enabled: bool) -> Self {
        self.virtual_hosted_style_request = enabled;
        self
    }

    fn build_store(&self) -> bashkit::Result<std::sync::Arc<dyn object_store::ObjectStore>> {
        let mut builder = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_virtual_hosted_style_request(self.virtual_hosted_style_request)
            .with_allow_http(self.allow_http);

        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(access_key_id) = &self.access_key_id {
            builder = builder.with_access_key_id(access_key_id);
        }
        if let Some(secret_access_key) = &self.secret_access_key {
            builder = builder.with_secret_access_key(secret_access_key);
        }
        if let Some(token) = &self.session_token {
            builder = builder.with_token(token);
        }

        builder
            .build()
            .map(|store| {
                std::sync::Arc::new(store) as std::sync::Arc<dyn object_store::ObjectStore>
            })
            .map_err(|err| {
                std::io::Error::other(format!("object store config error: {err}")).into()
            })
    }
}

#[derive(Clone, Debug)]
pub struct R2ObjectStoreConfig {
    pub bucket: String,
    pub account_id: Option<String>,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub allow_http: bool,
}

impl R2ObjectStoreConfig {
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            account_id: None,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            allow_http: false,
        }
    }

    pub fn with_account_id(mut self, account_id: impl Into<String>) -> Self {
        self.account_id = Some(account_id.into());
        self
    }

    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    pub fn with_credentials(
        mut self,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        self.access_key_id = Some(access_key_id.into());
        self.secret_access_key = Some(secret_access_key.into());
        self
    }

    pub fn with_session_token(mut self, token: impl Into<String>) -> Self {
        self.session_token = Some(token.into());
        self
    }

    pub fn into_s3_config(self) -> S3ObjectStoreConfig {
        let endpoint = self.endpoint.or_else(|| {
            self.account_id
                .map(|account| format!("https://{account}.r2.cloudflarestorage.com"))
        });
        S3ObjectStoreConfig {
            bucket: self.bucket,
            endpoint,
            region: "auto".to_string(),
            access_key_id: self.access_key_id,
            secret_access_key: self.secret_access_key,
            session_token: self.session_token,
            virtual_hosted_style_request: false,
            allow_http: self.allow_http,
        }
    }
}

#[async_trait::async_trait]
pub trait VerletVfsBackend: bashkit::FileSystem {
    async fn flush(&self) -> bashkit::Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl VerletVfsBackend for bashkit::InMemoryFs {}

#[derive(Clone)]
pub struct ReadOnlyFileSystem {
    inner: std::sync::Arc<dyn VerletVfsBackend>,
}

impl ReadOnlyFileSystem {
    pub fn new(inner: std::sync::Arc<dyn VerletVfsBackend>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl bashkit::FileSystemExt for ReadOnlyFileSystem {
    fn usage(&self) -> bashkit::FsUsage {
        self.inner.usage()
    }

    fn limits(&self) -> bashkit::FsLimits {
        self.inner.limits()
    }

    fn vfs_snapshot(&self) -> Option<bashkit::VfsSnapshot> {
        self.inner.vfs_snapshot()
    }
}

#[async_trait::async_trait]
impl bashkit::FileSystem for ReadOnlyFileSystem {
    async fn read_file(&self, path: &std::path::Path) -> bashkit::Result<Vec<u8>> {
        self.inner.read_file(path).await
    }

    async fn write_file(&self, _path: &std::path::Path, _content: &[u8]) -> bashkit::Result<()> {
        Err(readonly_error())
    }

    async fn append_file(&self, _path: &std::path::Path, _content: &[u8]) -> bashkit::Result<()> {
        Err(readonly_error())
    }

    async fn mkdir(&self, _path: &std::path::Path, _recursive: bool) -> bashkit::Result<()> {
        Err(readonly_error())
    }

    async fn remove(&self, _path: &std::path::Path, _recursive: bool) -> bashkit::Result<()> {
        Err(readonly_error())
    }

    async fn stat(&self, path: &std::path::Path) -> bashkit::Result<bashkit::Metadata> {
        self.inner.stat(path).await
    }

    async fn read_dir(&self, path: &std::path::Path) -> bashkit::Result<Vec<bashkit::DirEntry>> {
        self.inner.read_dir(path).await
    }

    async fn exists(&self, path: &std::path::Path) -> bashkit::Result<bool> {
        self.inner.exists(path).await
    }

    async fn rename(&self, _from: &std::path::Path, _to: &std::path::Path) -> bashkit::Result<()> {
        Err(readonly_error())
    }

    async fn copy(&self, _from: &std::path::Path, _to: &std::path::Path) -> bashkit::Result<()> {
        Err(readonly_error())
    }

    async fn symlink(
        &self,
        _target: &std::path::Path,
        _link: &std::path::Path,
    ) -> bashkit::Result<()> {
        Err(readonly_error())
    }

    async fn read_link(&self, path: &std::path::Path) -> bashkit::Result<std::path::PathBuf> {
        self.inner.read_link(path).await
    }

    async fn chmod(&self, _path: &std::path::Path, _mode: u32) -> bashkit::Result<()> {
        Err(readonly_error())
    }

    async fn set_modified_time(
        &self,
        _path: &std::path::Path,
        _time: std::time::SystemTime,
    ) -> bashkit::Result<()> {
        Err(readonly_error())
    }
}

#[async_trait::async_trait]
impl VerletVfsBackend for ReadOnlyFileSystem {
    async fn flush(&self) -> bashkit::Result<()> {
        self.inner.flush().await
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostFileSystemMode {
    ReadOnly,
    ReadWrite,
}

impl HostFileSystemMode {
    fn as_realfs_mode(self) -> bashkit::RealFsMode {
        match self {
            Self::ReadOnly => bashkit::RealFsMode::ReadOnly,
            Self::ReadWrite => bashkit::RealFsMode::ReadWrite,
        }
    }
}

pub struct HostFileSystem {
    root: std::path::PathBuf,
    mode: HostFileSystemMode,
    inner: bashkit::PosixFs<bashkit::RealFs>,
    operation_lock: std::sync::Arc<futures_util::lock::Mutex<()>>,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct HostFileSystemLockKey {
    device: u64,
    inode: u64,
}

#[cfg(not(unix))]
type HostFileSystemLockKey = std::path::PathBuf;

type HostFileSystemOperationLock = futures_util::lock::Mutex<()>;

fn shared_host_filesystem_operation_lock(
    root: &std::path::Path,
) -> std::io::Result<std::sync::Arc<HostFileSystemOperationLock>> {
    static LOCKS: std::sync::OnceLock<
        std::sync::Mutex<
            std::collections::HashMap<
                HostFileSystemLockKey,
                std::sync::Weak<HostFileSystemOperationLock>,
            >,
        >,
    > = std::sync::OnceLock::new();
    #[cfg(unix)]
    let key = {
        use std::os::unix::fs::MetadataExt as _;
        let metadata = std::fs::metadata(root)?;
        HostFileSystemLockKey {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    };
    #[cfg(not(unix))]
    let key = root.to_path_buf();

    let mut locks = LOCKS
        .get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(&key).and_then(std::sync::Weak::upgrade) {
        return Ok(lock);
    }
    let lock = std::sync::Arc::new(futures_util::lock::Mutex::new(()));
    locks.insert(key, std::sync::Arc::downgrade(&lock));
    Ok(lock)
}

impl HostFileSystem {
    pub fn new(
        root: impl AsRef<std::path::Path>,
        mode: HostFileSystemMode,
    ) -> bashkit::Result<Self> {
        let realfs = bashkit::RealFs::new(root, mode.as_realfs_mode())?;
        let root = realfs.root().to_path_buf();
        let operation_lock = shared_host_filesystem_operation_lock(&root)?;
        Ok(Self {
            root,
            mode,
            inner: bashkit::PosixFs::new(realfs),
            operation_lock,
        })
    }

    pub fn read_only(root: impl AsRef<std::path::Path>) -> bashkit::Result<Self> {
        Self::new(root, HostFileSystemMode::ReadOnly)
    }

    pub fn read_write(root: impl AsRef<std::path::Path>) -> bashkit::Result<Self> {
        Self::new(root, HostFileSystemMode::ReadWrite)
    }

    pub fn root(&self) -> &std::path::Path {
        &self.root
    }

    pub fn mode(&self) -> HostFileSystemMode {
        self.mode
    }

    fn reject_external_hard_link_mutation(&self, path: &std::path::Path) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;

            let normalized = normalize_vfs_path(path);
            let relative = normalized.strip_prefix("/").unwrap_or(&normalized);
            let joined = self.root.join(relative);
            if !joined.exists() {
                return Ok(());
            }
            let canonical = std::fs::canonicalize(&joined)?;
            if !canonical.starts_with(&self.root) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "path escapes host filesystem root",
                ));
            }
            let metadata = std::fs::metadata(&canonical)?;
            if metadata.is_file() && metadata.nlink() > 1 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "refusing to mutate a multiply-linked host file outside the mount boundary",
                ));
            }
        }
        #[cfg(not(unix))]
        let _ = path;
        Ok(())
    }

    #[cfg(unix)]
    fn host_path_is_special(&self, path: &std::path::Path) -> std::io::Result<bool> {
        use std::os::unix::fs::FileTypeExt as _;

        let normalized = normalize_vfs_path(path);
        let relative = normalized.strip_prefix("/").unwrap_or(normalized.as_path());
        let file_type = std::fs::symlink_metadata(self.root.join(relative))?.file_type();
        Ok(file_type.is_socket()
            || file_type.is_fifo()
            || file_type.is_block_device()
            || file_type.is_char_device())
    }

    #[cfg(unix)]
    async fn reject_special_file_open(
        &self,
        path: &std::path::Path,
        allow_missing: bool,
    ) -> bashkit::Result<()> {
        match self.inner.stat(path).await {
            Ok(_) => {}
            Err(bashkit::Error::Io(error))
                if allow_missing && error.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(());
            }
            Err(error) => return Err(error),
        }
        match self.host_path_is_special(path) {
            Ok(true) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("path is not a regular file: {}", path.display()),
            )
            .into()),
            Ok(false) => Ok(()),
            Err(error) if allow_missing && error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

impl std::fmt::Debug for HostFileSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostFileSystem")
            .field("root", &self.root)
            .field("mode", &self.mode)
            .finish()
    }
}

#[async_trait::async_trait]
impl bashkit::FileSystemExt for HostFileSystem {
    fn usage(&self) -> bashkit::FsUsage {
        self.inner.usage()
    }

    fn limits(&self) -> bashkit::FsLimits {
        self.inner.limits()
    }
}

#[async_trait::async_trait]
impl bashkit::FileSystem for HostFileSystem {
    async fn read_file(&self, path: &std::path::Path) -> bashkit::Result<Vec<u8>> {
        let _guard = self.operation_lock.lock().await;
        #[cfg(unix)]
        self.reject_special_file_open(path, false).await?;
        self.inner.read_file(path).await
    }

    async fn write_file(&self, path: &std::path::Path, content: &[u8]) -> bashkit::Result<()> {
        let _guard = self.operation_lock.lock().await;
        self.reject_external_hard_link_mutation(path)?;
        #[cfg(unix)]
        self.reject_special_file_open(path, true).await?;
        self.inner.write_file(path, content).await
    }

    async fn append_file(&self, path: &std::path::Path, content: &[u8]) -> bashkit::Result<()> {
        let _guard = self.operation_lock.lock().await;
        self.reject_external_hard_link_mutation(path)?;
        #[cfg(unix)]
        self.reject_special_file_open(path, true).await?;
        self.inner.append_file(path, content).await
    }

    async fn mkdir(&self, path: &std::path::Path, recursive: bool) -> bashkit::Result<()> {
        let _guard = self.operation_lock.lock().await;
        self.inner.mkdir(path, recursive).await
    }

    async fn remove(&self, path: &std::path::Path, recursive: bool) -> bashkit::Result<()> {
        let _guard = self.operation_lock.lock().await;
        self.inner.remove(path, recursive).await
    }

    async fn stat(&self, path: &std::path::Path) -> bashkit::Result<bashkit::Metadata> {
        let _guard = self.operation_lock.lock().await;
        let mut metadata = self.inner.stat(path).await?;
        #[cfg(unix)]
        if metadata.file_type == bashkit::FileType::File && self.host_path_is_special(path)? {
            metadata.file_type = bashkit::FileType::Fifo;
            metadata.size = 0;
        }
        Ok(metadata)
    }

    async fn read_dir(&self, path: &std::path::Path) -> bashkit::Result<Vec<bashkit::DirEntry>> {
        let _guard = self.operation_lock.lock().await;
        let mut entries = self.inner.read_dir(path).await?;
        #[cfg(unix)]
        for entry in &mut entries {
            if entry.metadata.file_type != bashkit::FileType::File {
                continue;
            }
            match self.host_path_is_special(&path.join(&entry.name)) {
                Ok(true) => {
                    entry.metadata.file_type = bashkit::FileType::Fifo;
                    entry.metadata.size = 0;
                }
                Ok(false) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(entries)
    }

    async fn exists(&self, path: &std::path::Path) -> bashkit::Result<bool> {
        let _guard = self.operation_lock.lock().await;
        self.inner.exists(path).await
    }

    async fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> bashkit::Result<()> {
        let _guard = self.operation_lock.lock().await;
        self.inner.rename(from, to).await
    }

    async fn copy(&self, from: &std::path::Path, to: &std::path::Path) -> bashkit::Result<()> {
        let _guard = self.operation_lock.lock().await;
        self.reject_external_hard_link_mutation(to)?;
        #[cfg(unix)]
        self.reject_special_file_open(from, false).await?;
        self.inner.copy(from, to).await
    }

    async fn symlink(
        &self,
        target: &std::path::Path,
        link: &std::path::Path,
    ) -> bashkit::Result<()> {
        let _guard = self.operation_lock.lock().await;
        self.inner.symlink(target, link).await
    }

    async fn read_link(&self, path: &std::path::Path) -> bashkit::Result<std::path::PathBuf> {
        let _guard = self.operation_lock.lock().await;
        self.inner.read_link(path).await
    }

    async fn chmod(&self, path: &std::path::Path, mode: u32) -> bashkit::Result<()> {
        let _guard = self.operation_lock.lock().await;
        self.reject_external_hard_link_mutation(path)?;
        self.inner.chmod(path, mode).await
    }

    async fn set_modified_time(
        &self,
        path: &std::path::Path,
        time: std::time::SystemTime,
    ) -> bashkit::Result<()> {
        let _guard = self.operation_lock.lock().await;
        self.reject_external_hard_link_mutation(path)?;
        #[cfg(unix)]
        self.reject_special_file_open(path, false).await?;
        self.inner.set_modified_time(path, time).await
    }
}

#[async_trait::async_trait]
impl VerletVfsBackend for HostFileSystem {}

pub struct VerletVfs {
    root: std::sync::Arc<dyn VerletVfsBackend>,
    mounts: std::sync::RwLock<
        std::collections::BTreeMap<std::path::PathBuf, std::sync::Arc<dyn VerletVfsBackend>>,
    >,
    journal: std::sync::Mutex<Vec<VfsMutation>>,
}

impl VerletVfs {
    pub fn new(root: std::sync::Arc<dyn VerletVfsBackend>) -> Self {
        Self {
            root,
            mounts: std::sync::RwLock::new(std::collections::BTreeMap::new()),
            journal: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn mount(
        &self,
        path: impl AsRef<std::path::Path>,
        fs: std::sync::Arc<dyn VerletVfsBackend>,
    ) -> bashkit::Result<()> {
        if !path.as_ref().has_root() {
            return Err(std::io::Error::other("mount path must be absolute").into());
        }
        let path = normalize_vfs_path(path.as_ref());
        if path == std::path::Path::new("/") {
            return Err(std::io::Error::other("mount path must not be /").into());
        }
        self.mounts.write().unwrap().insert(path, fs);
        Ok(())
    }

    pub fn has_mount(&self, path: impl AsRef<std::path::Path>) -> bool {
        let path = normalize_vfs_path(path.as_ref());
        self.mounts
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .contains_key(&path)
    }

    pub fn mutations(&self) -> Vec<VfsMutation> {
        self.journal
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
    }

    pub fn clear_mutations(&self) {
        self.journal
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clear();
    }

    pub async fn flush(&self) -> bashkit::Result<()> {
        self.root.flush().await?;
        let mounts: Vec<_> = self.mounts.read().unwrap().values().cloned().collect();
        for mount in mounts {
            mount.flush().await?;
        }
        Ok(())
    }

    fn record(
        &self,
        kind: VfsMutationKind,
        path: std::path::PathBuf,
        target: Option<std::path::PathBuf>,
    ) {
        self.journal
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .push(VfsMutation { kind, path, target });
    }

    fn resolve(
        &self,
        path: &std::path::Path,
    ) -> (std::sync::Arc<dyn VerletVfsBackend>, std::path::PathBuf) {
        let path = normalize_vfs_path(path);
        let mounts = self.mounts.read().unwrap();
        let best = mounts
            .iter()
            .filter(|(mount_path, _)| path.starts_with(mount_path))
            .max_by_key(|(mount_path, _)| mount_path.components().count());

        match best {
            Some((mount_path, fs)) => {
                let relative = path
                    .strip_prefix(mount_path)
                    .unwrap_or(std::path::Path::new(""));
                let resolved = if relative.as_os_str().is_empty() {
                    std::path::PathBuf::from("/")
                } else {
                    std::path::PathBuf::from("/").join(relative)
                };
                (fs.clone(), resolved)
            }
            None => (self.root.clone(), path),
        }
    }

    fn validate_path(&self, path: &std::path::Path) -> bashkit::Result<()> {
        self.root.limits().validate_path(path).map_err(|err| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, err.to_string()).into()
        })
    }

    fn direct_child_mount_names(&self, path: &std::path::Path) -> Vec<String> {
        self.mounts
            .read()
            .unwrap()
            .keys()
            .filter_map(|mount_path| {
                if mount_path.parent() == Some(path) {
                    mount_path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .map(str::to_string)
                } else {
                    None
                }
            })
            .collect()
    }
}

#[async_trait::async_trait]
impl bashkit::FileSystemExt for VerletVfs {
    fn usage(&self) -> bashkit::FsUsage {
        self.root.usage()
    }

    fn limits(&self) -> bashkit::FsLimits {
        self.root.limits()
    }
}

#[async_trait::async_trait]
impl bashkit::FileSystem for VerletVfs {
    async fn read_file(&self, path: &std::path::Path) -> bashkit::Result<Vec<u8>> {
        let (fs, resolved) = self.resolve(path);
        fs.read_file(&resolved).await
    }

    async fn write_file(&self, path: &std::path::Path, content: &[u8]) -> bashkit::Result<()> {
        self.validate_path(path)?;
        let path = normalize_vfs_path(path);
        let (fs, resolved) = self.resolve(&path);
        fs.write_file(&resolved, content).await?;
        self.record(VfsMutationKind::Write, path, None);
        Ok(())
    }

    async fn append_file(&self, path: &std::path::Path, content: &[u8]) -> bashkit::Result<()> {
        self.validate_path(path)?;
        let path = normalize_vfs_path(path);
        let (fs, resolved) = self.resolve(&path);
        fs.append_file(&resolved, content).await?;
        self.record(VfsMutationKind::Append, path, None);
        Ok(())
    }

    async fn mkdir(&self, path: &std::path::Path, recursive: bool) -> bashkit::Result<()> {
        self.validate_path(path)?;
        let path = normalize_vfs_path(path);
        let (fs, resolved) = self.resolve(&path);
        fs.mkdir(&resolved, recursive).await?;
        self.record(VfsMutationKind::Mkdir, path, None);
        Ok(())
    }

    async fn remove(&self, path: &std::path::Path, recursive: bool) -> bashkit::Result<()> {
        self.validate_path(path)?;
        let path = normalize_vfs_path(path);
        let (fs, resolved) = self.resolve(&path);
        fs.remove(&resolved, recursive).await?;
        self.record(VfsMutationKind::Remove, path, None);
        Ok(())
    }

    async fn stat(&self, path: &std::path::Path) -> bashkit::Result<bashkit::Metadata> {
        let path = normalize_vfs_path(path);
        if self.mounts.read().unwrap().contains_key(&path)
            || !self.direct_child_mount_names(&path).is_empty()
        {
            return Ok(directory_metadata());
        }
        let (fs, resolved) = self.resolve(&path);
        fs.stat(&resolved).await
    }

    async fn read_dir(&self, path: &std::path::Path) -> bashkit::Result<Vec<bashkit::DirEntry>> {
        let path = normalize_vfs_path(path);
        let (fs, resolved) = self.resolve(&path);
        let child_mounts = self.direct_child_mount_names(&path);
        let mut entries = match fs.read_dir(&resolved).await {
            Ok(entries) => entries,
            Err(_) if !child_mounts.is_empty() => Vec::new(),
            Err(err) => return Err(err),
        };
        let mut names = entries
            .iter()
            .map(|entry| entry.name.clone())
            .collect::<std::collections::BTreeSet<_>>();

        for name in child_mounts {
            if names.insert(name.clone()) {
                entries.push(bashkit::DirEntry {
                    name,
                    metadata: directory_metadata(),
                });
            }
        }
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(entries)
    }

    async fn exists(&self, path: &std::path::Path) -> bashkit::Result<bool> {
        let path = normalize_vfs_path(path);
        if self.mounts.read().unwrap().contains_key(&path)
            || !self.direct_child_mount_names(&path).is_empty()
        {
            return Ok(true);
        }
        let (fs, resolved) = self.resolve(&path);
        fs.exists(&resolved).await
    }

    async fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> bashkit::Result<()> {
        self.validate_path(from)?;
        self.validate_path(to)?;
        let from = normalize_vfs_path(from);
        let to = normalize_vfs_path(to);
        let (from_fs, from_resolved) = self.resolve(&from);
        let (to_fs, to_resolved) = self.resolve(&to);

        if std::sync::Arc::ptr_eq(&from_fs, &to_fs) {
            from_fs.rename(&from_resolved, &to_resolved).await?;
        } else {
            let meta = from_fs.stat(&from_resolved).await?;
            if meta.file_type == bashkit::FileType::Symlink {
                let target = from_fs.read_link(&from_resolved).await?;
                to_fs.symlink(&target, &to_resolved).await?;
            } else {
                let content = from_fs.read_file(&from_resolved).await?;
                to_fs.write_file(&to_resolved, &content).await?;
            }
            from_fs.remove(&from_resolved, false).await?;
        }

        self.record(VfsMutationKind::Rename, from, Some(to));
        Ok(())
    }

    async fn copy(&self, from: &std::path::Path, to: &std::path::Path) -> bashkit::Result<()> {
        self.validate_path(from)?;
        self.validate_path(to)?;
        let from = normalize_vfs_path(from);
        let to = normalize_vfs_path(to);
        let (from_fs, from_resolved) = self.resolve(&from);
        let (to_fs, to_resolved) = self.resolve(&to);

        if std::sync::Arc::ptr_eq(&from_fs, &to_fs) {
            from_fs.copy(&from_resolved, &to_resolved).await?;
        } else {
            let meta = from_fs.stat(&from_resolved).await?;
            if meta.file_type == bashkit::FileType::Symlink {
                let target = from_fs.read_link(&from_resolved).await?;
                to_fs.symlink(&target, &to_resolved).await?;
            } else {
                let content = from_fs.read_file(&from_resolved).await?;
                to_fs.write_file(&to_resolved, &content).await?;
            }
        }

        self.record(VfsMutationKind::Copy, from, Some(to));
        Ok(())
    }

    async fn symlink(
        &self,
        target: &std::path::Path,
        link: &std::path::Path,
    ) -> bashkit::Result<()> {
        self.validate_path(link)?;
        let (fs, resolved) = self.resolve(link);
        fs.symlink(target, &resolved).await
    }

    async fn read_link(&self, path: &std::path::Path) -> bashkit::Result<std::path::PathBuf> {
        let (fs, resolved) = self.resolve(path);
        fs.read_link(&resolved).await
    }

    async fn chmod(&self, path: &std::path::Path, mode: u32) -> bashkit::Result<()> {
        self.validate_path(path)?;
        let path = normalize_vfs_path(path);
        let (fs, resolved) = self.resolve(&path);
        fs.chmod(&resolved, mode).await?;
        self.record(VfsMutationKind::Chmod, path, None);
        Ok(())
    }

    async fn set_modified_time(
        &self,
        path: &std::path::Path,
        time: std::time::SystemTime,
    ) -> bashkit::Result<()> {
        self.validate_path(path)?;
        let path = normalize_vfs_path(path);
        let (fs, resolved) = self.resolve(&path);
        fs.set_modified_time(&resolved, time).await?;
        self.record(VfsMutationKind::SetModifiedTime, path, None);
        Ok(())
    }
}

pub struct ManagedObjectStoreFs {
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    prefix: String,
    state: std::sync::Arc<bashkit::InMemoryFs>,
    cache_policy: ObjectStoreCachePolicy,
    cache: std::sync::Mutex<std::collections::BTreeMap<std::path::PathBuf, CachedCleanFile>>,
    cache_clock: std::sync::atomic::AtomicU64,
    dirty: std::sync::Mutex<Vec<QueuedObjectStoreWriteback>>,
    dirty_clock: std::sync::atomic::AtomicU64,
    deleted: std::sync::Mutex<std::collections::BTreeSet<std::path::PathBuf>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CachedCleanFile {
    size: u64,
    last_access: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct QueuedObjectStoreWriteback {
    id: u64,
    op: ObjectStoreWriteback,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ObjectStoreWriteback {
    PutFile(std::path::PathBuf),
    PutDir(std::path::PathBuf),
    DeleteFile(std::path::PathBuf),
    DeleteDir(std::path::PathBuf),
}

impl ManagedObjectStoreFs {
    pub fn new(config: ObjectStoreMountConfig) -> bashkit::Result<Self> {
        Ok(Self {
            store: config.build_store()?,
            prefix: normalize_object_prefix(config.prefix),
            state: std::sync::Arc::new(bashkit::InMemoryFs::new()),
            cache_policy: config.cache_policy,
            cache: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            cache_clock: std::sync::atomic::AtomicU64::new(0),
            dirty: std::sync::Mutex::new(Vec::new()),
            dirty_clock: std::sync::atomic::AtomicU64::new(0),
            deleted: std::sync::Mutex::new(std::collections::BTreeSet::new()),
        })
    }

    pub fn from_store(
        store: std::sync::Arc<dyn object_store::ObjectStore>,
        prefix: impl Into<String>,
    ) -> Self {
        Self {
            store,
            prefix: normalize_object_prefix(prefix.into()),
            state: std::sync::Arc::new(bashkit::InMemoryFs::new()),
            cache_policy: ObjectStoreCachePolicy::default(),
            cache: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            cache_clock: std::sync::atomic::AtomicU64::new(0),
            dirty: std::sync::Mutex::new(Vec::new()),
            dirty_clock: std::sync::atomic::AtomicU64::new(0),
            deleted: std::sync::Mutex::new(std::collections::BTreeSet::new()),
        }
    }

    pub fn with_cache_policy(mut self, policy: ObjectStoreCachePolicy) -> Self {
        self.cache_policy = policy;
        self
    }

    fn push_dirty(&self, op: ObjectStoreWriteback) {
        let id = self
            .dirty_clock
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        self.dirty
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .push(QueuedObjectStoreWriteback { id, op });
    }

    fn next_cache_access(&self) -> u64 {
        self.cache_clock
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1
    }

    fn dirty_paths(&self) -> std::collections::BTreeSet<std::path::PathBuf> {
        self.dirty
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .iter()
            .flat_map(|queued| match &queued.op {
                ObjectStoreWriteback::PutFile(path)
                | ObjectStoreWriteback::PutDir(path)
                | ObjectStoreWriteback::DeleteFile(path)
                | ObjectStoreWriteback::DeleteDir(path) => {
                    vec![normalize_vfs_path(path)]
                }
            })
            .collect()
    }

    fn rewrite_pending_puts_for_rename(&self, from: &std::path::Path, to: &std::path::Path) {
        let from = normalize_vfs_path(from);
        let to = normalize_vfs_path(to);
        for queued in self
            .dirty
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .iter_mut()
        {
            match &mut queued.op {
                ObjectStoreWriteback::PutFile(path) | ObjectStoreWriteback::PutDir(path)
                    if path.as_path() == from.as_path() || path.starts_with(&from) =>
                {
                    *path = rebase_path(path, &from, &to);
                }
                _ => {}
            }
        }
    }

    fn mark_clean_cached(&self, path: &std::path::Path, size: u64) {
        let path = normalize_vfs_path(path);
        let mut cache = self.cache.lock().unwrap_or_else(|err| err.into_inner());
        if self.cache_policy.should_track(size) {
            cache.insert(
                path,
                CachedCleanFile {
                    size,
                    last_access: self.next_cache_access(),
                },
            );
        } else {
            cache.remove(&path);
        }
    }

    fn touch_clean_cached(&self, path: &std::path::Path) {
        let access = self.next_cache_access();
        if let Some(entry) = self
            .cache
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .get_mut(&normalize_vfs_path(path))
        {
            entry.last_access = access;
        }
    }

    fn untrack_clean_cached(&self, path: &std::path::Path) {
        let path = normalize_vfs_path(path);
        self.cache
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .retain(|cached_path, _| *cached_path != path && !cached_path.starts_with(&path));
    }

    #[cfg(test)]
    fn clean_cache_snapshot(&self) -> (u64, usize) {
        let cache = self.cache.lock().unwrap_or_else(|err| err.into_inner());
        (cache.values().map(|entry| entry.size).sum(), cache.len())
    }

    async fn gc_clean_cache(&self) -> bashkit::Result<()> {
        let policy = self.cache_policy.clone();
        let dirty = self.dirty_paths();
        let deleted = self
            .deleted
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone();

        let evict = {
            let mut cache = self.cache.lock().unwrap_or_else(|err| err.into_inner());
            cache.retain(|path, _| {
                !dirty.iter().any(|dirty_path| path.starts_with(dirty_path))
                    && !deleted
                        .iter()
                        .any(|deleted_path| path == deleted_path || path.starts_with(deleted_path))
            });

            let mut total: u64 = cache.values().map(|entry| entry.size).sum();
            let mut entries = cache.len();
            let mut candidates = cache
                .iter()
                .map(|(path, entry)| (path.clone(), entry.last_access, entry.size))
                .collect::<Vec<_>>();
            candidates.sort_by_key(|(_, last_access, _)| *last_access);

            let mut evict = Vec::new();
            for (path, _, size) in candidates {
                if total <= policy.max_clean_file_bytes && entries <= policy.max_clean_file_count {
                    break;
                }
                total = total.saturating_sub(size);
                entries = entries.saturating_sub(1);
                evict.push(path);
            }

            for path in &evict {
                cache.remove(path);
            }
            evict
        };

        for path in evict {
            if self.state.exists(&path).await.unwrap_or(false) {
                self.state.remove(&path, false).await?;
            }
        }
        Ok(())
    }

    fn is_deleted(&self, path: &std::path::Path) -> bool {
        let path = normalize_vfs_path(path);
        self.deleted
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .iter()
            .any(|deleted| path == *deleted || path.starts_with(deleted))
    }

    fn mark_deleted(&self, path: &std::path::Path) {
        self.deleted
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .insert(normalize_vfs_path(path));
    }

    fn unmark_deleted(&self, path: &std::path::Path) {
        let path = normalize_vfs_path(path);
        self.deleted
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .remove(&path);
    }

    async fn ensure_parent_dirs(&self, path: &std::path::Path) -> bashkit::Result<()> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
            && !self.state.exists(parent).await?
        {
            self.state.mkdir(parent, true).await?;
        }
        Ok(())
    }

    async fn rename_local_directory_tree(
        &self,
        from: &std::path::Path,
        to: &std::path::Path,
    ) -> bashkit::Result<()> {
        if self.state.exists(to).await.unwrap_or(false) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "destination exists",
            )
            .into());
        }
        self.ensure_parent_dirs(to).await?;
        self.state.mkdir(to, true).await?;

        let mut pending = vec![(from.to_path_buf(), to.to_path_buf())];
        while let Some((source_dir, target_dir)) = pending.pop() {
            for entry in self.state.read_dir(&source_dir).await? {
                let source = source_dir.join(&entry.name);
                let target = target_dir.join(&entry.name);
                match entry.metadata.file_type {
                    bashkit::FileType::Directory => {
                        self.state.mkdir(&target, true).await?;
                        pending.push((source, target));
                    }
                    bashkit::FileType::Symlink => {
                        let link_target = self.state.read_link(&source).await?;
                        self.state.symlink(&link_target, &target).await?;
                    }
                    bashkit::FileType::File | bashkit::FileType::Fifo => {
                        let bytes = self.state.read_file(&source).await?;
                        self.ensure_parent_dirs(&target).await?;
                        self.state.write_file(&target, &bytes).await?;
                    }
                }
            }
        }

        self.state.remove(from, true).await?;
        Ok(())
    }

    async fn hydrate_file(&self, path: &std::path::Path) -> bashkit::Result<Option<Vec<u8>>> {
        let key = self.key_for_file(path);
        let object_path = object_path(key);
        let result = match self.store.get(&object_path).await {
            Ok(result) => result,
            Err(err) if matches!(err, object_store::Error::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(object_error(err)),
        };
        let bytes = result.bytes().await.map_err(object_error)?.to_vec();
        if self.cache_policy.should_track(bytes.len() as u64) {
            self.ensure_parent_dirs(path).await?;
            self.state.write_file(path, &bytes).await?;
            self.mark_clean_cached(path, bytes.len() as u64);
            self.gc_clean_cache().await?;
        } else {
            self.untrack_clean_cached(path);
        }
        Ok(Some(bytes))
    }

    async fn remote_file_exists(&self, path: &std::path::Path) -> bashkit::Result<bool> {
        if self.is_deleted(path) {
            return Ok(false);
        }
        match self.store.head(&object_path(self.key_for_file(path))).await {
            Ok(_) => Ok(true),
            Err(err) if matches!(err, object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(object_error(err)),
        }
    }

    async fn remote_dir_exists(&self, path: &std::path::Path) -> bashkit::Result<bool> {
        if self.is_deleted(path) {
            return Ok(false);
        }
        if path == std::path::Path::new("/") {
            return Ok(true);
        }
        if self
            .store
            .head(&object_path(self.key_for_dir_marker(path)))
            .await
            .is_ok()
        {
            return Ok(true);
        }
        let prefix = object_prefix_path(self.key_for_dir_prefix(path));
        let listed = self
            .store
            .list_with_delimiter(prefix.as_ref())
            .await
            .map_err(object_error)?;
        Ok(!listed.objects.is_empty() || !listed.common_prefixes.is_empty())
    }

    fn key_for_file(&self, path: &std::path::Path) -> String {
        let rel = relative_vfs_key(path);
        if rel.is_empty() {
            self.prefix.trim_end_matches('/').to_string()
        } else {
            format!("{}{}", self.prefix, rel)
        }
    }

    fn key_for_dir_prefix(&self, path: &std::path::Path) -> String {
        let rel = relative_vfs_key(path);
        if rel.is_empty() {
            self.prefix.clone()
        } else {
            format!("{}{}/", self.prefix, rel)
        }
    }

    fn key_for_dir_marker(&self, path: &std::path::Path) -> String {
        format!("{}.dir", self.key_for_dir_prefix(path))
    }

    async fn remote_entries(
        &self,
        path: &std::path::Path,
    ) -> bashkit::Result<Vec<bashkit::DirEntry>> {
        let prefix = object_prefix_path(self.key_for_dir_prefix(path));
        let result = self
            .store
            .list_with_delimiter(prefix.as_ref())
            .await
            .map_err(object_error)?;
        let prefix_raw = self.key_for_dir_prefix(path);
        let mut entries = Vec::new();
        let mut seen = std::collections::BTreeSet::new();

        for object in result.objects {
            let key = object.location.to_string();
            let Some(name) = key.strip_prefix(&prefix_raw) else {
                continue;
            };
            if name.is_empty() || name == ".dir" || name.contains('/') {
                continue;
            }
            if self.is_deleted(&path.join(name)) {
                continue;
            }
            if seen.insert(name.to_string()) {
                entries.push(bashkit::DirEntry {
                    name: name.to_string(),
                    metadata: bashkit::Metadata {
                        file_type: bashkit::FileType::File,
                        size: object.size,
                        mode: 0o644,
                        modified: object.last_modified.into(),
                        created: object.last_modified.into(),
                    },
                });
            }
        }

        for common in result.common_prefixes {
            let key = common.to_string();
            let Some(mut name) = key.strip_prefix(&prefix_raw).map(str::to_string) else {
                continue;
            };
            name = name.trim_end_matches('/').to_string();
            if self.is_deleted(&path.join(&name)) {
                continue;
            }
            if !name.is_empty() && !name.contains('/') && seen.insert(name.clone()) {
                entries.push(bashkit::DirEntry {
                    name,
                    metadata: directory_metadata(),
                });
            }
        }

        entries.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(entries)
    }
}

#[async_trait::async_trait]
impl bashkit::FileSystemExt for ManagedObjectStoreFs {
    fn vfs_snapshot(&self) -> Option<bashkit::VfsSnapshot> {
        self.state.vfs_snapshot()
    }
}

#[async_trait::async_trait]
impl bashkit::FileSystem for ManagedObjectStoreFs {
    async fn read_file(&self, path: &std::path::Path) -> bashkit::Result<Vec<u8>> {
        let path = normalize_vfs_path(path);
        if self.state.exists(&path).await.unwrap_or(false) {
            self.touch_clean_cached(&path);
            return self.state.read_file(&path).await;
        }
        if self.is_deleted(&path) {
            return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "file not found").into());
        }
        if let Some(bytes) = self.hydrate_file(&path).await? {
            return Ok(bytes);
        }
        if self.remote_dir_exists(&path).await? {
            return Err(
                std::io::Error::new(std::io::ErrorKind::IsADirectory, "is a directory").into(),
            );
        }
        Err(std::io::Error::new(std::io::ErrorKind::NotFound, "file not found").into())
    }

    async fn write_file(&self, path: &std::path::Path, content: &[u8]) -> bashkit::Result<()> {
        let path = normalize_vfs_path(path);
        self.ensure_parent_dirs(&path).await?;
        self.state.write_file(&path, content).await?;
        self.untrack_clean_cached(&path);
        self.unmark_deleted(&path);
        self.push_dirty(ObjectStoreWriteback::PutFile(path));
        self.gc_clean_cache().await?;
        Ok(())
    }

    async fn append_file(&self, path: &std::path::Path, content: &[u8]) -> bashkit::Result<()> {
        let path = normalize_vfs_path(path);
        let mut existing = if self.exists(&path).await? {
            self.read_file(&path).await?
        } else {
            Vec::new()
        };
        existing.extend_from_slice(content);
        self.write_file(&path, &existing).await?;
        Ok(())
    }

    async fn mkdir(&self, path: &std::path::Path, recursive: bool) -> bashkit::Result<()> {
        let path = normalize_vfs_path(path);
        self.state.mkdir(&path, recursive).await?;
        self.untrack_clean_cached(&path);
        self.unmark_deleted(&path);
        self.push_dirty(ObjectStoreWriteback::PutDir(path));
        Ok(())
    }

    async fn remove(&self, path: &std::path::Path, recursive: bool) -> bashkit::Result<()> {
        let path = normalize_vfs_path(path);
        let meta = self.stat(&path).await?;
        if meta.file_type == bashkit::FileType::Directory
            && !recursive
            && !self.read_dir(&path).await?.is_empty()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::DirectoryNotEmpty,
                "directory not empty",
            )
            .into());
        }
        if self.state.exists(&path).await.unwrap_or(false) {
            self.state.remove(&path, recursive).await?;
        }
        self.untrack_clean_cached(&path);
        self.mark_deleted(&path);
        match meta.file_type {
            bashkit::FileType::Directory => self.push_dirty(ObjectStoreWriteback::DeleteDir(path)),
            _ => self.push_dirty(ObjectStoreWriteback::DeleteFile(path)),
        }
        Ok(())
    }

    async fn stat(&self, path: &std::path::Path) -> bashkit::Result<bashkit::Metadata> {
        let path = normalize_vfs_path(path);
        if self.state.exists(&path).await.unwrap_or(false) {
            return self.state.stat(&path).await;
        }
        if self.is_deleted(&path) {
            return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "not found").into());
        }
        match self
            .store
            .head(&object_path(self.key_for_file(&path)))
            .await
        {
            Ok(meta) => {
                return Ok(bashkit::Metadata {
                    file_type: bashkit::FileType::File,
                    size: meta.size,
                    mode: 0o644,
                    modified: meta.last_modified.into(),
                    created: meta.last_modified.into(),
                });
            }
            Err(err) if matches!(err, object_store::Error::NotFound { .. }) => {}
            Err(err) => return Err(object_error(err)),
        }
        if self.remote_dir_exists(&path).await? {
            return Ok(directory_metadata());
        }
        Err(std::io::Error::new(std::io::ErrorKind::NotFound, "not found").into())
    }

    async fn read_dir(&self, path: &std::path::Path) -> bashkit::Result<Vec<bashkit::DirEntry>> {
        let path = normalize_vfs_path(path);
        let local_exists = self.state.exists(&path).await.unwrap_or(false);
        if self.is_deleted(&path) && !local_exists {
            return Err(
                std::io::Error::new(std::io::ErrorKind::NotFound, "directory not found").into(),
            );
        }
        let mut entries = if local_exists {
            self.state.read_dir(&path).await?
        } else if self.remote_dir_exists(&path).await? {
            Vec::new()
        } else {
            return Err(
                std::io::Error::new(std::io::ErrorKind::NotFound, "directory not found").into(),
            );
        };
        let mut seen = entries
            .iter()
            .map(|entry| entry.name.clone())
            .collect::<std::collections::BTreeSet<_>>();
        for entry in self.remote_entries(&path).await? {
            if seen.insert(entry.name.clone()) {
                entries.push(entry);
            }
        }
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(entries)
    }

    async fn exists(&self, path: &std::path::Path) -> bashkit::Result<bool> {
        let path = normalize_vfs_path(path);
        if self.state.exists(&path).await.unwrap_or(false) {
            return Ok(true);
        }
        if self.is_deleted(&path) {
            return Ok(false);
        }
        Ok(self.remote_file_exists(&path).await? || self.remote_dir_exists(&path).await?)
    }

    async fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> bashkit::Result<()> {
        let from = normalize_vfs_path(from);
        let to = normalize_vfs_path(to);
        let meta = self.stat(&from).await?;
        match meta.file_type {
            bashkit::FileType::Directory => {
                if self.remote_dir_exists(&from).await? {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        "remote directory rename not supported",
                    )
                    .into());
                }
                if !self.state.exists(&from).await.unwrap_or(false) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        "remote directory rename not supported",
                    )
                    .into());
                }
                self.ensure_parent_dirs(&to).await?;
                self.rename_local_directory_tree(&from, &to).await?;
                self.rewrite_pending_puts_for_rename(&from, &to);
                self.untrack_clean_cached(&from);
                self.untrack_clean_cached(&to);
                self.push_dirty(ObjectStoreWriteback::DeleteDir(from));
                self.push_dirty(ObjectStoreWriteback::PutDir(to));
            }
            _ => {
                let bytes = self.read_file(&from).await?;
                self.write_file(&to, &bytes).await?;
                self.remove(&from, false).await?;
            }
        }
        self.gc_clean_cache().await?;
        Ok(())
    }

    async fn copy(&self, from: &std::path::Path, to: &std::path::Path) -> bashkit::Result<()> {
        let from = normalize_vfs_path(from);
        let to = normalize_vfs_path(to);
        let meta = self.stat(&from).await?;
        if meta.file_type == bashkit::FileType::Directory {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "directory copy not supported",
            )
            .into());
        }
        let bytes = self.read_file(&from).await?;
        self.write_file(&to, &bytes).await?;
        Ok(())
    }

    async fn symlink(
        &self,
        _target: &std::path::Path,
        _link: &std::path::Path,
    ) -> bashkit::Result<()> {
        Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "symlink not supported").into())
    }

    async fn read_link(&self, _path: &std::path::Path) -> bashkit::Result<std::path::PathBuf> {
        Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "symlink not supported").into())
    }

    async fn chmod(&self, _path: &std::path::Path, _mode: u32) -> bashkit::Result<()> {
        Ok(())
    }

    async fn set_modified_time(
        &self,
        _path: &std::path::Path,
        _time: std::time::SystemTime,
    ) -> bashkit::Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl VerletVfsBackend for ManagedObjectStoreFs {
    async fn flush(&self) -> bashkit::Result<()> {
        let ops = {
            self.dirty
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .clone()
        };
        let mut oversized_clean_files = Vec::new();

        for queued in &ops {
            match &queued.op {
                ObjectStoreWriteback::PutFile(path) => {
                    if self.state.exists(&path).await.unwrap_or(false) {
                        let bytes = self.state.read_file(&path).await?;
                        let size = bytes.len() as u64;
                        self.store
                            .put(&object_path(self.key_for_file(&path)), bytes.into())
                            .await
                            .map_err(object_error)?;
                        self.unmark_deleted(&path);
                        if self.cache_policy.should_track(size) {
                            self.mark_clean_cached(&path, size);
                        } else {
                            self.untrack_clean_cached(&path);
                            oversized_clean_files.push((path.clone(), size));
                        }
                    }
                }
                ObjectStoreWriteback::PutDir(path) => {
                    self.store
                        .put(
                            &object_path(self.key_for_dir_marker(&path)),
                            Vec::new().into(),
                        )
                        .await
                        .map_err(object_error)?;
                    self.unmark_deleted(&path);
                }
                ObjectStoreWriteback::DeleteFile(path) => {
                    ignore_not_found(
                        self.store
                            .delete(&object_path(self.key_for_file(&path)))
                            .await,
                    )?;
                    self.untrack_clean_cached(&path);
                    self.mark_deleted(&path);
                }
                ObjectStoreWriteback::DeleteDir(path) => {
                    let prefix = object_prefix_path(self.key_for_dir_prefix(&path));
                    if let Some(prefix) = prefix {
                        let keys = self
                            .store
                            .list(Some(&prefix))
                            .map_ok(|meta| meta.location)
                            .try_collect::<Vec<_>>()
                            .await
                            .map_err(object_error)?;
                        for key in keys {
                            ignore_not_found(self.store.delete(&key).await)?;
                        }
                    }
                    ignore_not_found(
                        self.store
                            .delete(&object_path(self.key_for_dir_marker(&path)))
                            .await,
                    )?;
                    self.untrack_clean_cached(&path);
                    self.mark_deleted(&path);
                }
            }
        }

        let processed = ops
            .iter()
            .map(|queued| queued.id)
            .collect::<std::collections::BTreeSet<_>>();
        self.dirty
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .retain(|queued| !processed.contains(&queued.id));

        let dirty = self.dirty_paths();
        for (path, _) in oversized_clean_files {
            if !dirty.iter().any(|dirty_path| path.starts_with(dirty_path))
                && self.state.exists(&path).await.unwrap_or(false)
            {
                self.state.remove(&path, false).await?;
            }
        }
        self.gc_clean_cache().await?;
        Ok(())
    }
}

fn normalize_vfs_path(path: &std::path::Path) -> std::path::PathBuf {
    if path.has_root() {
        bashkit::normalize_path(path)
    } else {
        bashkit::normalize_path(&std::path::PathBuf::from("/").join(path))
    }
}

fn rebase_path(
    path: &std::path::Path,
    from: &std::path::Path,
    to: &std::path::Path,
) -> std::path::PathBuf {
    let path = normalize_vfs_path(path);
    let from = normalize_vfs_path(from);
    let to = normalize_vfs_path(to);
    if path == from {
        return to;
    }
    match path.strip_prefix(&from) {
        Ok(rest) => normalize_vfs_path(&to.join(rest)),
        Err(_) => path,
    }
}

fn normalize_object_prefix(mut prefix: String) -> String {
    prefix = prefix.trim_start_matches('/').to_string();
    if !prefix.is_empty() && !prefix.ends_with('/') {
        prefix.push('/');
    }
    prefix
}

fn relative_vfs_key(path: &std::path::Path) -> String {
    normalize_vfs_path(path)
        .to_string_lossy()
        .trim_start_matches('/')
        .to_string()
}

fn object_path(key: String) -> object_store::path::Path {
    if key.is_empty() {
        object_store::path::Path::default()
    } else {
        object_store::path::Path::from(key)
    }
}

fn object_prefix_path(key: String) -> Option<object_store::path::Path> {
    let key = key.trim_end_matches('/').to_string();
    if key.is_empty() {
        None
    } else {
        Some(object_store::path::Path::from(key))
    }
}

fn directory_metadata() -> bashkit::Metadata {
    bashkit::Metadata {
        file_type: bashkit::FileType::Directory,
        size: 0,
        mode: 0o755,
        modified: std::time::SystemTime::now(),
        created: std::time::SystemTime::now(),
    }
}

fn readonly_error() -> bashkit::Error {
    std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "filesystem is mounted read-only",
    )
    .into()
}

fn object_error(err: object_store::Error) -> bashkit::Error {
    let kind = match err {
        object_store::Error::NotFound { .. } => std::io::ErrorKind::NotFound,
        _ => std::io::ErrorKind::Other,
    };
    std::io::Error::new(kind, err.to_string()).into()
}

fn ignore_not_found(result: object_store::Result<()>) -> bashkit::Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(err) if matches!(err, object_store::Error::NotFound { .. }) => Ok(()),
        Err(err) => Err(object_error(err)),
    }
}

#[cfg(test)]
mod tests;
