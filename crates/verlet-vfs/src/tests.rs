use crate::VerletVfsBackend as _;
use bashkit::FileSystem as _;
use object_store::ObjectStoreExt as _;

fn unique_host_fs_root(label: &str) -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let root = std::env::temp_dir().join(format!(
        "verlet-vfs-{label}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    root
}

#[cfg(unix)]
#[tokio::test]
async fn host_filesystem_rw_is_rooted_and_rejects_symlink_escape() {
    let fixture = unique_host_fs_root("rooting");
    let root = fixture.join("root");
    let outside = fixture.join("outside");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("secret.txt"), "outside-safe").unwrap();
    std::os::unix::fs::symlink(outside.join("secret.txt"), root.join("escape-link")).unwrap();
    std::os::unix::fs::symlink(outside.clone(), root.join("escape-dir")).unwrap();
    std::os::unix::fs::symlink("escape-dir/secret.txt", root.join("escape-chain")).unwrap();

    let fs = crate::HostFileSystem::read_write(&root).unwrap();
    fs.write_file(std::path::Path::new("/round-trip.txt"), b"host mutation")
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(root.join("round-trip.txt")).unwrap(),
        "host mutation"
    );

    assert!(
        fs.read_file(std::path::Path::new("/escape-link"))
            .await
            .is_err()
    );
    assert!(
        fs.read_file(std::path::Path::new("/escape-chain"))
            .await
            .is_err()
    );
    assert!(
        fs.write_file(std::path::Path::new("/escape-link"), b"escaped")
            .await
            .is_err()
    );
    assert!(
        fs.symlink(
            std::path::Path::new("../../outside/secret.txt"),
            std::path::Path::new("/agent-link")
        )
        .await
        .is_err(),
        "an agent-created relative symlink must not escape the host root"
    );
    assert!(
        fs.symlink(
            &outside.join("secret.txt"),
            std::path::Path::new("/agent-absolute-link")
        )
        .await
        .is_err(),
        "an agent-created absolute symlink must not escape the host root"
    );
    fs.write_file(
        std::path::Path::new("/../../parent-escape.txt"),
        b"contained",
    )
    .await
    .unwrap();
    fs.write_file(std::path::Path::new("/absolute-path.txt"), b"contained")
        .await
        .unwrap();

    assert_eq!(
        std::fs::read_to_string(outside.join("secret.txt")).unwrap(),
        "outside-safe"
    );
    assert!(!fixture.join("parent-escape.txt").exists());
    assert_eq!(
        std::fs::read_to_string(root.join("parent-escape.txt")).unwrap(),
        "contained",
        "parent components are normalized inside the host root"
    );
    assert_eq!(
        std::fs::read_to_string(root.join("absolute-path.txt")).unwrap(),
        "contained",
        "absolute virtual paths are rooted under the host directory"
    );
    let _ = std::fs::remove_dir_all(fixture);
}

#[cfg(unix)]
#[tokio::test]
async fn host_filesystem_reports_a_unix_socket_as_a_non_file_kind() {
    let fixture = unique_host_fs_root("socket-kind");
    let socket_path = fixture.join("live.sock");
    let _listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let fs = crate::HostFileSystem::read_only(&fixture).unwrap();

    let metadata = fs.stat(std::path::Path::new("/live.sock")).await.unwrap();

    assert_eq!(metadata.file_type, bashkit::FileType::Fifo);
    let _ = std::fs::remove_dir_all(fixture);
}

#[cfg(unix)]
#[tokio::test]
async fn host_filesystem_rw_rejects_mutating_a_preexisting_external_hard_link() {
    let fixture = unique_host_fs_root("hard-link");
    let root = fixture.join("root");
    let outside = fixture.join("outside");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    let outside_file = outside.join("shared.txt");
    std::fs::write(&outside_file, "outside-safe").unwrap();
    std::fs::hard_link(&outside_file, root.join("shared.txt")).unwrap();

    let fs = crate::HostFileSystem::read_write(&root).unwrap();

    assert!(
        fs.write_file(std::path::Path::new("/shared.txt"), b"escaped")
            .await
            .is_err()
    );
    assert!(
        fs.append_file(std::path::Path::new("/shared.txt"), b"-escaped")
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read_to_string(outside_file).unwrap(),
        "outside-safe"
    );
    let _ = std::fs::remove_dir_all(fixture);
}

#[cfg(unix)]
#[test]
fn host_filesystem_mounts_of_the_same_directory_share_an_operation_lock() {
    let fixture = unique_host_fs_root("shared-lock");
    let root = fixture.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let first = crate::HostFileSystem::read_write(&root).unwrap();
    let second = crate::HostFileSystem::read_write(root.join(".")).unwrap();

    assert!(std::sync::Arc::ptr_eq(
        &first.operation_lock,
        &second.operation_lock
    ));
    let _ = std::fs::remove_dir_all(fixture);
}

#[derive(Debug)]
struct FailingFirstPutStore {
    inner: std::sync::Arc<dyn object_store::ObjectStore>,
    fail_next_put: std::sync::atomic::AtomicBool,
}

impl FailingFirstPutStore {
    fn new(inner: std::sync::Arc<dyn object_store::ObjectStore>) -> Self {
        Self {
            inner,
            fail_next_put: std::sync::atomic::AtomicBool::new(true),
        }
    }
}

impl std::fmt::Display for FailingFirstPutStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FailingFirstPutStore")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for FailingFirstPutStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if self
            .fail_next_put
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(object_store::Error::Generic {
                store: "failing-first-put",
                source: Box::new(std::io::Error::other("injected put failure")),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[test]
fn r2_config_maps_to_s3_compatible_endpoint_shape() {
    let s3 = crate::R2ObjectStoreConfig::new("verlet-files")
        .with_account_id("account123")
        .with_credentials("key", "secret")
        .into_s3_config();

    assert_eq!(s3.bucket, "verlet-files");
    assert_eq!(
        s3.endpoint.as_deref(),
        Some("https://account123.r2.cloudflarestorage.com")
    );
    assert_eq!(s3.region, "auto");
    assert!(!s3.virtual_hosted_style_request);
    assert_eq!(s3.access_key_id.as_deref(), Some("key"));
    assert_eq!(s3.secret_access_key.as_deref(), Some("secret"));
}

#[test]
fn object_store_mount_prefixes_are_normalized_for_virtual_paths() {
    let config = crate::ObjectStoreMountConfig::in_memory("/tenant/session");
    assert_eq!(config.prefix, "tenant/session/");

    let fs = crate::ManagedObjectStoreFs::new(config).unwrap();
    assert_eq!(
        fs.key_for_file(std::path::Path::new("/dir/file.txt")),
        "tenant/session/dir/file.txt"
    );
    assert_eq!(
        fs.key_for_dir_prefix(std::path::Path::new("/dir")),
        "tenant/session/dir/"
    );
    assert_eq!(
        crate::relative_vfs_key(std::path::Path::new("../escaped.txt")),
        "escaped.txt"
    );
}

#[tokio::test]
async fn clean_hydrated_files_are_evicted_and_rehydrated_from_object_store() {
    let store = std::sync::Arc::new(object_store::memory::InMemory::new())
        as std::sync::Arc<dyn object_store::ObjectStore>;
    store
        .put(
            &object_store::path::Path::from("cache/a.txt"),
            Vec::from("alpha").into(),
        )
        .await
        .unwrap();
    store
        .put(
            &object_store::path::Path::from("cache/b.txt"),
            Vec::from("beta").into(),
        )
        .await
        .unwrap();
    let fs = crate::ManagedObjectStoreFs::new(
        crate::ObjectStoreMountConfig::shared(store, "cache")
            .with_cache_policy(crate::ObjectStoreCachePolicy::bounded(5, 1, 1024)),
    )
    .unwrap();

    assert_eq!(
        fs.read_file(std::path::Path::new("/a.txt")).await.unwrap(),
        b"alpha"
    );
    assert_eq!(fs.clean_cache_snapshot(), (5, 1));
    assert!(
        fs.state
            .exists(std::path::Path::new("/a.txt"))
            .await
            .unwrap()
    );

    assert_eq!(
        fs.read_file(std::path::Path::new("/b.txt")).await.unwrap(),
        b"beta"
    );
    assert_eq!(fs.clean_cache_snapshot(), (4, 1));
    assert!(
        !fs.state
            .exists(std::path::Path::new("/a.txt"))
            .await
            .unwrap()
    );
    assert!(
        fs.state
            .exists(std::path::Path::new("/b.txt"))
            .await
            .unwrap()
    );

    assert_eq!(
        fs.read_file(std::path::Path::new("/a.txt")).await.unwrap(),
        b"alpha"
    );
    assert_eq!(fs.clean_cache_snapshot(), (5, 1));
    assert!(
        fs.state
            .exists(std::path::Path::new("/a.txt"))
            .await
            .unwrap()
    );
    assert!(
        !fs.state
            .exists(std::path::Path::new("/b.txt"))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn dirty_files_are_not_evicted_before_flush() {
    let store = std::sync::Arc::new(object_store::memory::InMemory::new())
        as std::sync::Arc<dyn object_store::ObjectStore>;
    let fs = crate::ManagedObjectStoreFs::new(
        crate::ObjectStoreMountConfig::shared(store.clone(), "dirty")
            .with_cache_policy(crate::ObjectStoreCachePolicy::bounded(1, 1, 1)),
    )
    .unwrap();

    fs.write_file(std::path::Path::new("/large.txt"), b"larger than cache")
        .await
        .unwrap();
    fs.gc_clean_cache().await.unwrap();
    assert!(
        fs.state
            .exists(std::path::Path::new("/large.txt"))
            .await
            .unwrap()
    );
    assert_eq!(
        fs.read_file(std::path::Path::new("/large.txt"))
            .await
            .unwrap(),
        b"larger than cache"
    );

    fs.flush().await.unwrap();
    assert!(
        !fs.state
            .exists(std::path::Path::new("/large.txt"))
            .await
            .unwrap()
    );
    let stored = store
        .get(&object_store::path::Path::from("dirty/large.txt"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(stored.as_ref(), b"larger than cache");
}

#[tokio::test]
async fn failed_flush_keeps_dirty_file_for_retry() {
    let inner = std::sync::Arc::new(object_store::memory::InMemory::new())
        as std::sync::Arc<dyn object_store::ObjectStore>;
    let failing = std::sync::Arc::new(FailingFirstPutStore::new(inner.clone()))
        as std::sync::Arc<dyn object_store::ObjectStore>;
    let fs = crate::ManagedObjectStoreFs::new(
        crate::ObjectStoreMountConfig::shared(failing, "retry")
            .with_cache_policy(crate::ObjectStoreCachePolicy::bounded(1, 1, 1)),
    )
    .unwrap();

    fs.write_file(std::path::Path::new("/large.txt"), b"larger than cache")
        .await
        .unwrap();
    let err = fs.flush().await.unwrap_err();
    assert!(err.to_string().contains("injected put failure"));
    assert!(
        fs.state
            .exists(std::path::Path::new("/large.txt"))
            .await
            .unwrap()
    );
    assert_eq!(
        fs.read_file(std::path::Path::new("/large.txt"))
            .await
            .unwrap(),
        b"larger than cache"
    );
    assert!(
        inner
            .get(&object_store::path::Path::from("retry/large.txt"))
            .await
            .is_err()
    );

    fs.flush().await.unwrap();
    assert!(
        !fs.state
            .exists(std::path::Path::new("/large.txt"))
            .await
            .unwrap()
    );
    let stored = inner
        .get(&object_store::path::Path::from("retry/large.txt"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(stored.as_ref(), b"larger than cache");
}

#[tokio::test]
async fn local_dirty_directory_rename_rebases_pending_object_store_writes() {
    let store = std::sync::Arc::new(object_store::memory::InMemory::new())
        as std::sync::Arc<dyn object_store::ObjectStore>;
    let fs = crate::ManagedObjectStoreFs::new(crate::ObjectStoreMountConfig::shared(
        store.clone(),
        "rename",
    ))
    .unwrap();

    fs.mkdir(std::path::Path::new("/old"), true).await.unwrap();
    fs.write_file(std::path::Path::new("/old/a.txt"), b"alpha")
        .await
        .unwrap();
    fs.rename(std::path::Path::new("/old"), std::path::Path::new("/new"))
        .await
        .unwrap();
    fs.flush().await.unwrap();

    let stored = store
        .get(&object_store::path::Path::from("rename/new/a.txt"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(stored.as_ref(), b"alpha");
    assert!(
        store
            .get(&object_store::path::Path::from("rename/old/a.txt"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn hydrated_remote_directory_rename_is_explicitly_unsupported() {
    let store = std::sync::Arc::new(object_store::memory::InMemory::new())
        as std::sync::Arc<dyn object_store::ObjectStore>;
    store
        .put(
            &object_store::path::Path::from("remote/old/a.txt"),
            Vec::from("alpha").into(),
        )
        .await
        .unwrap();
    let fs = crate::ManagedObjectStoreFs::new(crate::ObjectStoreMountConfig::shared(
        store.clone(),
        "remote",
    ))
    .unwrap();

    assert_eq!(
        fs.read_file(std::path::Path::new("/old/a.txt"))
            .await
            .unwrap(),
        b"alpha"
    );
    let err = fs
        .rename(std::path::Path::new("/old"), std::path::Path::new("/new"))
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("remote directory rename not supported")
    );

    let stored = store
        .get(&object_store::path::Path::from("remote/old/a.txt"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(stored.as_ref(), b"alpha");
    assert!(
        store
            .get(&object_store::path::Path::from("remote/new/a.txt"))
            .await
            .is_err()
    );
}
