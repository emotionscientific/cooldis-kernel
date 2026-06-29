use super::*;
use cooldis_fs::DirObjectStore;
use cooldis_fs::workspace::{self, Change};
use futures_util::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::fmt::{Display, Result as FmtResult};
use std::fs;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};

static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(label: &str) -> Self {
        let id = NEXT_TEMP_DIR.fetch_add(1, AtomicOrdering::SeqCst);
        let path =
            std::env::temp_dir().join(format!("cooldis-vfs-{label}-{}-{id}", std::process::id()));
        remove_dir_if_exists(&path)
            .unwrap_or_else(|err| panic!("failed to remove {}: {err}", path.display()));
        fs::create_dir_all(&path)
            .unwrap_or_else(|err| panic!("failed to create {}: {err}", path.display()));
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = remove_dir_if_exists(&self.path);
    }
}

fn remove_dir_if_exists(path: &Path) -> std::io::Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

#[derive(Debug)]
struct FailingFirstPutStore {
    inner: Arc<dyn ObjectStore>,
    fail_next_put: AtomicBool,
}

impl FailingFirstPutStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            fail_next_put: AtomicBool::new(true),
        }
    }
}

impl Display for FailingFirstPutStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str("FailingFirstPutStore")
    }
}

#[async_trait]
impl ObjectStore for FailingFirstPutStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if self.fail_next_put.swap(false, AtomicOrdering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "failing-first-put",
                source: Box::new(IoError::other("injected put failure")),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[test]
fn r2_config_maps_to_s3_compatible_endpoint_shape() {
    let s3 = R2ObjectStoreConfig::new("cooldis-files")
        .with_account_id("account123")
        .with_credentials("key", "secret")
        .into_s3_config();

    assert_eq!(s3.bucket, "cooldis-files");
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
    let config = ObjectStoreMountConfig::in_memory("/tenant/session");
    assert_eq!(config.prefix, "tenant/session/");

    let fs = ManagedObjectStoreFs::new(config).unwrap();
    assert_eq!(
        fs.key_for_file(Path::new("/dir/file.txt")),
        "tenant/session/dir/file.txt"
    );
    assert_eq!(
        fs.key_for_dir_prefix(Path::new("/dir")),
        "tenant/session/dir/"
    );
    assert_eq!(relative_vfs_key(Path::new("../escaped.txt")), "escaped.txt");
}

/// Verifies the v1 "mountable" acceptance bar from cooldis-fs
/// `docs/design.md`: VFS writes through a mounted workspace directory are
/// exactly what cooldis-fs commits, diffs, checks out, and re-commits.
#[tokio::test]
async fn cooldis_fs_workspace_dir_mount_commits_and_restores_vfs_writes() {
    let workspace_dir = TestDir::new("workspace");
    let store_dir = TestDir::new("store");
    let checkout_dir = TestDir::new("checkout");
    let store = DirObjectStore::open(store_dir.path()).unwrap();

    let vfs = CooldisVfs::new(Arc::new(InMemoryFs::new()));
    let host_workspace = Arc::new(HostFileSystem::read_write(workspace_dir.path()).unwrap());
    vfs.mount("/workspace", host_workspace).unwrap();

    vfs.mkdir(Path::new("/workspace/nested"), true)
        .await
        .unwrap();
    vfs.write_file(Path::new("/workspace/root.txt"), b"alpha\n")
        .await
        .unwrap();
    vfs.write_file(Path::new("/workspace/nested/story.txt"), b"chapter one\n")
        .await
        .unwrap();
    vfs.flush().await.unwrap();

    let record1 = workspace::commit(&store, workspace_dir.path(), None, "episode-1").unwrap();

    vfs.write_file(Path::new("/workspace/root.txt"), b"alpha edited\n")
        .await
        .unwrap();
    vfs.write_file(Path::new("/workspace/extra.txt"), b"new file\n")
        .await
        .unwrap();
    vfs.remove(Path::new("/workspace/nested/story.txt"), false)
        .await
        .unwrap();
    vfs.flush().await.unwrap();

    let record2 = workspace::commit(
        &store,
        workspace_dir.path(),
        Some(record1.commit),
        "episode-2",
    )
    .unwrap();

    let mut diff = workspace::diff(&store, record1.root, record2.root)
        .unwrap()
        .into_iter()
        .map(|entry| (entry.path.to_string_lossy().into_owned(), entry.change))
        .collect::<Vec<_>>();
    diff.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        diff,
        vec![
            ("extra.txt".to_owned(), Change::Added),
            ("nested/story.txt".to_owned(), Change::Removed),
            ("root.txt".to_owned(), Change::Modified),
        ]
    );

    workspace::checkout(&store, record1.root, checkout_dir.path()).unwrap();
    let restored =
        workspace::commit(&store, checkout_dir.path(), None, "episode-1-restored").unwrap();
    assert_eq!(restored.root, record1.root);
    assert_eq!(
        fs::read(checkout_dir.path().join("root.txt")).unwrap(),
        b"alpha\n"
    );
    assert_eq!(
        fs::read(checkout_dir.path().join("nested/story.txt")).unwrap(),
        b"chapter one\n"
    );

    assert_eq!(record2.parent, Some(record1.commit));
    assert_eq!(record1.stats.files, 2);
    assert_eq!(
        record1.stats.total_bytes,
        (b"alpha\n".len() + b"chapter one\n".len()) as u64
    );
    assert_eq!(record2.stats.files, 2);
    assert_eq!(
        record2.stats.total_bytes,
        (b"alpha edited\n".len() + b"new file\n".len()) as u64
    );
}

#[tokio::test]
async fn clean_hydrated_files_are_evicted_and_rehydrated_from_object_store() {
    let store = Arc::new(InMemoryObjectStore::new()) as Arc<dyn ObjectStore>;
    store
        .put(&ObjectPath::from("cache/a.txt"), Vec::from("alpha").into())
        .await
        .unwrap();
    store
        .put(&ObjectPath::from("cache/b.txt"), Vec::from("beta").into())
        .await
        .unwrap();
    let fs = ManagedObjectStoreFs::new(
        ObjectStoreMountConfig::shared(store, "cache")
            .with_cache_policy(ObjectStoreCachePolicy::bounded(5, 1, 1024)),
    )
    .unwrap();

    assert_eq!(fs.read_file(Path::new("/a.txt")).await.unwrap(), b"alpha");
    assert_eq!(fs.clean_cache_snapshot(), (5, 1));
    assert!(fs.state.exists(Path::new("/a.txt")).await.unwrap());

    assert_eq!(fs.read_file(Path::new("/b.txt")).await.unwrap(), b"beta");
    assert_eq!(fs.clean_cache_snapshot(), (4, 1));
    assert!(!fs.state.exists(Path::new("/a.txt")).await.unwrap());
    assert!(fs.state.exists(Path::new("/b.txt")).await.unwrap());

    assert_eq!(fs.read_file(Path::new("/a.txt")).await.unwrap(), b"alpha");
    assert_eq!(fs.clean_cache_snapshot(), (5, 1));
    assert!(fs.state.exists(Path::new("/a.txt")).await.unwrap());
    assert!(!fs.state.exists(Path::new("/b.txt")).await.unwrap());
}

#[tokio::test]
async fn dirty_files_are_not_evicted_before_flush() {
    let store = Arc::new(InMemoryObjectStore::new()) as Arc<dyn ObjectStore>;
    let fs = ManagedObjectStoreFs::new(
        ObjectStoreMountConfig::shared(store.clone(), "dirty")
            .with_cache_policy(ObjectStoreCachePolicy::bounded(1, 1, 1)),
    )
    .unwrap();

    fs.write_file(Path::new("/large.txt"), b"larger than cache")
        .await
        .unwrap();
    fs.gc_clean_cache().await.unwrap();
    assert!(fs.state.exists(Path::new("/large.txt")).await.unwrap());
    assert_eq!(
        fs.read_file(Path::new("/large.txt")).await.unwrap(),
        b"larger than cache"
    );

    fs.flush().await.unwrap();
    assert!(!fs.state.exists(Path::new("/large.txt")).await.unwrap());
    let stored = store
        .get(&ObjectPath::from("dirty/large.txt"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(stored.as_ref(), b"larger than cache");
}

#[tokio::test]
async fn failed_flush_keeps_dirty_file_for_retry() {
    let inner = Arc::new(InMemoryObjectStore::new()) as Arc<dyn ObjectStore>;
    let failing = Arc::new(FailingFirstPutStore::new(inner.clone())) as Arc<dyn ObjectStore>;
    let fs = ManagedObjectStoreFs::new(
        ObjectStoreMountConfig::shared(failing, "retry")
            .with_cache_policy(ObjectStoreCachePolicy::bounded(1, 1, 1)),
    )
    .unwrap();

    fs.write_file(Path::new("/large.txt"), b"larger than cache")
        .await
        .unwrap();
    let err = fs.flush().await.unwrap_err();
    assert!(err.to_string().contains("injected put failure"));
    assert!(fs.state.exists(Path::new("/large.txt")).await.unwrap());
    assert_eq!(
        fs.read_file(Path::new("/large.txt")).await.unwrap(),
        b"larger than cache"
    );
    assert!(
        inner
            .get(&ObjectPath::from("retry/large.txt"))
            .await
            .is_err()
    );

    fs.flush().await.unwrap();
    assert!(!fs.state.exists(Path::new("/large.txt")).await.unwrap());
    let stored = inner
        .get(&ObjectPath::from("retry/large.txt"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(stored.as_ref(), b"larger than cache");
}

#[tokio::test]
async fn local_dirty_directory_rename_rebases_pending_object_store_writes() {
    let store = Arc::new(InMemoryObjectStore::new()) as Arc<dyn ObjectStore>;
    let fs =
        ManagedObjectStoreFs::new(ObjectStoreMountConfig::shared(store.clone(), "rename")).unwrap();

    fs.mkdir(Path::new("/old"), true).await.unwrap();
    fs.write_file(Path::new("/old/a.txt"), b"alpha")
        .await
        .unwrap();
    fs.rename(Path::new("/old"), Path::new("/new"))
        .await
        .unwrap();
    fs.flush().await.unwrap();

    let stored = store
        .get(&ObjectPath::from("rename/new/a.txt"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(stored.as_ref(), b"alpha");
    assert!(
        store
            .get(&ObjectPath::from("rename/old/a.txt"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn hydrated_remote_directory_rename_is_explicitly_unsupported() {
    let store = Arc::new(InMemoryObjectStore::new()) as Arc<dyn ObjectStore>;
    store
        .put(
            &ObjectPath::from("remote/old/a.txt"),
            Vec::from("alpha").into(),
        )
        .await
        .unwrap();
    let fs =
        ManagedObjectStoreFs::new(ObjectStoreMountConfig::shared(store.clone(), "remote")).unwrap();

    assert_eq!(
        fs.read_file(Path::new("/old/a.txt")).await.unwrap(),
        b"alpha"
    );
    let err = fs
        .rename(Path::new("/old"), Path::new("/new"))
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("remote directory rename not supported")
    );

    let stored = store
        .get(&ObjectPath::from("remote/old/a.txt"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(stored.as_ref(), b"alpha");
    assert!(
        store
            .get(&ObjectPath::from("remote/new/a.txt"))
            .await
            .is_err()
    );
}
