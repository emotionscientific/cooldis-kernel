use super::*;
use futures_util::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::fmt::{Display, Result as FmtResult};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

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
