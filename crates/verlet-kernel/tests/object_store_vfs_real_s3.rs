use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::time::{Duration, timeout};
use uuid::Uuid;
use verlet::{
    BashkitExecutionHarness, ObjectStoreMountConfig, S3ObjectStoreConfig, TenantRegistration,
    TenantRuntimeContext, ThreadEvent, ThreadStartRequest, ThreadTopology, VerletSupervisor,
    VirtualBashRuntimeConfig, VirtualBashRuntimeFactory, VirtualMount,
};

#[tokio::test]
#[ignore = "requires VERLET_S3_* credentials and mutates a unique object-store prefix"]
async fn virtual_bash_mount_round_trips_real_s3_or_r2() {
    let live = live_s3_config("verlet-smoke");

    let config = VirtualBashRuntimeConfig {
        cwd: PathBuf::from("/s3"),
        mounts: vec![VirtualMount::object_store(
            "/s3",
            ObjectStoreMountConfig::s3(live.config.clone(), &live.prefix),
        )],
        ..VirtualBashRuntimeConfig::default()
    };
    let mut writer = BashkitExecutionHarness::new(config).await.unwrap();
    let output = writer
        .execute(
            "mkdir -p roundtrip && echo alpha > roundtrip/a.txt && echo beta >> roundtrip/a.txt",
        )
        .await
        .unwrap();
    assert!(output.success(), "{output:?}");

    let reload = VirtualBashRuntimeConfig {
        cwd: PathBuf::from("/s3"),
        mounts: vec![VirtualMount::object_store(
            "/s3",
            ObjectStoreMountConfig::s3(live.config, &live.prefix),
        )],
        ..VirtualBashRuntimeConfig::default()
    };
    let mut reader = BashkitExecutionHarness::new(reload).await.unwrap();
    let output = reader
        .execute("cat roundtrip/a.txt && rm -r roundtrip && test ! -e roundtrip/a.txt")
        .await
        .unwrap();
    assert!(output.success(), "{output:?}");
    assert_eq!(output.stdout, "alpha\nbeta\n");
}

#[tokio::test]
#[ignore = "requires VERLET_S3_* credentials and mutates a unique object-store prefix"]
async fn verlet_agent_thread_creates_file_on_real_s3_or_r2() {
    let live = live_s3_config("verlet-agent-smoke");
    let object_key = format!("{}/agent-created.txt", live.prefix.trim_end_matches('/'));
    let verifier = live.build_object_store();

    let config = VirtualBashRuntimeConfig {
        cwd: PathBuf::from("/r2"),
        mounts: vec![VirtualMount::object_store(
            "/r2",
            ObjectStoreMountConfig::s3(live.config, &live.prefix),
        )],
        ..VirtualBashRuntimeConfig::default()
    };
    let supervisor = VerletSupervisor::new();
    supervisor
        .register_tenant(TenantRegistration {
            context: TenantRuntimeContext::local(
                "tenant-r2-agent",
                "/tmp/verlet-r2-agent-runtime",
                "/tmp/verlet-r2-agent-state",
            ),
            runtime_factory: Arc::new(VirtualBashRuntimeFactory::new(config)),
        })
        .await
        .unwrap();

    let thread = supervisor
        .start_thread(ThreadStartRequest {
            tenant_id: "tenant-r2-agent".to_string(),
            user_id: "user-r2".to_string(),
            session_id: "session-r2".to_string(),
            topology: ThreadTopology::root(),
            metadata: Default::default(),
        })
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    supervisor
        .submit_to(
            &thread.context().coordinates,
            "turn-r2-create",
            "printf 'created by verlet agent\\n' > /r2/agent-created.txt && cat /r2/agent-created.txt",
        )
        .await
        .unwrap();

    let output = next_output(&mut events).await;
    assert_eq!(output, "created by verlet agent\n");

    let bytes = verifier
        .get(&ObjectPath::from(object_key.clone()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(bytes.as_ref(), b"created by verlet agent\n");

    verifier
        .delete(&ObjectPath::from(object_key))
        .await
        .unwrap();
}

#[derive(Clone)]
struct LiveS3Config {
    config: S3ObjectStoreConfig,
    prefix: String,
}

impl LiveS3Config {
    fn build_object_store(&self) -> impl ObjectStore {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&self.config.bucket)
            .with_region(&self.config.region)
            .with_virtual_hosted_style_request(self.config.virtual_hosted_style_request)
            .with_allow_http(self.config.allow_http);

        if let Some(endpoint) = &self.config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(access_key_id) = &self.config.access_key_id {
            builder = builder.with_access_key_id(access_key_id);
        }
        if let Some(secret_access_key) = &self.config.secret_access_key {
            builder = builder.with_secret_access_key(secret_access_key);
        }
        if let Some(token) = &self.config.session_token {
            builder = builder.with_token(token);
        }

        builder.build().unwrap()
    }
}

fn live_s3_config(default_prefix: &str) -> LiveS3Config {
    let bucket = verlet_runtime_contracts::env_compat::var("VERLET_S3_BUCKET")
        .expect("VERLET_S3_BUCKET is required");
    let region = verlet_runtime_contracts::env_compat::var("VERLET_S3_REGION")
        .unwrap_or_else(|_| "auto".to_string());
    let access_key_id = verlet_runtime_contracts::env_compat::var("VERLET_S3_ACCESS_KEY_ID")
        .expect("VERLET_S3_ACCESS_KEY_ID is required");
    let secret_access_key =
        verlet_runtime_contracts::env_compat::var("VERLET_S3_SECRET_ACCESS_KEY")
            .expect("VERLET_S3_SECRET_ACCESS_KEY is required");
    let prefix = verlet_runtime_contracts::env_compat::var("VERLET_S3_PREFIX")
        .unwrap_or_else(|_| format!("{default_prefix}/{}", Uuid::now_v7()));

    let mut config =
        S3ObjectStoreConfig::new(bucket, region).with_credentials(access_key_id, secret_access_key);
    if let Ok(endpoint) = verlet_runtime_contracts::env_compat::var("VERLET_S3_ENDPOINT") {
        config = config.with_endpoint(endpoint);
    }
    if let Ok(token) = verlet_runtime_contracts::env_compat::var("VERLET_S3_SESSION_TOKEN") {
        config = config.with_session_token(token);
    }
    if verlet_runtime_contracts::env_compat::var("VERLET_S3_ALLOW_HTTP")
        .is_ok_and(|value| value == "1")
    {
        config = config.with_allow_http(true);
    }

    LiveS3Config { config, prefix }
}

async fn next_output(events: &mut broadcast::Receiver<ThreadEvent>) -> String {
    timeout(Duration::from_secs(30), async {
        loop {
            match events.recv().await.unwrap() {
                ThreadEvent::Output { text, .. } => break text,
                ThreadEvent::Failed { message, .. } => panic!("thread failed: {message}"),
                _ => {}
            }
        }
    })
    .await
    .unwrap()
}
