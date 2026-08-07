//! Minimal daemon HTTP projection for the store-primary sync protocol.
//!
//! The transport carries the Verlet V1 JSON types without re-encoding event
//! history. It is intentionally request/response only: there is no WebSocket
//! or live correctness lane. Bearer material is accepted only in the
//! `Authorization` header and is never rendered in diagnostics or `Debug`.

use crate::daemon::remote_store::endpoint::SyncIngressQueueAcknowledger as _;
use crate::daemon::remote_store::endpoint::SyncLeaseRenewer as _;
use crate::daemon::remote_store::endpoint::SyncPullSource as _;
use crate::daemon::remote_store::endpoint::SyncPushGate as _;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;

const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_HEADER_COUNT: usize = 128;
const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;
const MAX_IN_FLIGHT_REQUESTS: usize = 128;
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const ACCEPT_RETRY_MIN: std::time::Duration = std::time::Duration::from_millis(10);
const ACCEPT_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(1);
const PUSH_PATH: &str = "/v1/sync/push";
const PULL_PATH: &str = "/v1/sync/pull";
const RENEW_PATH: &str = "/v1/sync/renew";
const INGRESS_ACK_PATH: &str = "/v1/sync/ingress/ack";

enum SyncListener {
    Tcp(tokio::net::TcpListener),
    #[cfg(unix)]
    Unix(UnixListenerState),
}

#[cfg(unix)]
struct UnixListenerState {
    listener: tokio::net::UnixListener,
    path: std::path::PathBuf,
}

#[cfg(unix)]
impl Drop for UnixListenerState {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Bound daemon sync endpoint. Dropping its `serve` future closes the listener
/// and aborts every in-flight request task; SQLite writes remain protected by
/// the endpoint and lease authority's transaction spawn shields.
pub struct DaemonSyncHttpServer {
    listener: SyncListener,
    endpoint: std::sync::Arc<crate::daemon::remote_store::endpoint::SqliteSyncEndpoint>,
}

impl std::fmt::Debug for DaemonSyncHttpServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DaemonSyncHttpServer")
            .finish_non_exhaustive()
    }
}

impl DaemonSyncHttpServer {
    pub async fn bind(
        listen: crate::AppServerListenAddr,
        endpoint: std::sync::Arc<crate::daemon::remote_store::endpoint::SqliteSyncEndpoint>,
    ) -> crate::VerletResult<Self> {
        let listener = match listen {
            crate::AppServerListenAddr::WebSocket(addr) => {
                if !addr.ip().is_loopback() {
                    return Err(crate::VerletError::RuntimeFactory(format!(
                        "daemon sync listen address {addr} is not loopback; terminate authenticated TLS at a local proxy before exposing it"
                    )));
                }
                SyncListener::Tcp(tokio::net::TcpListener::bind(addr).await.map_err(|error| {
                    crate::VerletError::RuntimeFactory(format!(
                        "failed to bind daemon sync endpoint {addr}: {error}"
                    ))
                })?)
            }
            crate::AppServerListenAddr::Unix(path) => {
                #[cfg(unix)]
                {
                    prepare_unix_socket_path(&path)?;
                    let listener = tokio::net::UnixListener::bind(&path).map_err(|error| {
                        crate::VerletError::RuntimeFactory(format!(
                            "failed to bind daemon sync socket {}: {error}",
                            path.display()
                        ))
                    })?;
                    SyncListener::Unix(UnixListenerState { listener, path })
                }
                #[cfg(not(unix))]
                {
                    let _ = path;
                    return Err(crate::VerletError::RuntimeFactory(
                        "unix daemon sync sockets are only supported on Unix platforms".to_string(),
                    ));
                }
            }
        };
        Ok(Self { listener, endpoint })
    }

    pub fn local_addr(&self) -> crate::VerletResult<Option<std::net::SocketAddr>> {
        match &self.listener {
            SyncListener::Tcp(listener) => listener.local_addr().map(Some).map_err(|error| {
                crate::VerletError::RuntimeFactory(format!(
                    "failed to inspect daemon sync endpoint address: {error}"
                ))
            }),
            #[cfg(unix)]
            SyncListener::Unix(_) => Ok(None),
        }
    }

    pub fn display_addr(&self) -> crate::VerletResult<String> {
        match &self.listener {
            SyncListener::Tcp(listener) => listener
                .local_addr()
                .map(|addr| format!("http://{addr}"))
                .map_err(|error| {
                    crate::VerletError::RuntimeFactory(format!(
                        "failed to inspect daemon sync endpoint address: {error}"
                    ))
                }),
            #[cfg(unix)]
            SyncListener::Unix(state) => Ok(format!("unix://{}", state.path.display())),
        }
    }

    pub async fn serve(self) -> crate::VerletResult<()> {
        match self.listener {
            SyncListener::Tcp(listener) => serve_tcp(listener, self.endpoint).await,
            #[cfg(unix)]
            SyncListener::Unix(state) => serve_unix(state, self.endpoint).await,
        }
    }
}

async fn serve_tcp(
    listener: tokio::net::TcpListener,
    endpoint: std::sync::Arc<crate::daemon::remote_store::endpoint::SqliteSyncEndpoint>,
) -> crate::VerletResult<()> {
    let mut requests = tokio::task::JoinSet::new();
    let mut accept_retry = ACCEPT_RETRY_MIN;
    loop {
        tokio::select! {
            accepted = listener.accept(), if requests.len() < MAX_IN_FLIGHT_REQUESTS => {
                match accepted {
                    Ok((stream, _)) => {
                        accept_retry = ACCEPT_RETRY_MIN;
                        spawn_request(&mut requests, stream, std::sync::Arc::clone(&endpoint));
                    }
                    Err(error) => {
                        eprintln!(
                            "verlet daemon sync accept failed; retrying in {} ms: {error}",
                            accept_retry.as_millis(),
                        );
                        tokio::time::sleep(accept_retry).await;
                        accept_retry = next_accept_retry(accept_retry);
                    }
                }
            }
            completed = requests.join_next(), if !requests.is_empty() => {
                report_request_completion(completed);
            }
        }
    }
}

#[cfg(unix)]
async fn serve_unix(
    state: UnixListenerState,
    endpoint: std::sync::Arc<crate::daemon::remote_store::endpoint::SqliteSyncEndpoint>,
) -> crate::VerletResult<()> {
    let mut requests = tokio::task::JoinSet::new();
    let mut accept_retry = ACCEPT_RETRY_MIN;
    loop {
        tokio::select! {
            accepted = state.listener.accept(), if requests.len() < MAX_IN_FLIGHT_REQUESTS => {
                match accepted {
                    Ok((stream, _)) => {
                        accept_retry = ACCEPT_RETRY_MIN;
                        spawn_request(&mut requests, stream, std::sync::Arc::clone(&endpoint));
                    }
                    Err(error) => {
                        eprintln!(
                            "verlet daemon sync unix accept failed; retrying in {} ms: {error}",
                            accept_retry.as_millis(),
                        );
                        tokio::time::sleep(accept_retry).await;
                        accept_retry = next_accept_retry(accept_retry);
                    }
                }
            }
            completed = requests.join_next(), if !requests.is_empty() => {
                report_request_completion(completed);
            }
        }
    }
}

fn spawn_request<S>(
    requests: &mut tokio::task::JoinSet<crate::VerletResult<()>>,
    stream: S,
    endpoint: std::sync::Arc<crate::daemon::remote_store::endpoint::SqliteSyncEndpoint>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    requests.spawn(request_with_timeout(
        REQUEST_TIMEOUT,
        handle_connection(stream, endpoint),
    ));
}

fn next_accept_retry(current: std::time::Duration) -> std::time::Duration {
    current.saturating_mul(2).min(ACCEPT_RETRY_MAX)
}

async fn request_with_timeout<T>(
    deadline: std::time::Duration,
    future: impl std::future::Future<Output = crate::VerletResult<T>>,
) -> crate::VerletResult<T> {
    tokio::time::timeout(deadline, future)
        .await
        .map_err(|_| protocol_error("sync request timed out"))?
}

fn report_request_completion(
    completed: Option<Result<crate::VerletResult<()>, tokio::task::JoinError>>,
) {
    match completed {
        Some(Ok(Err(error))) => eprintln!("verlet daemon sync request failed: {error}"),
        Some(Err(error)) => eprintln!("verlet daemon sync request task failed: {error}"),
        Some(Ok(Ok(()))) | None => {}
    }
}

async fn handle_connection<S>(
    mut stream: S,
    endpoint: std::sync::Arc<crate::daemon::remote_store::endpoint::SqliteSyncEndpoint>,
) -> crate::VerletResult<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let request = match read_request(&mut stream).await {
        Ok(request) => request,
        Err(_) => {
            write_json(
                &mut stream,
                400,
                &serde_json::json!({ "error": "invalid_request" }),
            )
            .await?;
            return Ok(());
        }
    };
    if request.method != "POST" {
        write_json(
            &mut stream,
            405,
            &serde_json::json!({ "error": "method_not_allowed" }),
        )
        .await?;
        return Ok(());
    }
    let bearer = request.bearer_token().unwrap_or_default();
    match request.path.as_str() {
        PUSH_PATH => {
            let push = match serde_json::from_slice::<
                crate::daemon::remote_store::endpoint::SyncPushRequestV1,
            >(&request.body)
            {
                Ok(push) => push,
                Err(_) => {
                    write_json(
                        &mut stream,
                        400,
                        &serde_json::json!({ "error": "invalid_request" }),
                    )
                    .await?;
                    return Ok(());
                }
            };
            match endpoint.push(bearer, push).await {
                Ok(outcome) => {
                    let status = push_status(&outcome);
                    write_json(&mut stream, status, &outcome).await?;
                }
                Err(_) => {
                    write_json(
                        &mut stream,
                        503,
                        &serde_json::json!({ "error": "endpoint_unavailable" }),
                    )
                    .await?;
                }
            }
        }
        PULL_PATH => {
            let pull = match serde_json::from_slice::<
                crate::daemon::remote_store::endpoint::SyncPullRequestV1,
            >(&request.body)
            {
                Ok(pull)
                    if pull.schema
                        == crate::daemon::remote_store::endpoint::SYNC_PULL_SCHEMA_V1 =>
                {
                    pull
                }
                _ => {
                    write_json(
                        &mut stream,
                        400,
                        &serde_json::json!({ "error": "invalid_request" }),
                    )
                    .await?;
                    return Ok(());
                }
            };
            match endpoint
                .pull_after(bearer, &pull.stream_id, pull.cursor)
                .await
            {
                Ok(records) => {
                    write_json(
                        &mut stream,
                        200,
                        &crate::daemon::remote_store::endpoint::SyncPullResponseV1 {
                            schema: crate::daemon::remote_store::endpoint::SYNC_PULL_SCHEMA_V1
                                .to_string(),
                            records,
                        },
                    )
                    .await?;
                }
                Err(crate::VerletError::History(message))
                    if message == "sync pull not authorized" =>
                {
                    write_json(
                        &mut stream,
                        403,
                        &serde_json::json!({ "error": "not_authorized" }),
                    )
                    .await?;
                }
                Err(crate::VerletError::History(_)) => {
                    write_json(
                        &mut stream,
                        409,
                        &serde_json::json!({ "error": "cursor_conflict" }),
                    )
                    .await?;
                }
                Err(_) => {
                    write_json(
                        &mut stream,
                        503,
                        &serde_json::json!({ "error": "endpoint_unavailable" }),
                    )
                    .await?;
                }
            }
        }
        RENEW_PATH => match endpoint.renew_lease(bearer).await {
            Ok(grant) => {
                let status = if grant.is_some() { 200 } else { 403 };
                write_json(
                    &mut stream,
                    status,
                    &crate::daemon::remote_store::endpoint::SyncLeaseRenewalResponseV1 {
                        schema: crate::daemon::remote_store::endpoint::SYNC_LEASE_RENEWAL_SCHEMA_V1
                            .to_string(),
                        grant,
                    },
                )
                .await?;
            }
            Err(_) => {
                write_json(
                    &mut stream,
                    503,
                    &serde_json::json!({ "error": "endpoint_unavailable" }),
                )
                .await?;
            }
        },
        INGRESS_ACK_PATH => {
            let ack = match serde_json::from_slice::<crate::daemon::remote_store::endpoint::SyncIngressQueueAckRequestV1>(&request.body) {
                Ok(ack) if ack.schema == crate::daemon::remote_store::endpoint::SYNC_INGRESS_QUEUE_ACK_SCHEMA_V1 => ack,
                _ => {
                    write_json(&mut stream, 400, &serde_json::json!({ "error": "invalid_request" })).await?;
                    return Ok(());
                }
            };
            match endpoint.acknowledge_ingress(bearer, ack).await {
                Ok(()) => {
                    write_json(
                        &mut stream,
                        200,
                        &serde_json::json!({ "acknowledged": true }),
                    )
                    .await?
                }
                Err(crate::VerletError::History(message))
                    if message == "sync pull not authorized" =>
                {
                    write_json(
                        &mut stream,
                        403,
                        &serde_json::json!({ "error": "not_authorized" }),
                    )
                    .await?;
                }
                Err(_) => {
                    write_json(
                        &mut stream,
                        503,
                        &serde_json::json!({ "error": "endpoint_unavailable" }),
                    )
                    .await?;
                }
            }
        }
        _ => {
            write_json(
                &mut stream,
                404,
                &serde_json::json!({ "error": "not_found" }),
            )
            .await?
        }
    }
    Ok(())
}

fn push_status(outcome: &crate::daemon::remote_store::endpoint::SyncPushOutcome) -> u16 {
    match outcome {
        crate::daemon::remote_store::endpoint::SyncPushOutcome::Accepted { .. } => 200,
        crate::daemon::remote_store::endpoint::SyncPushOutcome::Rejected { rejection } => match &rejection.reason {
            crate::daemon::remote_store::endpoint::SyncPushRejectionReason::CredentialUnknown
            | crate::daemon::remote_store::endpoint::SyncPushRejectionReason::ScopeViolation { .. }
            | crate::daemon::remote_store::endpoint::SyncPushRejectionReason::CredentialLeaseMismatch { .. } => 403,
            crate::daemon::remote_store::endpoint::SyncPushRejectionReason::RequestInvalid { .. } => 400,
            crate::daemon::remote_store::endpoint::SyncPushRejectionReason::LeaseFence { .. }
            | crate::daemon::remote_store::endpoint::SyncPushRejectionReason::SequenceFenceConflict { .. } => 409,
        },
    }
}

struct HttpRequest {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl HttpRequest {
    fn bearer_token(&self) -> Option<&str> {
        self.headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
            .and_then(|(_, value)| value.strip_prefix("Bearer "))
            .filter(|token| !token.is_empty())
    }
}

async fn read_request<S>(stream: &mut S) -> crate::VerletResult<HttpRequest>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut buffered = Vec::new();
    let header_end = loop {
        if let Some(index) = find_bytes(&buffered, b"\r\n\r\n") {
            break index + 4;
        }
        if buffered.len() >= MAX_HEADER_BYTES {
            return Err(protocol_error("sync request headers exceed limit"));
        }
        let mut chunk = [0_u8; 4096];
        let read = stream
            .read(&mut chunk)
            .await
            .map_err(|_| protocol_error("failed to read sync request"))?;
        if read == 0 {
            return Err(protocol_error("sync request ended before headers"));
        }
        buffered.extend_from_slice(&chunk[..read]);
    };
    if header_end > MAX_HEADER_BYTES {
        return Err(protocol_error("sync request headers exceed limit"));
    }
    let head = std::str::from_utf8(&buffered[..header_end - 4])
        .map_err(|_| protocol_error("sync request headers are not UTF-8"))?;
    let mut lines = head.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| protocol_error("sync request line is missing"))?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| protocol_error("sync request method is missing"))?
        .to_string();
    let path = request_parts
        .next()
        .ok_or_else(|| protocol_error("sync request path is missing"))?
        .to_string();
    if request_parts.next() != Some("HTTP/1.1") || request_parts.next().is_some() {
        return Err(protocol_error("sync endpoint requires HTTP/1.1"));
    }
    let mut headers = Vec::new();
    for line in lines {
        if headers.len() >= MAX_HEADER_COUNT {
            return Err(protocol_error("sync request has too many headers"));
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| protocol_error("sync request header is malformed"))?;
        if name.is_empty()
            || name != name.trim()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
        {
            return Err(protocol_error("sync request header name is invalid"));
        }
        headers.push((name.to_string(), value.trim().to_string()));
    }
    if headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("transfer-encoding"))
    {
        return Err(protocol_error("chunked sync requests are not supported"));
    }
    let content_lengths = headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map(|(_, value)| value)
        .collect::<Vec<_>>();
    if content_lengths.len() > 1 {
        return Err(protocol_error(
            "sync request has duplicate content-length headers",
        ));
    }
    if headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("authorization"))
        .count()
        > 1
    {
        return Err(protocol_error(
            "sync request has duplicate authorization headers",
        ));
    }
    let content_length = content_lengths
        .first()
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|_| protocol_error("sync request content-length is invalid"))?
        .unwrap_or(0);
    if content_length > MAX_BODY_BYTES {
        return Err(protocol_error("sync request body exceeds limit"));
    }
    let required = header_end
        .checked_add(content_length)
        .ok_or_else(|| protocol_error("sync request size overflow"))?;
    while buffered.len() < required {
        let mut chunk = [0_u8; 8192];
        let read = stream
            .read(&mut chunk)
            .await
            .map_err(|_| protocol_error("failed to read sync request body"))?;
        if read == 0 {
            return Err(protocol_error("sync request body ended early"));
        }
        buffered.extend_from_slice(&chunk[..read]);
    }
    Ok(HttpRequest {
        method,
        path,
        headers,
        body: buffered[header_end..required].to_vec(),
    })
}

async fn write_json<S, T>(stream: &mut S, status: u16, value: &T) -> crate::VerletResult<()>
where
    S: tokio::io::AsyncWrite + Unpin,
    T: serde::Serialize + ?Sized,
{
    let body =
        serde_json::to_vec(value).map_err(|_| protocol_error("failed to encode sync response"))?;
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        503 => "Service Unavailable",
        _ => "Error",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(head.as_bytes())
        .await
        .map_err(|_| protocol_error("failed to write sync response"))?;
    stream
        .write_all(&body)
        .await
        .map_err(|_| protocol_error("failed to write sync response"))?;
    stream
        .shutdown()
        .await
        .map_err(|_| protocol_error("failed to close sync response"))
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(unix)]
fn prepare_unix_socket_path(path: &std::path::Path) -> crate::VerletResult<()> {
    if let Some(parent) = path.parent() {
        let parent_existed = parent.exists();
        std::fs::create_dir_all(parent).map_err(|error| {
            crate::VerletError::RuntimeFactory(format!(
                "failed to create daemon sync socket directory {}: {error}",
                parent.display()
            ))
        })?;
        if !parent_existed {
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700)).map_err(
                |error| {
                    crate::VerletError::RuntimeFactory(format!(
                        "failed to secure daemon sync socket directory {}: {error}",
                        parent.display()
                    ))
                },
            )?;
        }
    }
    if path.exists() {
        let metadata = std::fs::symlink_metadata(path).map_err(|error| {
            crate::VerletError::RuntimeFactory(format!(
                "failed to inspect existing daemon sync socket {}: {error}",
                path.display()
            ))
        })?;
        if metadata.file_type().is_file() || metadata.file_type().is_dir() {
            return Err(crate::VerletError::RuntimeFactory(format!(
                "refusing to replace non-socket daemon sync path {}",
                path.display()
            )));
        }
        std::fs::remove_file(path).map_err(|error| {
            crate::VerletError::RuntimeFactory(format!(
                "failed to remove stale daemon sync socket {}: {error}",
                path.display()
            ))
        })?;
    }
    Ok(())
}

fn protocol_error(message: impl Into<String>) -> crate::VerletError {
    crate::VerletError::RuntimeExecution(message.into())
}

/// Child-side projection of the daemon sync endpoint. TCP/TLS origins use
/// reqwest; Unix listeners use the same bounded HTTP/1.1 wire over a local
/// socket so placement capability is identical for every served listener.
#[derive(Clone)]
pub struct HttpSyncClient {
    transport: SyncClientTransport,
}

#[derive(Clone)]
enum SyncClientTransport {
    Network {
        client: reqwest::Client,
        base_url: String,
    },
    #[cfg(unix)]
    Unix { path: std::path::PathBuf },
}

impl std::fmt::Debug for HttpSyncClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSyncClient")
            .field("endpoint", &self.endpoint_label())
            .finish_non_exhaustive()
    }
}

impl HttpSyncClient {
    pub fn new(base_url: impl Into<String>) -> crate::VerletResult<Self> {
        let base_url = base_url.into();
        #[cfg(unix)]
        if let Some(path) = base_url.strip_prefix("unix://") {
            if path.is_empty() {
                return Err(crate::VerletError::RuntimeFactory(
                    "sync endpoint Unix URL requires a path".to_string(),
                ));
            }
            return Ok(Self {
                transport: SyncClientTransport::Unix {
                    path: std::path::PathBuf::from(path),
                },
            });
        }
        let parsed = reqwest::Url::parse(&base_url).map_err(|_| {
            crate::VerletError::RuntimeFactory("sync endpoint URL is invalid".to_string())
        })?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(crate::VerletError::RuntimeFactory(
                "sync endpoint URL must use http:// or https://".to_string(),
            ));
        }
        if !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.path() != "/"
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(crate::VerletError::RuntimeFactory(
                "sync endpoint URL must be an origin without credentials, a path, a query, or a fragment"
                    .to_string(),
            ));
        }
        let base_url = parsed.as_str().trim_end_matches('/').to_string();
        Ok(Self {
            transport: SyncClientTransport::Network {
                client: reqwest::Client::new(),
                base_url,
            },
        })
    }

    fn endpoint_label(&self) -> String {
        match &self.transport {
            SyncClientTransport::Network { base_url, .. } => base_url.clone(),
            #[cfg(unix)]
            SyncClientTransport::Unix { path } => format!("unix://{}", path.display()),
        }
    }

    async fn send<T: serde::Serialize + ?Sized>(
        &self,
        path: &str,
        bearer_token: &str,
        body: &T,
    ) -> crate::VerletResult<(reqwest::StatusCode, Vec<u8>)> {
        match &self.transport {
            SyncClientTransport::Network { client, base_url } => {
                let response = client
                    .post(format!("{base_url}{path}"))
                    .bearer_auth(bearer_token)
                    .json(body)
                    .send()
                    .await
                    .map_err(|_| transport_error("request"))?;
                let status = response.status();
                let bytes = response
                    .bytes()
                    .await
                    .map_err(|_| transport_error("response"))?;
                Ok((status, bytes.to_vec()))
            }
            #[cfg(unix)]
            SyncClientTransport::Unix { path: socket } => {
                send_unix_request(socket, path, bearer_token, body).await
            }
        }
    }

    fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> crate::VerletResult<T> {
        serde_json::from_slice(bytes).map_err(|_| transport_error("response decode"))
    }
}

#[cfg(unix)]
async fn send_unix_request<T: serde::Serialize + ?Sized>(
    socket: &std::path::Path,
    path: &str,
    bearer_token: &str,
    body: &T,
) -> crate::VerletResult<(reqwest::StatusCode, Vec<u8>)> {
    let body = serde_json::to_vec(body).map_err(|_| transport_error("request encode"))?;
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {bearer_token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let mut stream = tokio::net::UnixStream::connect(socket)
        .await
        .map_err(|_| transport_error("request"))?;
    stream
        .write_all(head.as_bytes())
        .await
        .map_err(|_| transport_error("request"))?;
    stream
        .write_all(&body)
        .await
        .map_err(|_| transport_error("request"))?;
    stream
        .shutdown()
        .await
        .map_err(|_| transport_error("request"))?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .map_err(|_| transport_error("response"))?;
    decode_unix_response(&response)
}

#[cfg(unix)]
fn decode_unix_response(response: &[u8]) -> crate::VerletResult<(reqwest::StatusCode, Vec<u8>)> {
    let header_end = find_bytes(response, b"\r\n\r\n")
        .map(|index| index + 4)
        .ok_or_else(|| transport_error("response headers"))?;
    let headers = std::str::from_utf8(&response[..header_end])
        .map_err(|_| transport_error("response headers"))?;
    let mut lines = headers.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .and_then(|status| reqwest::StatusCode::from_u16(status).ok())
        .ok_or_else(|| transport_error("response status"))?;
    let content_length = lines
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .ok_or_else(|| transport_error("response content length"))?;
    let end = header_end
        .checked_add(content_length)
        .ok_or_else(|| transport_error("response size"))?;
    if response.len() != end {
        return Err(transport_error("response body"));
    }
    Ok((status, response[header_end..end].to_vec()))
}

#[async_trait::async_trait]
impl crate::daemon::remote_store::endpoint::SyncPushGate for HttpSyncClient {
    async fn push(
        &self,
        bearer_token: &str,
        request: crate::daemon::remote_store::endpoint::SyncPushRequestV1,
    ) -> crate::VerletResult<crate::daemon::remote_store::endpoint::SyncPushOutcome> {
        let (status, body) = self.send(PUSH_PATH, bearer_token, &request).await?;
        let outcome =
            Self::decode::<crate::daemon::remote_store::endpoint::SyncPushOutcome>(&body)?;
        if status.as_u16() != push_status(&outcome) || !outcome.matches_request(&request) {
            return Err(transport_error("push response validation"));
        }
        Ok(outcome)
    }
}

#[async_trait::async_trait]
impl crate::daemon::remote_store::endpoint::SyncPullSource for HttpSyncClient {
    async fn pull_after(
        &self,
        bearer_token: &str,
        stream_id: &crate::EventStreamId,
        cursor: Option<crate::StreamCursorV1>,
    ) -> crate::VerletResult<Vec<crate::StreamRecordEnvelopeV1>> {
        let request = crate::daemon::remote_store::endpoint::SyncPullRequestV1 {
            schema: crate::daemon::remote_store::endpoint::SYNC_PULL_SCHEMA_V1.to_string(),
            stream_id: stream_id.clone(),
            cursor,
        };
        let (status, body) = self.send(PULL_PATH, bearer_token, &request).await?;
        if status == reqwest::StatusCode::FORBIDDEN {
            return Err(crate::VerletError::History(
                "sync pull not authorized".to_string(),
            ));
        }
        if !status.is_success() {
            return Err(if status == reqwest::StatusCode::CONFLICT {
                crate::VerletError::History("sync pull cursor conflict".to_string())
            } else {
                transport_error("pull")
            });
        }
        let response =
            Self::decode::<crate::daemon::remote_store::endpoint::SyncPullResponseV1>(&body)?;
        if response.schema != crate::daemon::remote_store::endpoint::SYNC_PULL_SCHEMA_V1 {
            return Err(transport_error("pull schema"));
        }
        Ok(response.records)
    }
}

#[async_trait::async_trait]
impl crate::daemon::remote_store::endpoint::SyncIngressQueueAcknowledger for HttpSyncClient {
    async fn acknowledge_ingress(
        &self,
        bearer_token: &str,
        request: crate::daemon::remote_store::endpoint::SyncIngressQueueAckRequestV1,
    ) -> crate::VerletResult<()> {
        let (status, body) = self.send(INGRESS_ACK_PATH, bearer_token, &request).await?;
        if status == reqwest::StatusCode::FORBIDDEN {
            return Err(crate::VerletError::History(
                "sync pull not authorized".to_string(),
            ));
        }
        if !status.is_success() {
            return Err(transport_error("ingress acknowledgement"));
        }
        let response = Self::decode::<serde_json::Value>(&body)?;
        if response
            .get("acknowledged")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
        {
            return Err(transport_error("ingress acknowledgement response"));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::daemon::remote_store::endpoint::SyncLeaseRenewer for HttpSyncClient {
    async fn renew_lease(
        &self,
        bearer_token: &str,
    ) -> crate::VerletResult<Option<crate::daemon::remote_store::lease::StreamLeaseGrantV1>> {
        let (status, body) = self
            .send(RENEW_PATH, bearer_token, &serde_json::json!({}))
            .await?;
        if status == reqwest::StatusCode::FORBIDDEN {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(transport_error("lease renewal"));
        }
        let response = Self::decode::<
            crate::daemon::remote_store::endpoint::SyncLeaseRenewalResponseV1,
        >(&body)?;
        if response.schema != crate::daemon::remote_store::endpoint::SYNC_LEASE_RENEWAL_SCHEMA_V1 {
            return Err(transport_error("lease renewal schema"));
        }
        Ok(response.grant)
    }
}

fn transport_error(operation: &str) -> crate::VerletError {
    crate::VerletError::RuntimeExecution(format!("sync endpoint {operation} failed"))
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt as _;

    #[test]
    fn accept_retry_backoff_is_bounded() {
        assert_eq!(
            crate::daemon::remote_store::endpoint_http::next_accept_retry(
                crate::daemon::remote_store::endpoint_http::ACCEPT_RETRY_MIN
            ),
            std::time::Duration::from_millis(20)
        );
        assert_eq!(
            crate::daemon::remote_store::endpoint_http::next_accept_retry(
                crate::daemon::remote_store::endpoint_http::ACCEPT_RETRY_MAX
            ),
            crate::daemon::remote_store::endpoint_http::ACCEPT_RETRY_MAX
        );
    }

    #[tokio::test(start_paused = true)]
    async fn request_deadline_bounds_a_stalled_connection_task() {
        // tight-timeout: paused time exercises the configured request deadline itself
        let result = crate::daemon::remote_store::endpoint_http::request_with_timeout(
            std::time::Duration::from_millis(5),
            std::future::pending::<crate::VerletResult<()>>(),
        )
        .await;
        assert!(matches!(
            result,
            Err(crate::VerletError::RuntimeExecution(message)) if message == "sync request timed out"
        ));
    }

    #[tokio::test]
    async fn parser_rejects_duplicate_content_length_headers() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        client
            .write_all(
                b"POST /v1/sync/renew HTTP/1.1\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n",
            )
            .await
            .unwrap();
        client.shutdown().await.unwrap();

        assert!(
            crate::daemon::remote_store::endpoint_http::read_request(&mut server)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn parser_rejects_excessive_header_count_inside_byte_limit() {
        let mut request = String::from("POST /v1/sync/renew HTTP/1.1\r\n");
        for index in 0..=crate::daemon::remote_store::endpoint_http::MAX_HEADER_COUNT {
            request.push_str(&format!("X-{index}: v\r\n"));
        }
        request.push_str("\r\n");
        assert!(request.len() < crate::daemon::remote_store::endpoint_http::MAX_HEADER_BYTES);
        let (mut client, mut server) = tokio::io::duplex(request.len() + 1);
        client.write_all(request.as_bytes()).await.unwrap();
        client.shutdown().await.unwrap();

        assert!(
            crate::daemon::remote_store::endpoint_http::read_request(&mut server)
                .await
                .is_err()
        );
    }

    #[test]
    fn client_base_url_rejects_every_secret_bearing_url_surface() {
        assert!(
            crate::daemon::remote_store::endpoint_http::HttpSyncClient::new(
                "http://bearer@127.0.0.1:8080"
            )
            .is_err()
        );
        assert!(
            crate::daemon::remote_store::endpoint_http::HttpSyncClient::new(
                "https://127.0.0.1:8080?token=bearer"
            )
            .is_err()
        );
        assert!(
            crate::daemon::remote_store::endpoint_http::HttpSyncClient::new(
                "https://127.0.0.1:8080#bearer"
            )
            .is_err()
        );
        assert!(
            crate::daemon::remote_store::endpoint_http::HttpSyncClient::new(
                "http://127.0.0.1:8080"
            )
            .is_ok()
        );
    }
}
