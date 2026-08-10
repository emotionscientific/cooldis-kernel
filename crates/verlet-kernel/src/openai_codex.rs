use base64::Engine as _;
use sha2::Digest as _;

pub(crate) const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
pub(crate) const ORIGINATOR: &str = "verlet";
pub(crate) const CALLBACK_ADDR: &str = "127.0.0.1:1455";
pub(crate) const CALLBACK_PATH: &str = "/auth/callback";
pub(crate) const BROWSER_REDIRECT_URI: &str = "http://localhost:1455/auth/callback";
pub(crate) const DEVICE_REDIRECT_URI: &str = "https://auth.openai.com/deviceauth/callback";
pub(crate) const DEVICE_VERIFICATION_URI: &str = "https://auth.openai.com/codex/device";
pub(crate) const SCOPE: &str = "openid profile email offline_access";
pub(crate) const AUTHORIZE_URL: &str = "https://auth.openai.com/oauth/authorize";
pub(crate) const TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
pub(crate) const DEVICE_USER_CODE_URL: &str =
    "https://auth.openai.com/api/accounts/deviceauth/usercode";
pub(crate) const DEVICE_TOKEN_URL: &str = "https://auth.openai.com/api/accounts/deviceauth/token";

#[derive(Debug, thiserror::Error)]
#[error("OpenAI Codex authentication failed: {0}")]
pub(crate) struct OpenAICodexError(String);

type Result<T> = std::result::Result<T, OpenAICodexError>;

fn error(message: impl Into<String>) -> OpenAICodexError {
    OpenAICodexError(message.into())
}

fn pkce_pair_from_bytes(random: &[u8]) -> (String, String) {
    let verifier = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(random);
    let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(sha2::Sha256::digest(verifier.as_bytes()));
    (verifier, challenge)
}

fn credential_from_token_value(
    value: &serde_json::Value,
    now_ms: i64,
) -> Result<verlet_metadata::provider_store::LlmProviderCredential> {
    credential_from_token_value_with_identity(value, now_ms, None, None)
}

fn credential_from_token_value_with_identity(
    value: &serde_json::Value,
    now_ms: i64,
    fallback_account_id: Option<String>,
    fallback_email: Option<String>,
) -> Result<verlet_metadata::provider_store::LlmProviderCredential> {
    let access = value
        .get("access_token")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| error("token response did not include an access token"))?;
    let refresh = value
        .get("refresh_token")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| error("token response did not include a refresh token"))?;
    let expires_in = value
        .get("expires_in")
        .and_then(serde_json::Value::as_i64)
        .filter(|seconds| *seconds > 0)
        .ok_or_else(|| error("token response did not include a positive expiry"))?;
    let id_token = value
        .get("id_token")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let access_claims = jwt_claims(access);
    let id_claims = jwt_claims(id_token);
    let account_id = access_claims
        .0
        .or(id_claims.0)
        .or(fallback_account_id)
        .ok_or_else(|| {
        error("token response did not identify a ChatGPT account; run `verlet auth login openai-codex` again")
    })?;
    let email = access_claims.1.or(id_claims.1).or(fallback_email);
    Ok(
        verlet_metadata::provider_store::LlmProviderCredential::OAuth {
            access: access.to_string(),
            refresh: refresh.to_string(),
            expires_at_ms: now_ms.saturating_add(expires_in.saturating_mul(1000)),
            account_id: Some(account_id),
            email,
        },
    )
}

fn jwt_payload(token: &str) -> Option<serde_json::Value> {
    let payload = token.split('.').nth(1)?;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    serde_json::from_slice(&decoded).ok()
}

fn jwt_claims(token: &str) -> (Option<String>, Option<String>) {
    let Some(payload) = jwt_payload(token) else {
        return (None, None);
    };
    let account_id = payload
        .get("https://api.openai.com/auth")
        .and_then(|auth| auth.get("chatgpt_account_id"))
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            payload
                .get("chatgpt_account_id")
                .and_then(serde_json::Value::as_str)
        })
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let email = payload
        .get("https://api.openai.com/profile")
        .and_then(|profile| profile.get("email"))
        .and_then(serde_json::Value::as_str)
        .or_else(|| payload.get("email").and_then(serde_json::Value::as_str))
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    (account_id, email)
}

#[derive(Clone)]
struct OAuthEndpoints {
    authorize: String,
    token: String,
    device_user_code: String,
    device_token: String,
    device_verification: String,
    browser_redirect: String,
    device_redirect: String,
}

impl Default for OAuthEndpoints {
    fn default() -> Self {
        Self {
            authorize: AUTHORIZE_URL.to_string(),
            token: TOKEN_URL.to_string(),
            device_user_code: DEVICE_USER_CODE_URL.to_string(),
            device_token: DEVICE_TOKEN_URL.to_string(),
            device_verification: DEVICE_VERIFICATION_URI.to_string(),
            browser_redirect: BROWSER_REDIRECT_URI.to_string(),
            device_redirect: DEVICE_REDIRECT_URI.to_string(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct OpenAICodexOAuthClient {
    http: reqwest::Client,
    endpoints: OAuthEndpoints,
    device_poll_floor: std::time::Duration,
    user_auth_timeout: std::time::Duration,
}

pub(crate) struct BrowserLogin {
    listener: tokio::net::TcpListener,
    state: String,
    verifier: String,
    authorization_url: String,
}

impl BrowserLogin {
    pub(crate) fn authorization_url(&self) -> &str {
        &self.authorization_url
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeviceLogin {
    pub(crate) user_code: String,
    pub(crate) verification_uri: String,
    device_auth_id: String,
    interval: std::time::Duration,
}

impl OpenAICodexOAuthClient {
    pub(crate) fn new() -> Result<Self> {
        Self::with_endpoints(OAuthEndpoints::default())
    }

    fn with_endpoints(endpoints: OAuthEndpoints) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|err| error(format!("could not build the OAuth HTTP client: {err}")))?;
        Ok(Self {
            http,
            endpoints,
            device_poll_floor: std::time::Duration::from_secs(1),
            user_auth_timeout: std::time::Duration::from_secs(15 * 60),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_test_endpoints(base_url: &str) -> Result<Self> {
        let mut endpoints = OAuthEndpoints::default();
        endpoints.authorize = format!("{base_url}/authorize");
        endpoints.token = format!("{base_url}/oauth/token");
        endpoints.device_user_code = format!("{base_url}/device/usercode");
        endpoints.device_token = format!("{base_url}/device/token");
        endpoints.device_verification = format!("{base_url}/device/verify");
        let mut client = Self::with_endpoints(endpoints)?;
        client.device_poll_floor = std::time::Duration::ZERO;
        client.user_auth_timeout = std::time::Duration::from_secs(5);
        Ok(client)
    }

    pub(crate) async fn begin_browser_login(&self) -> Result<BrowserLogin> {
        let listener = tokio::net::TcpListener::bind(CALLBACK_ADDR)
            .await
            .map_err(|err| {
                error(format!(
                    "could not bind {CALLBACK_ADDR} for the OAuth callback: {err}; retry with `verlet auth login openai-codex --device`"
                ))
            })?;
        let mut random = [0u8; 32];
        getrandom::fill(&mut random)
            .map_err(|err| error(format!("could not generate OAuth PKCE state: {err}")))?;
        let state = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(random);
        getrandom::fill(&mut random)
            .map_err(|err| error(format!("could not generate OAuth PKCE verifier: {err}")))?;
        let (verifier, challenge) = pkce_pair_from_bytes(&random);
        let mut url = reqwest::Url::parse(&self.endpoints.authorize)
            .map_err(|err| error(format!("invalid OAuth authorization URL: {err}")))?;
        url.query_pairs_mut()
            .append_pair("response_type", "code")
            .append_pair("client_id", CLIENT_ID)
            .append_pair("redirect_uri", &self.endpoints.browser_redirect)
            .append_pair("scope", SCOPE)
            .append_pair("code_challenge", &challenge)
            .append_pair("code_challenge_method", "S256")
            .append_pair("state", &state)
            .append_pair("id_token_add_organizations", "true")
            .append_pair("codex_cli_simplified_flow", "true")
            .append_pair("originator", ORIGINATOR);
        Ok(BrowserLogin {
            listener,
            state,
            verifier,
            authorization_url: url.into(),
        })
    }

    pub(crate) async fn complete_browser_login(
        &self,
        login: BrowserLogin,
    ) -> Result<verlet_metadata::provider_store::LlmProviderCredential> {
        let code = tokio::time::timeout(
            self.user_auth_timeout,
            wait_for_browser_callback(login.listener, &login.state),
        )
        .await
        .map_err(|_| error("browser login timed out before the callback arrived"))??;
        self.exchange_code(&code, &login.verifier, &self.endpoints.browser_redirect)
            .await
    }

    pub(crate) async fn start_device_login(&self) -> Result<DeviceLogin> {
        let response = self
            .http
            .post(&self.endpoints.device_user_code)
            .json(&serde_json::json!({ "client_id": CLIENT_ID }))
            .send()
            .await
            .map_err(|err| error(format!("could not start device login: {err}")))?;
        let value = response_json(response, "device login start").await?;
        let device_auth_id = required_string(&value, "device_auth_id", "device login response")?;
        let user_code = required_string(&value, "user_code", "device login response")?;
        let verification_uri = value
            .get("verification_uri")
            .or_else(|| value.get("verification_url"))
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or(&self.endpoints.device_verification)
            .to_string();
        let interval_seconds = value
            .get("interval")
            .and_then(|value| {
                value
                    .as_u64()
                    .or_else(|| value.as_str().and_then(|value| value.trim().parse().ok()))
            })
            .unwrap_or(5);
        Ok(DeviceLogin {
            user_code,
            verification_uri,
            device_auth_id,
            interval: self
                .device_poll_floor
                .max(std::time::Duration::from_secs(interval_seconds)),
        })
    }

    pub(crate) async fn complete_device_login(
        &self,
        login: DeviceLogin,
    ) -> Result<verlet_metadata::provider_store::LlmProviderCredential> {
        tokio::time::timeout(self.user_auth_timeout, self.poll_device_login(login))
            .await
            .map_err(|_| error("device login timed out before authorization completed"))?
    }

    async fn poll_device_login(
        &self,
        mut login: DeviceLogin,
    ) -> Result<verlet_metadata::provider_store::LlmProviderCredential> {
        loop {
            tokio::time::sleep(login.interval).await;
            let response = self
                .http
                .post(&self.endpoints.device_token)
                .json(&serde_json::json!({
                    "device_auth_id": login.device_auth_id,
                    "user_code": login.user_code,
                }))
                .send()
                .await
                .map_err(|err| error(format!("could not poll device login: {err}")))?;
            let status = response.status();
            let text = response
                .text()
                .await
                .map_err(|err| error(format!("could not read device login response: {err}")))?;
            if status.is_success() {
                let value: serde_json::Value = serde_json::from_str(&text)
                    .map_err(|err| error(format!("device login returned invalid JSON: {err}")))?;
                let code = required_string(&value, "authorization_code", "device login response")?;
                let verifier = required_string(&value, "code_verifier", "device login response")?;
                return self
                    .exchange_code(&code, &verifier, &self.endpoints.device_redirect)
                    .await;
            }
            let code = oauth_error_code(&text);
            if matches!(status.as_u16(), 403 | 404)
                || code.as_deref() == Some("deviceauth_authorization_pending")
            {
                continue;
            }
            if code.as_deref() == Some("slow_down") {
                login.interval += std::time::Duration::from_secs(5);
                continue;
            }
            return Err(error(format!(
                "device login failed with status {status}{}",
                code.map(|code| format!(" ({code})")).unwrap_or_default()
            )));
        }
    }

    async fn exchange_code(
        &self,
        code: &str,
        verifier: &str,
        redirect_uri: &str,
    ) -> Result<verlet_metadata::provider_store::LlmProviderCredential> {
        let response = self
            .http
            .post(&self.endpoints.token)
            .form(&[
                ("grant_type", "authorization_code"),
                ("client_id", CLIENT_ID),
                ("code", code),
                ("code_verifier", verifier),
                ("redirect_uri", redirect_uri),
            ])
            .send()
            .await
            .map_err(|err| error(format!("could not exchange the authorization code: {err}")))?;
        let value = response_json(response, "authorization code exchange").await?;
        credential_from_token_value(&value, verlet_history::now_ms())
    }
}

async fn wait_for_browser_callback(
    listener: tokio::net::TcpListener,
    expected_state: &str,
) -> Result<String> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    loop {
        let (mut socket, _) = listener
            .accept()
            .await
            .map_err(|err| error(format!("could not accept the OAuth callback: {err}")))?;
        let mut request = vec![0u8; 8192];
        let length = socket
            .read(&mut request)
            .await
            .map_err(|err| error(format!("could not read the OAuth callback: {err}")))?;
        let first_line = String::from_utf8_lossy(&request[..length])
            .lines()
            .next()
            .unwrap_or_default()
            .to_string();
        let path = first_line.split_whitespace().nth(1).unwrap_or_default();
        let parsed = reqwest::Url::parse(&format!("http://localhost{path}"));
        let (status, message, result) = match parsed {
            Ok(url) if url.path() != CALLBACK_PATH => (
                "404 Not Found",
                "This is not the Verlet OAuth callback.",
                None,
            ),
            Ok(url)
                if url
                    .query_pairs()
                    .find(|(name, _)| name == "state")
                    .map(|(_, value)| value.into_owned())
                    .as_deref()
                    != Some(expected_state) =>
            {
                (
                    "400 Bad Request",
                    "Verlet rejected the callback because its state did not match.",
                    None,
                )
            }
            Ok(url) => {
                let params = url
                    .query_pairs()
                    .into_owned()
                    .collect::<std::collections::HashMap<_, _>>();
                if let Some(code) = params.get("code").filter(|code| !code.is_empty()) {
                    (
                        "200 OK",
                        "OpenAI login complete. You can close this window and return to Verlet.",
                        Some(Ok(code.clone())),
                    )
                } else if params.contains_key("error") {
                    (
                        "400 Bad Request",
                        "OpenAI login was not completed. Return to Verlet for details.",
                        Some(Err(error(
                            "the authorization server rejected the browser login",
                        ))),
                    )
                } else {
                    (
                        "400 Bad Request",
                        "The OAuth callback did not include an authorization code.",
                        None,
                    )
                }
            }
            Err(_) => (
                "400 Bad Request",
                "Verlet could not parse the OAuth callback.",
                None,
            ),
        };
        let body = format!(
            "<!doctype html><meta charset=\"utf-8\"><title>Verlet OpenAI login</title><p>{message}</p>"
        );
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket
            .write_all(response.as_bytes())
            .await
            .map_err(|err| error(format!("could not respond to the OAuth callback: {err}")))?;
        if let Some(result) = result {
            return result;
        }
    }
}

fn required_string(value: &serde_json::Value, field: &str, source: &str) -> Result<String> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| error(format!("{source} did not include {field}")))
}

async fn response_json(response: reqwest::Response, operation: &str) -> Result<serde_json::Value> {
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| error(format!("could not read {operation} response: {err}")))?;
    if !status.is_success() {
        let code = oauth_error_code(&text)
            .map(|code| format!(" ({code})"))
            .unwrap_or_default();
        return Err(error(format!("{operation} returned status {status}{code}")));
    }
    serde_json::from_str(&text)
        .map_err(|err| error(format!("{operation} returned invalid JSON: {err}")))
}

fn oauth_error_code(text: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    match value.get("error")? {
        serde_json::Value::String(code) => Some(code.clone()),
        serde_json::Value::Object(error) => error
            .get("code")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        _ => None,
    }
}

static REFRESH_GATE: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();

fn refresh_gate() -> &'static tokio::sync::Mutex<()> {
    REFRESH_GATE.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn credential_needs_refresh(expires_at_ms: i64, now_ms: i64) -> bool {
    expires_at_ms <= now_ms.saturating_add(60_000)
}

/// Provider-specific wire adapter for the ChatGPT-plan Codex endpoint.
///
/// The endpoint speaks the Responses protocol but does not accept every field
/// supported by the public Responses API. Keep those differences here so the
/// generic Responses adapter remains faithful to its own contract.
#[derive(Clone)]
struct OpenAICodexResponsesAdapter {
    inner: verlet_provider::OpenAIResponsesAdapter,
}

impl OpenAICodexResponsesAdapter {
    fn omit_unsupported_max_output_tokens(mut body: serde_json::Value) -> serde_json::Value {
        if let Some(body) = body.as_object_mut() {
            body.remove("max_output_tokens");
        }
        body
    }
}

impl verlet_provider::ProviderWireAdapter for OpenAICodexResponsesAdapter {
    fn api(&self) -> verlet_history::ProviderApi {
        verlet_provider::ProviderWireAdapter::api(&self.inner)
    }

    fn build_request_body(
        &self,
        request: &verlet_provider::ProviderRequest,
    ) -> verlet_provider::ProviderResult<serde_json::Value> {
        verlet_provider::ProviderWireAdapter::build_request_body(&self.inner, request)
            .map(Self::omit_unsupported_max_output_tokens)
    }

    fn decode_response_body(
        &self,
        body: &serde_json::Value,
    ) -> verlet_provider::ProviderResult<verlet_provider::ProviderResponse> {
        verlet_provider::ProviderWireAdapter::decode_response_body(&self.inner, body)
    }

    fn decode_stream_events(
        &self,
        sse: &str,
    ) -> verlet_provider::ProviderResult<Vec<verlet_provider::ProviderStreamEvent>> {
        verlet_provider::ProviderWireAdapter::decode_stream_events(&self.inner, sse)
    }
}

#[derive(Clone)]
pub(crate) struct OpenAICodexProviderClient {
    store: verlet_metadata::provider_store::SqliteMetadataStore,
    http: reqwest::Client,
    token_url: String,
    responses_url: String,
    adapter: std::sync::Arc<OpenAICodexResponsesAdapter>,
}

impl OpenAICodexProviderClient {
    pub(crate) fn new(store: verlet_metadata::provider_store::SqliteMetadataStore) -> Result<Self> {
        Self::with_urls(
            store,
            TOKEN_URL,
            verlet_metadata::provider_store::OPENAI_CODEX_RESPONSES_URL,
        )
    }

    pub(crate) fn with_responses_url(
        store: verlet_metadata::provider_store::SqliteMetadataStore,
        responses_url: impl Into<String>,
    ) -> Result<Self> {
        Self::with_urls(store, TOKEN_URL, responses_url)
    }

    fn with_urls(
        store: verlet_metadata::provider_store::SqliteMetadataStore,
        token_url: impl Into<String>,
        responses_url: impl Into<String>,
    ) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .map_err(|err| error(format!("could not build the Codex HTTP client: {err}")))?;
        Ok(Self {
            store,
            http,
            token_url: token_url.into(),
            responses_url: responses_url.into(),
            adapter: std::sync::Arc::new(OpenAICodexResponsesAdapter {
                inner: verlet_provider::OpenAIResponsesAdapter {
                    include_encrypted_reasoning: false,
                    reasoning_summary: verlet_provider::OpenAIReasoningSummary::Auto,
                },
            }),
        })
    }

    async fn fresh_credential(
        &self,
    ) -> Result<verlet_metadata::provider_store::LlmProviderCredential> {
        use verlet_metadata::provider_store::LlmProviderAuthStore as _;

        let _guard = refresh_gate().lock().await;
        let credential = self
            .store
            .get_credential(verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID)
            .await
            .map_err(|err| error(format!("could not read the provider store: {err}")))?
            .ok_or_else(relogin_error)?;
        let observed_credential = credential.clone();
        let verlet_metadata::provider_store::LlmProviderCredential::OAuth {
            access,
            refresh,
            expires_at_ms,
            account_id,
            email,
        } = credential
        else {
            return Err(relogin_error());
        };
        if !credential_needs_refresh(expires_at_ms, verlet_history::now_ms()) {
            return Ok(
                verlet_metadata::provider_store::LlmProviderCredential::OAuth {
                    access,
                    refresh,
                    expires_at_ms,
                    account_id,
                    email,
                },
            );
        }
        let response = self
            .http
            .post(&self.token_url)
            .form(&[
                ("grant_type", "refresh_token"),
                ("refresh_token", refresh.as_str()),
                ("client_id", CLIENT_ID),
            ])
            .send()
            .await
            .map_err(|err| error(format!("could not refresh the OAuth credential: {err}")))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|err| error(format!("could not read the OAuth refresh response: {err}")))?;
        let response_code = oauth_error_code(&text);
        let current_credential = self
            .store
            .get_credential(verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID)
            .await
            .map_err(|err| error(format!("could not re-read the provider store: {err}")))?;
        if current_credential.as_ref() != Some(&observed_credential) {
            return adopt_credential_changed_during_refresh(current_credential);
        }
        if !status.is_success() {
            if matches!(
                response_code.as_deref(),
                Some("refresh_token_reused" | "refresh_token_expired" | "invalid_grant")
            ) {
                self.clear_invalid_credential().await?;
                return Err(relogin_error());
            }
            return Err(error(format!(
                "OAuth refresh returned status {status}{}",
                response_code
                    .map(|code| format!(" ({code})"))
                    .unwrap_or_default()
            )));
        }
        let refreshed = serde_json::from_str(&text)
            .map_err(|err| error(format!("OAuth refresh returned invalid JSON: {err}")))
            .and_then(|value| {
                credential_from_token_value_with_identity(
                    &value,
                    verlet_history::now_ms(),
                    account_id,
                    email,
                )
            });
        let refreshed = match refreshed {
            Ok(refreshed) => refreshed,
            Err(_) => {
                self.clear_invalid_credential().await?;
                return Err(relogin_error());
            }
        };
        self.store
            .set_credential(
                verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID,
                refreshed.clone(),
            )
            .await
            .map_err(|err| {
                error(format!(
                    "could not store the refreshed OAuth credential: {err}"
                ))
            })?;
        Ok(refreshed)
    }

    async fn clear_invalid_credential(&self) -> Result<()> {
        use verlet_metadata::provider_store::LlmProviderAuthStore as _;

        self.store
            .delete_credential(verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID)
            .await
            .map_err(|err| {
                error(format!(
                    "could not clear the invalid OAuth credential: {err}; after correcting the provider store, run `verlet auth login openai-codex` again"
                ))
            })
    }

    fn endpoint_from_credential(
        &self,
        credential: verlet_metadata::provider_store::LlmProviderCredential,
    ) -> Result<verlet_provider::ProviderEndpoint> {
        let verlet_metadata::provider_store::LlmProviderCredential::OAuth {
            access,
            account_id: Some(account_id),
            ..
        } = credential
        else {
            return Err(relogin_error());
        };
        Ok(verlet_provider::ProviderEndpoint {
            url: self.responses_url.clone(),
            auth: verlet_provider::ProviderAuth::Bearer { token: access },
            headers: vec![
                (
                    "OpenAI-Beta".to_string(),
                    "responses=experimental".to_string(),
                ),
                ("chatgpt-account-id".to_string(), account_id),
                ("originator".to_string(), ORIGINATOR.to_string()),
            ],
        })
    }

    async fn provider_http_client(&self) -> Result<verlet_provider::ProviderHttpClient> {
        let endpoint = self.endpoint_from_credential(self.fresh_credential().await?)?;
        Ok(verlet_provider::ProviderHttpClient::with_http(
            self.http.clone(),
            endpoint,
            self.adapter.clone(),
        ))
    }
}

fn relogin_error() -> OpenAICodexError {
    error(
        "the saved login is missing or no longer valid; run `verlet auth login openai-codex` again",
    )
}

fn adopt_credential_changed_during_refresh(
    credential: Option<verlet_metadata::provider_store::LlmProviderCredential>,
) -> Result<verlet_metadata::provider_store::LlmProviderCredential> {
    let Some(credential) = credential else {
        return Err(relogin_error());
    };
    match &credential {
        verlet_metadata::provider_store::LlmProviderCredential::OAuth {
            expires_at_ms,
            account_id: Some(_),
            ..
        } if !credential_needs_refresh(*expires_at_ms, verlet_history::now_ms()) => Ok(credential),
        verlet_metadata::provider_store::LlmProviderCredential::OAuth { .. } => Err(error(
            "the saved OAuth credential changed while refresh was in flight; retry the request",
        )),
        verlet_metadata::provider_store::LlmProviderCredential::ApiKey { .. } => {
            Err(relogin_error())
        }
    }
}

#[async_trait::async_trait]
impl verlet_provider::ProviderClient for OpenAICodexProviderClient {
    fn capabilities(&self) -> Option<verlet_provider::ProviderCapabilityRecord> {
        use verlet_provider::ProviderWireAdapter as _;
        Some(self.adapter.capabilities())
    }

    async fn complete(
        &self,
        request: &verlet_provider::ProviderRequest,
    ) -> verlet_provider::ProviderResult<verlet_provider::ProviderResponse> {
        self.provider_http_client()
            .await
            .map_err(|err| verlet_provider::ProviderError::Http(err.to_string()))?
            .complete(request)
            .await
    }

    async fn stream(
        &self,
        request: &verlet_provider::ProviderRequest,
    ) -> verlet_provider::ProviderResult<Vec<verlet_provider::ProviderStreamEvent>> {
        self.provider_http_client()
            .await
            .map_err(|err| verlet_provider::ProviderError::Http(err.to_string()))?
            .stream(request)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use verlet_metadata::provider_store::LlmProviderAuthStore as _;
    use verlet_provider::{ProviderClient as _, ProviderWireAdapter as _};

    static CALLBACK_TEST_GATE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn jwt(payload: serde_json::Value) -> String {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode("{}");
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&payload).unwrap());
        format!("{header}.{payload}.signature")
    }

    struct FakeHttpServer {
        base_url: String,
        requests: Arc<Mutex<Vec<String>>>,
        task: tokio::task::JoinHandle<()>,
    }

    struct GatedFakeHttpServer {
        base_url: String,
        requests: Arc<Mutex<Vec<String>>>,
        request_seen: Arc<tokio::sync::Notify>,
        release_response: Arc<tokio::sync::Notify>,
        task: tokio::task::JoinHandle<()>,
    }

    async fn fake_http_server(responses: Vec<(u16, serde_json::Value)>) -> FakeHttpServer {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            let mut responses = VecDeque::from(responses);
            while let Some((status, body)) = responses.pop_front() {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = read_http_request(&mut socket).await;
                captured.lock().unwrap().push(request);
                let body = serde_json::to_string(&body).unwrap();
                let reason = if status < 300 { "OK" } else { "Bad Request" };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        FakeHttpServer {
            base_url: format!("http://{address}"),
            requests,
            task,
        }
    }

    async fn gated_fake_http_server(status: u16, body: serde_json::Value) -> GatedFakeHttpServer {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let request_seen = Arc::new(tokio::sync::Notify::new());
        let seen = Arc::clone(&request_seen);
        let release_response = Arc::new(tokio::sync::Notify::new());
        let release = Arc::clone(&release_response);
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut socket).await;
            captured.lock().unwrap().push(request);
            seen.notify_one();
            release.notified().await;
            let body = serde_json::to_string(&body).unwrap();
            let reason = if status < 300 { "OK" } else { "Bad Request" };
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        GatedFakeHttpServer {
            base_url: format!("http://{address}"),
            requests,
            request_seen,
            release_response,
            task,
        }
    }

    async fn read_http_request(socket: &mut tokio::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            let read = socket.read(&mut buffer).await.unwrap();
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..read]);
            let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(&bytes[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            if bytes.len() >= header_end + 4 + content_length {
                break;
            }
        }
        String::from_utf8(bytes).unwrap()
    }

    fn token_response(account_id: &str, access: &str, refresh: &str) -> serde_json::Value {
        let token = jwt(serde_json::json!({
            "https://api.openai.com/auth": { "chatgpt_account_id": account_id },
            "email": "user@example.com"
        }));
        serde_json::json!({
            "access_token": if access.is_empty() { token } else { access.to_string() },
            "refresh_token": refresh,
            "expires_in": 3600
        })
    }

    fn oauth_credential(
        access: impl Into<String>,
        refresh: impl Into<String>,
        expires_at_ms: i64,
    ) -> verlet_metadata::provider_store::LlmProviderCredential {
        verlet_metadata::provider_store::LlmProviderCredential::OAuth {
            access: access.into(),
            refresh: refresh.into(),
            expires_at_ms,
            account_id: Some("acct-123".to_string()),
            email: Some("user@example.com".to_string()),
        }
    }

    #[test]
    fn pkce_uses_s256_base64url_without_padding() {
        let (verifier, challenge) = pkce_pair_from_bytes(&[7; 32]);
        assert_eq!(verifier.len(), 43);
        assert_eq!(challenge.len(), 43);
        assert!(!verifier.contains('='));
        assert!(!challenge.contains('='));
        assert_ne!(verifier, challenge);
    }

    #[test]
    fn token_response_becomes_oauth_credential_with_display_claims() {
        let access = jwt(serde_json::json!({
            "https://api.openai.com/auth": { "chatgpt_account_id": "acct-123" },
            "https://api.openai.com/profile": { "email": "user@example.com" }
        }));
        let credential = credential_from_token_value(
            &serde_json::json!({
                "access_token": access,
                "refresh_token": "refresh-secret",
                "expires_in": 3600
            }),
            1_700_000_000_000,
        )
        .unwrap();

        assert_eq!(
            credential,
            verlet_metadata::provider_store::LlmProviderCredential::OAuth {
                access,
                refresh: "refresh-secret".to_string(),
                expires_at_ms: 1_700_003_600_000,
                account_id: Some("acct-123".to_string()),
                email: Some("user@example.com".to_string()),
            }
        );
    }

    #[tokio::test]
    async fn browser_login_validates_state_and_exchanges_pkce_code() {
        let _callback_guard = CALLBACK_TEST_GATE.lock().await;
        let token = token_response("acct-browser", "", "refresh-browser");
        let server = fake_http_server(vec![(200, token)]).await;
        let mut endpoints = OAuthEndpoints::default();
        endpoints.authorize = format!("{}/authorize", server.base_url);
        endpoints.token = format!("{}/token", server.base_url);
        let client = OpenAICodexOAuthClient::with_endpoints(endpoints).unwrap();
        let login = client.begin_browser_login().await.unwrap();
        let authorization = reqwest::Url::parse(login.authorization_url()).unwrap();
        let query = authorization
            .query_pairs()
            .into_owned()
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            query.get("code_challenge_method").map(String::as_str),
            Some("S256")
        );
        assert_eq!(query.get("originator").map(String::as_str), Some("verlet"));
        assert_eq!(query.get("scope").map(String::as_str), Some(SCOPE));
        let state = query["state"].clone();

        let callback = tokio::spawn(async move {
            for (path, expected_status) in [
                (
                    format!("/not-the-callback?code=browser-code&state={state}"),
                    "404 Not Found",
                ),
                (
                    format!("{CALLBACK_PATH}?code=browser-code&state=wrong-state"),
                    "400 Bad Request",
                ),
                (
                    format!("{CALLBACK_PATH}?code=&state={state}"),
                    "400 Bad Request",
                ),
                (
                    format!("{CALLBACK_PATH}?code=browser-code&state={state}"),
                    "200 OK",
                ),
            ] {
                let mut socket = tokio::net::TcpStream::connect(CALLBACK_ADDR).await.unwrap();
                let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n");
                socket.write_all(request.as_bytes()).await.unwrap();
                let mut response = Vec::new();
                socket.read_to_end(&mut response).await.unwrap();
                assert!(String::from_utf8_lossy(&response).contains(expected_status));
            }
        });
        let credential = client.complete_browser_login(login).await.unwrap();
        callback.await.unwrap();
        server.task.await.unwrap();

        assert!(matches!(
            credential,
            verlet_metadata::provider_store::LlmProviderCredential::OAuth {
                account_id: Some(ref account_id),
                ..
            } if account_id == "acct-browser"
        ));
        {
            let requests = server.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert!(requests[0].starts_with("POST /token "));
            assert!(requests[0].contains("code=browser-code"));
            assert!(requests[0].contains("code_verifier="));
            assert!(
                requests[0]
                    .contains("redirect_uri=http%3A%2F%2Flocalhost%3A1455%2Fauth%2Fcallback")
            );
        }
        assert!(tokio::net::TcpStream::connect(CALLBACK_ADDR).await.is_err());
    }

    #[tokio::test]
    async fn browser_login_bind_failure_recommends_the_device_flow() {
        let _callback_guard = CALLBACK_TEST_GATE.lock().await;
        let _occupied = tokio::net::TcpListener::bind(CALLBACK_ADDR).await.unwrap();
        let client = OpenAICodexOAuthClient::new().unwrap();

        let error = match client.begin_browser_login().await {
            Ok(_) => panic!("browser login unexpectedly bound an occupied callback port"),
            Err(error) => error,
        };

        let message = error.to_string();
        assert!(message.contains(CALLBACK_ADDR));
        assert!(message.contains("verlet auth login openai-codex --device"));
    }

    #[tokio::test]
    async fn device_login_uses_device_endpoints_then_exchanges_the_code() {
        let server = fake_http_server(vec![
            (
                200,
                serde_json::json!({
                    "device_auth_id": "device-123",
                    "user_code": "ABCD-EFGH",
                    "interval": 0
                }),
            ),
            (
                200,
                serde_json::json!({
                    "authorization_code": "device-code",
                    "code_verifier": "device-verifier"
                }),
            ),
            (200, token_response("acct-device", "", "refresh-device")),
        ])
        .await;
        let mut endpoints = OAuthEndpoints::default();
        endpoints.device_user_code = format!("{}/device/usercode", server.base_url);
        endpoints.device_token = format!("{}/device/token", server.base_url);
        endpoints.token = format!("{}/oauth/token", server.base_url);
        let mut client = OpenAICodexOAuthClient::with_endpoints(endpoints).unwrap();
        client.device_poll_floor = std::time::Duration::ZERO;
        let login = client.start_device_login().await.unwrap();
        assert_eq!(login.user_code, "ABCD-EFGH");
        assert_eq!(login.verification_uri, DEVICE_VERIFICATION_URI);
        let credential = client.complete_device_login(login).await.unwrap();
        server.task.await.unwrap();

        assert!(matches!(
            credential,
            verlet_metadata::provider_store::LlmProviderCredential::OAuth {
                account_id: Some(ref account_id),
                ..
            } if account_id == "acct-device"
        ));
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[0].starts_with("POST /device/usercode "));
        assert!(requests[0].contains(CLIENT_ID));
        assert!(requests[1].starts_with("POST /device/token "));
        assert!(requests[1].contains("device-123"));
        assert!(requests[2].starts_with("POST /oauth/token "));
        assert!(requests[2].contains("code=device-code"));
        assert!(requests[2].contains("code_verifier=device-verifier"));
        assert!(
            requests[2]
                .contains("redirect_uri=https%3A%2F%2Fauth.openai.com%2Fdeviceauth%2Fcallback")
        );
    }

    #[tokio::test]
    async fn concurrent_refresh_is_single_flight_and_adopts_the_rotated_store_value() {
        let server = fake_http_server(vec![(
            200,
            token_response("acct-123", "", "rotated-refresh"),
        )])
        .await;
        let store = verlet_metadata::provider_store::SqliteMetadataStore::in_memory()
            .await
            .unwrap();
        store
            .set_credential(
                verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID,
                oauth_credential("expired-access", "single-use-refresh", 1),
            )
            .await
            .unwrap();
        let client = OpenAICodexProviderClient::with_urls(
            store.clone(),
            format!("{}/oauth/token", server.base_url),
            "http://unused.invalid/responses",
        )
        .unwrap();

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let client = client.clone();
            tasks.push(tokio::spawn(async move {
                client.fresh_credential().await.unwrap()
            }));
        }
        for task in tasks {
            let credential = task.await.unwrap();
            assert!(matches!(
                credential,
                verlet_metadata::provider_store::LlmProviderCredential::OAuth {
                    ref refresh,
                    ..
                } if refresh == "rotated-refresh"
            ));
        }
        server.task.await.unwrap();
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }

    #[test]
    fn refresh_window_includes_expired_and_exactly_sixty_seconds_remaining() {
        let now_ms = 1_700_000_000_000;
        assert!(credential_needs_refresh(now_ms - 1, now_ms));
        assert!(credential_needs_refresh(now_ms + 60_000, now_ms));
        assert!(!credential_needs_refresh(now_ms + 60_001, now_ms));
        assert!(credential_needs_refresh(i64::MIN, i64::MAX));
    }

    #[tokio::test]
    async fn credential_deleted_during_refresh_is_not_resurrected() {
        let server =
            gated_fake_http_server(200, token_response("acct-123", "", "rotated-refresh")).await;
        let store = verlet_metadata::provider_store::SqliteMetadataStore::in_memory()
            .await
            .unwrap();
        store
            .set_credential(
                verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID,
                oauth_credential("expired-access", "single-use-refresh", 1),
            )
            .await
            .unwrap();
        let client = OpenAICodexProviderClient::with_urls(
            store.clone(),
            format!("{}/oauth/token", server.base_url),
            "http://unused.invalid/responses",
        )
        .unwrap();
        let refresh = tokio::spawn(async move { client.fresh_credential().await });
        // tight-timeout: the loopback fixture must observe the in-memory request promptly
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            server.request_seen.notified(),
        )
        .await
        .expect("refresh request did not reach the fake token endpoint");
        store
            .delete_credential(verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID)
            .await
            .unwrap();
        server.release_response.notify_one();

        let message = refresh.await.unwrap().unwrap_err().to_string();
        server.task.await.unwrap();
        assert!(message.contains("verlet auth login openai-codex"));
        assert!(
            store
                .get_credential(verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn refresh_token_reused_preserves_a_newer_store_winner() {
        let server = gated_fake_http_server(
            400,
            serde_json::json!({ "error": { "code": "refresh_token_reused" } }),
        )
        .await;
        let store = verlet_metadata::provider_store::SqliteMetadataStore::in_memory()
            .await
            .unwrap();
        store
            .set_credential(
                verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID,
                oauth_credential("expired-access", "spent-refresh", 1),
            )
            .await
            .unwrap();
        let client = OpenAICodexProviderClient::with_urls(
            store.clone(),
            format!("{}/oauth/token", server.base_url),
            "http://unused.invalid/responses",
        )
        .unwrap();
        let refresh = tokio::spawn(async move { client.fresh_credential().await });
        // tight-timeout: the loopback fixture must observe the in-memory request promptly
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            server.request_seen.notified(),
        )
        .await
        .expect("refresh request did not reach the fake token endpoint");
        let winner = oauth_credential(
            "winner-access",
            "winner-refresh",
            verlet_history::now_ms() + 3_600_000,
        );
        store
            .set_credential(
                verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID,
                winner.clone(),
            )
            .await
            .unwrap();
        server.release_response.notify_one();

        assert_eq!(refresh.await.unwrap().unwrap(), winner);
        server.task.await.unwrap();
        assert_eq!(
            store
                .get_credential(verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID)
                .await
                .unwrap(),
            Some(winner)
        );
    }

    #[tokio::test]
    async fn expired_refresh_token_is_cleared_and_instructs_relogin() {
        let server = fake_http_server(vec![(
            400,
            serde_json::json!({ "error": { "code": "invalid_grant" } }),
        )])
        .await;
        let store = verlet_metadata::provider_store::SqliteMetadataStore::in_memory()
            .await
            .unwrap();
        store
            .set_credential(
                verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID,
                oauth_credential("expired-access", "expired-refresh", 1),
            )
            .await
            .unwrap();
        let client = OpenAICodexProviderClient::with_urls(
            store.clone(),
            format!("{}/oauth/token", server.base_url),
            "http://unused.invalid/responses",
        )
        .unwrap();

        let message = client.fresh_credential().await.unwrap_err().to_string();
        server.task.await.unwrap();
        assert!(message.contains("verlet auth login openai-codex"));
        assert!(
            store
                .get_credential(verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn unusable_successful_refresh_response_clears_the_spent_credential() {
        let server = fake_http_server(vec![(
            200,
            serde_json::json!({
                "access_token": "rotated-access",
                "expires_in": 3600
            }),
        )])
        .await;
        let store = verlet_metadata::provider_store::SqliteMetadataStore::in_memory()
            .await
            .unwrap();
        store
            .set_credential(
                verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID,
                oauth_credential("expired-access", "single-use-refresh", 1),
            )
            .await
            .unwrap();
        let client = OpenAICodexProviderClient::with_urls(
            store.clone(),
            format!("{}/oauth/token", server.base_url),
            "http://unused.invalid/responses",
        )
        .unwrap();

        let message = client.fresh_credential().await.unwrap_err().to_string();
        server.task.await.unwrap();
        assert!(message.contains("verlet auth login openai-codex"));
        assert!(
            store
                .get_credential(verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn refresh_token_reused_clears_the_store_and_instructs_relogin() {
        let server = fake_http_server(vec![(
            400,
            serde_json::json!({ "error": { "code": "refresh_token_reused" } }),
        )])
        .await;
        let store = verlet_metadata::provider_store::SqliteMetadataStore::in_memory()
            .await
            .unwrap();
        store
            .set_credential(
                verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID,
                oauth_credential("expired-access", "spent-refresh", 1),
            )
            .await
            .unwrap();
        let client = OpenAICodexProviderClient::with_urls(
            store.clone(),
            format!("{}/oauth/token", server.base_url),
            "http://unused.invalid/responses",
        )
        .unwrap();

        let message = client.fresh_credential().await.unwrap_err().to_string();
        server.task.await.unwrap();
        assert!(message.contains("verlet auth login openai-codex"));
        assert!(
            store
                .get_credential(verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn codex_adapter_removes_only_max_output_tokens_from_complete_and_stream_requests() {
        let inner = verlet_provider::OpenAIResponsesAdapter {
            include_encrypted_reasoning: false,
            reasoning_summary: verlet_provider::OpenAIReasoningSummary::Auto,
        };
        let adapter = OpenAICodexResponsesAdapter {
            inner: inner.clone(),
        };
        let request = verlet_provider::ProviderRequest::new(
            verlet_history::ProviderApi::OpenAIResponses,
            verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID,
            "gpt-5.6-sol",
        );

        let bodies = [
            (
                adapter.build_request_body(&request).unwrap(),
                inner.build_request_body(&request).unwrap(),
            ),
            (
                adapter.build_stream_request_body(&request).unwrap(),
                inner.build_stream_request_body(&request).unwrap(),
            ),
        ];
        for (codex_body, mut generic_body) in bodies {
            assert_eq!(
                generic_body
                    .as_object_mut()
                    .unwrap()
                    .remove("max_output_tokens"),
                Some(serde_json::json!(request.max_tokens))
            );
            assert_eq!(codex_body, generic_body);
        }
    }

    #[tokio::test]
    async fn provider_reuses_responses_plumbing_with_codex_url_and_headers() {
        let server = fake_http_server(vec![(
            200,
            serde_json::json!({ "status": "completed", "output_text": "hello" }),
        )])
        .await;
        let store = verlet_metadata::provider_store::SqliteMetadataStore::in_memory()
            .await
            .unwrap();
        store
            .set_credential(
                verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID,
                oauth_credential(
                    "access-secret",
                    "refresh-secret",
                    verlet_history::now_ms() + 3_600_000,
                ),
            )
            .await
            .unwrap();
        let client = OpenAICodexProviderClient::with_urls(
            store,
            "http://unused.invalid/token",
            format!("{}/backend-api/codex/responses", server.base_url),
        )
        .unwrap();
        let request = verlet_provider::ProviderRequest::new(
            verlet_history::ProviderApi::OpenAIResponses,
            verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID,
            "gpt-5.6-sol",
        );

        let response = client.complete(&request).await.unwrap();
        server.task.await.unwrap();
        assert_eq!(
            response.content,
            vec![verlet_history::CanonicalContent::text("hello")]
        );
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let request = requests[0].to_ascii_lowercase();
        assert!(request.starts_with("post /backend-api/codex/responses "));
        assert!(request.contains("authorization: bearer access-secret"));
        assert!(request.contains("chatgpt-account-id: acct-123"));
        assert!(request.contains("originator: verlet"));
        assert!(request.contains("openai-beta: responses=experimental"));
        assert!(request.contains("\"model\":\"gpt-5.6-sol\""));
        assert!(request.contains("\"store\":false"));
        assert!(!request.contains("\"max_output_tokens\""));
    }
}
