//! Telegram protocol adapter pieces for Cooldis IO.
//!
//! This crate owns the Telegram Bot API wire subset and maps it to
//! `cooldis-io-core` envelopes. It intentionally stops before product policy:
//! a daemon or product adapter still decides tenant mapping, queueing, dedupe
//! persistence, and whether an inbound Telegram event queues, steers, or
//! interrupts a turn.

use async_trait::async_trait;
use cooldis_io_core::{
    ConversationKind, DeliveryReceipt, EgressAdapter, EgressEnvelope, IngressAck, IngressContent,
    IngressEnvelope, IngressSink, IoActor, IoAttachment, IoConversation, IoDedupeKey, IoError,
    IoProtocolAdapter, IoProtocolCapabilities, IoResult, IoSource, IoTarget,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;

pub const TELEGRAM_PROTOCOL: &str = "telegram.bot";

#[derive(Debug, thiserror::Error)]
pub enum TelegramError {
    #[error("Telegram update {0} does not contain a supported inbound event")]
    UnsupportedUpdate(i64),
    #[error("Telegram egress target uses protocol {0:?}")]
    InvalidProtocol(String),
    #[error("Telegram egress target uses instance {target:?}, expected {expected:?}")]
    InvalidInstance { target: String, expected: String },
    #[error("Telegram target is missing chat id")]
    MissingChatId,
    #[error("invalid Telegram chat id {0:?}")]
    InvalidChatId(String),
    #[error("invalid Telegram message thread id {0:?}")]
    InvalidThreadId(String),
    #[error("Telegram egress has no visible text to deliver")]
    NoVisibleText,
    #[error("Telegram API returned {status}: {body}")]
    Api { status: u16, body: String },
    #[error("Telegram API response was missing result")]
    MissingApiResult,
    #[error("Telegram API transport failed: {0}")]
    Transport(String),
    #[error("Telegram API response decode failed: {0}")]
    Decode(String),
}

impl From<TelegramError> for IoError {
    fn from(value: TelegramError) -> Self {
        match value {
            TelegramError::UnsupportedUpdate(_)
            | TelegramError::InvalidProtocol(_)
            | TelegramError::InvalidInstance { .. }
            | TelegramError::MissingChatId
            | TelegramError::InvalidChatId(_)
            | TelegramError::InvalidThreadId(_) => IoError::InvalidEnvelope(value.to_string()),
            TelegramError::NoVisibleText
            | TelegramError::Api { .. }
            | TelegramError::MissingApiResult
            | TelegramError::Transport(_)
            | TelegramError::Decode(_) => IoError::Delivery(value.to_string()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramNormalizer {
    instance_id: String,
}

impl TelegramNormalizer {
    pub fn new(instance_id: impl Into<String>) -> Self {
        Self {
            instance_id: instance_id.into(),
        }
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn source(&self) -> IoSource {
        IoSource::new(TELEGRAM_PROTOCOL, self.instance_id.clone())
    }

    pub fn normalize_update(
        &self,
        update: &TelegramUpdate,
        received_at_ms: u64,
    ) -> IoResult<Option<IngressEnvelope>> {
        if let Some(message) = update.message.as_ref() {
            return Ok(Some(self.envelope_from_message(
                update.update_id,
                "message",
                message,
                None,
                message_content(message),
                received_at_ms,
            )));
        }

        if let Some(message) = update.edited_message.as_ref() {
            return Ok(Some(self.envelope_from_message(
                update.update_id,
                "edited_message",
                message,
                None,
                message_content(message),
                received_at_ms,
            )));
        }

        if let Some(message) = update.channel_post.as_ref() {
            return Ok(Some(self.envelope_from_message(
                update.update_id,
                "channel_post",
                message,
                None,
                message_content(message),
                received_at_ms,
            )));
        }

        if let Some(message) = update.edited_channel_post.as_ref() {
            return Ok(Some(self.envelope_from_message(
                update.update_id,
                "edited_channel_post",
                message,
                None,
                message_content(message),
                received_at_ms,
            )));
        }

        if let Some(callback) = update.callback_query.as_ref() {
            let Some(message) = callback.message.as_ref() else {
                return Ok(None);
            };
            return Ok(Some(self.envelope_from_message(
                update.update_id,
                "callback_query",
                message,
                Some(actor_from_user(&callback.from)),
                IngressContent::Event {
                    kind: "telegram.callback_query".to_string(),
                    payload: json!({
                        "id": callback.id,
                        "data": callback.data,
                    }),
                },
                received_at_ms,
            )));
        }

        Ok(None)
    }

    fn envelope_from_message(
        &self,
        update_id: i64,
        update_kind: &'static str,
        message: &TelegramMessage,
        actor: Option<IoActor>,
        content: IngressContent,
        received_at_ms: u64,
    ) -> IngressEnvelope {
        let source = self.source();
        let mut envelope = IngressEnvelope::new(
            source.clone(),
            conversation_from_chat(&message.chat, message.message_thread_id),
            content,
            received_at_ms,
        )
        .with_dedupe_key(IoDedupeKey::for_source(
            &source,
            format!("update:{update_id}"),
        ))
        .with_metadata("telegram_update_id", update_id.to_string())
        .with_metadata("telegram_update_kind", update_kind)
        .with_metadata("telegram_message_id", message.message_id.to_string())
        .with_metadata("telegram_message_date", message.date.to_string());

        envelope.actor = actor.or_else(|| actor_from_message(message));
        envelope.attachments = message_attachments(message);
        envelope
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramWebhookAdapter {
    normalizer: TelegramNormalizer,
}

impl TelegramWebhookAdapter {
    pub fn new(instance_id: impl Into<String>) -> Self {
        Self {
            normalizer: TelegramNormalizer::new(instance_id),
        }
    }

    pub fn normalizer(&self) -> &TelegramNormalizer {
        &self.normalizer
    }

    pub async fn submit_update(
        &self,
        sink: &dyn IngressSink,
        update: &TelegramUpdate,
        received_at_ms: u64,
    ) -> IoResult<Option<IngressAck>> {
        let Some(envelope) = self.normalizer.normalize_update(update, received_at_ms)? else {
            return Ok(None);
        };
        sink.submit(envelope).await.map(Some)
    }
}

impl IoProtocolAdapter for TelegramWebhookAdapter {
    fn kind(&self) -> &'static str {
        TELEGRAM_PROTOCOL
    }

    fn capabilities(&self) -> IoProtocolCapabilities {
        IoProtocolCapabilities {
            ingress: true,
            egress: false,
            streaming: false,
            durable_offsets: true,
            attachments: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TelegramEgressAdapter {
    instance_id: String,
    client: TelegramBotClient,
}

impl TelegramEgressAdapter {
    pub fn new(instance_id: impl Into<String>, bot_token: impl Into<String>) -> Self {
        Self {
            instance_id: instance_id.into(),
            client: TelegramBotClient::new(bot_token),
        }
    }

    pub fn with_client(instance_id: impl Into<String>, client: TelegramBotClient) -> Self {
        Self {
            instance_id: instance_id.into(),
            client,
        }
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn build_send_message(
        &self,
        envelope: &EgressEnvelope,
    ) -> IoResult<Option<TelegramSendMessageRequest>> {
        self.validate_target(&envelope.target)?;
        build_send_message_request(envelope)
    }

    fn validate_target(&self, target: &IoTarget) -> IoResult<()> {
        if target.source.protocol != TELEGRAM_PROTOCOL {
            return Err(TelegramError::InvalidProtocol(target.source.protocol.clone()).into());
        }
        if target.source.instance_id != self.instance_id {
            return Err(TelegramError::InvalidInstance {
                target: target.source.instance_id.clone(),
                expected: self.instance_id.clone(),
            }
            .into());
        }
        Ok(())
    }
}

impl IoProtocolAdapter for TelegramEgressAdapter {
    fn kind(&self) -> &'static str {
        TELEGRAM_PROTOCOL
    }

    fn capabilities(&self) -> IoProtocolCapabilities {
        IoProtocolCapabilities {
            ingress: false,
            egress: true,
            streaming: false,
            durable_offsets: false,
            attachments: false,
        }
    }
}

#[async_trait]
impl EgressAdapter for TelegramEgressAdapter {
    async fn deliver(&self, envelope: EgressEnvelope) -> IoResult<DeliveryReceipt> {
        self.validate_target(&envelope.target)?;
        let Some(request) = build_send_message_request(&envelope)? else {
            return Ok(suppressed_receipt(&envelope, "no_visible_text"));
        };

        let message = self.client.send_message(&request).await?;
        Ok(DeliveryReceipt::delivered(
            &envelope,
            message.message_id.to_string(),
        ))
    }
}

#[derive(Clone, Debug)]
pub struct TelegramBotClient {
    token: String,
    api_base: String,
    http: reqwest::Client,
}

impl TelegramBotClient {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            api_base: "https://api.telegram.org".to_string(),
            http: reqwest::Client::new(),
        }
    }

    pub fn with_api_base(mut self, api_base: impl Into<String>) -> Self {
        self.api_base = api_base.into();
        self
    }

    pub async fn send_message(
        &self,
        request: &TelegramSendMessageRequest,
    ) -> IoResult<TelegramMessage> {
        let url = format!(
            "{}/bot{}/sendMessage",
            self.api_base.trim_end_matches('/'),
            self.token
        );
        let response = self
            .http
            .post(url)
            .json(request)
            .send()
            .await
            .map_err(|err| TelegramError::Transport(sanitize_reqwest_error(err)))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| TelegramError::Transport(sanitize_reqwest_error(err)))?;

        if !status.is_success() {
            return Err(TelegramError::Api {
                status: status.as_u16(),
                body: truncate_body(&body),
            }
            .into());
        }

        let decoded: TelegramApiMessageResponse =
            serde_json::from_str(&body).map_err(|err| TelegramError::Decode(err.to_string()))?;
        if !decoded.ok {
            return Err(TelegramError::Api {
                status: decoded.error_code.unwrap_or(status.as_u16() as i64) as u16,
                body: decoded.description.unwrap_or_else(|| "not ok".to_string()),
            }
            .into());
        }
        decoded.result.ok_or(TelegramError::MissingApiResult.into())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TelegramUpdate {
    pub update_id: i64,
    #[serde(default)]
    pub message: Option<TelegramMessage>,
    #[serde(default)]
    pub edited_message: Option<TelegramMessage>,
    #[serde(default)]
    pub channel_post: Option<TelegramMessage>,
    #[serde(default)]
    pub edited_channel_post: Option<TelegramMessage>,
    #[serde(default)]
    pub callback_query: Option<TelegramCallbackQuery>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TelegramCallbackQuery {
    pub id: String,
    pub from: TelegramUser,
    #[serde(default)]
    pub message: Option<TelegramMessage>,
    #[serde(default)]
    pub data: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TelegramMessage {
    pub message_id: i64,
    #[serde(default)]
    pub message_thread_id: Option<i64>,
    #[serde(default)]
    pub from: Option<TelegramUser>,
    #[serde(default)]
    pub sender_chat: Option<TelegramChat>,
    pub chat: TelegramChat,
    pub date: i64,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub caption: Option<String>,
    #[serde(default)]
    pub document: Option<TelegramDocument>,
    #[serde(default)]
    pub photo: Vec<TelegramPhotoSize>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TelegramUser {
    pub id: i64,
    #[serde(default)]
    pub is_bot: bool,
    pub first_name: String,
    #[serde(default)]
    pub last_name: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub language_code: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TelegramChat {
    pub id: i64,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub first_name: Option<String>,
    #[serde(default)]
    pub last_name: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TelegramDocument {
    pub file_id: String,
    #[serde(default)]
    pub file_unique_id: Option<String>,
    #[serde(default)]
    pub file_name: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub file_size: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TelegramPhotoSize {
    pub file_id: String,
    #[serde(default)]
    pub file_unique_id: Option<String>,
    pub width: u32,
    pub height: u32,
    #[serde(default)]
    pub file_size: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TelegramSendMessageRequest {
    pub chat_id: TelegramChatId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_thread_id: Option<i64>,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_to_message_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disable_notification: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum TelegramChatId {
    Id(i64),
    Username(String),
}

#[derive(Debug, Deserialize)]
struct TelegramApiMessageResponse {
    ok: bool,
    #[serde(default)]
    result: Option<TelegramMessage>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    error_code: Option<i64>,
}

pub fn build_send_message_request(
    envelope: &EgressEnvelope,
) -> IoResult<Option<TelegramSendMessageRequest>> {
    let Some(text) = envelope.kind.visible_text() else {
        return Ok(None);
    };
    if text.is_empty() {
        return Ok(None);
    }

    Ok(Some(TelegramSendMessageRequest {
        chat_id: parse_chat_id(&envelope.target.conversation.external_conversation_id)?,
        message_thread_id: envelope
            .target
            .conversation
            .external_thread_id
            .as_deref()
            .map(parse_thread_id)
            .transpose()?,
        text: text.to_string(),
        reply_to_message_id: envelope
            .target
            .metadata
            .get("telegram_reply_to_message_id")
            .map(|value| parse_i64(value, TelegramError::InvalidThreadId(value.clone())))
            .transpose()?,
        disable_notification: envelope
            .target
            .metadata
            .get("telegram_disable_notification")
            .and_then(|value| value.parse::<bool>().ok()),
    }))
}

pub fn target_from_message(instance_id: impl Into<String>, message: &TelegramMessage) -> IoTarget {
    IoTarget {
        source: IoSource::new(TELEGRAM_PROTOCOL, instance_id),
        conversation: conversation_from_chat(&message.chat, message.message_thread_id),
        actor: actor_from_message(message),
        metadata: BTreeMap::new(),
    }
}

fn conversation_from_chat(chat: &TelegramChat, message_thread_id: Option<i64>) -> IoConversation {
    let kind = match chat.kind.as_str() {
        "private" => ConversationKind::Direct,
        "channel" => ConversationKind::Channel,
        "supergroup" | "group" => ConversationKind::Group,
        _ => ConversationKind::Group,
    };

    let mut conversation = IoConversation::new(format!("telegram:chat:{}", chat.id), kind)
        .with_title(chat_title(chat));

    if let Some(thread_id) = message_thread_id {
        conversation = conversation.with_external_thread_id(thread_id.to_string());
    }

    conversation
        .metadata
        .insert("telegram_chat_type".to_string(), chat.kind.clone());
    if let Some(username) = chat.username.as_ref() {
        conversation
            .metadata
            .insert("telegram_chat_username".to_string(), username.clone());
    }
    conversation
}

fn actor_from_message(message: &TelegramMessage) -> Option<IoActor> {
    message
        .from
        .as_ref()
        .map(actor_from_user)
        .or_else(|| message.sender_chat.as_ref().map(actor_from_chat))
}

fn actor_from_user(user: &TelegramUser) -> IoActor {
    let mut actor = IoActor::new(format!("telegram:user:{}", user.id));
    actor.display_name = Some(user_display_name(user));
    actor.is_bot = user.is_bot;
    if let Some(username) = user.username.as_ref() {
        actor
            .metadata
            .insert("telegram_username".to_string(), username.clone());
    }
    if let Some(language_code) = user.language_code.as_ref() {
        actor
            .metadata
            .insert("telegram_language_code".to_string(), language_code.clone());
    }
    actor
}

fn actor_from_chat(chat: &TelegramChat) -> IoActor {
    let mut actor = IoActor::new(format!("telegram:chat:{}", chat.id));
    actor.display_name = Some(chat_title(chat));
    actor
        .metadata
        .insert("telegram_chat_type".to_string(), chat.kind.clone());
    actor
}

fn message_content(message: &TelegramMessage) -> IngressContent {
    if let Some(text) = message.text.as_ref().filter(|text| !text.is_empty()) {
        return text_or_command(text);
    }
    if let Some(caption) = message
        .caption
        .as_ref()
        .filter(|caption| !caption.is_empty())
    {
        return text_or_command(caption);
    }

    IngressContent::Event {
        kind: "telegram.message".to_string(),
        payload: json!({
            "message_id": message.message_id,
            "has_document": message.document.is_some(),
            "photo_count": message.photo.len(),
        }),
    }
}

fn text_or_command(text: &str) -> IngressContent {
    let Some(command) = text.strip_prefix('/') else {
        return IngressContent::text(text);
    };

    let mut parts = command.splitn(2, char::is_whitespace);
    let name = parts.next().unwrap_or_default().to_string();
    let args = parts
        .next()
        .map(str::trim)
        .filter(|args| !args.is_empty())
        .map(ToOwned::to_owned);

    if name.is_empty() {
        IngressContent::text(text)
    } else {
        IngressContent::Command { name, args }
    }
}

fn message_attachments(message: &TelegramMessage) -> Vec<IoAttachment> {
    let mut attachments = Vec::new();

    if let Some(document) = message.document.as_ref() {
        let mut attachment = IoAttachment::new(
            format!("telegram:file:{}", document.file_id),
            document
                .mime_type
                .clone()
                .unwrap_or_else(|| "application/octet-stream".to_string()),
        );
        attachment.name = document.file_name.clone();
        attachment.uri = Some(format!("telegram:file:{}", document.file_id));
        attachment.size_bytes = document.file_size;
        if let Some(file_unique_id) = document.file_unique_id.as_ref() {
            attachment.metadata.insert(
                "telegram_file_unique_id".to_string(),
                file_unique_id.clone(),
            );
        }
        attachments.push(attachment);
    }

    if let Some(photo) = message.photo.last() {
        let mut attachment =
            IoAttachment::new(format!("telegram:file:{}", photo.file_id), "image/jpeg");
        attachment.uri = Some(format!("telegram:file:{}", photo.file_id));
        attachment.size_bytes = photo.file_size;
        attachment
            .metadata
            .insert("telegram_width".to_string(), photo.width.to_string());
        attachment
            .metadata
            .insert("telegram_height".to_string(), photo.height.to_string());
        if let Some(file_unique_id) = photo.file_unique_id.as_ref() {
            attachment.metadata.insert(
                "telegram_file_unique_id".to_string(),
                file_unique_id.clone(),
            );
        }
        attachments.push(attachment);
    }

    attachments
}

fn parse_chat_id(value: &str) -> IoResult<TelegramChatId> {
    let raw = value.strip_prefix("telegram:chat:").unwrap_or(value).trim();
    if raw.is_empty() {
        return Err(TelegramError::MissingChatId.into());
    }
    if let Ok(id) = raw.parse::<i64>() {
        return Ok(TelegramChatId::Id(id));
    }
    if raw.starts_with('@') {
        return Ok(TelegramChatId::Username(raw.to_string()));
    }
    Err(TelegramError::InvalidChatId(value.to_string()).into())
}

fn parse_thread_id(value: &str) -> IoResult<i64> {
    let raw = value.strip_prefix("telegram:topic:").unwrap_or(value);
    parse_i64(raw, TelegramError::InvalidThreadId(value.to_string()))
}

fn parse_i64(value: &str, err: TelegramError) -> IoResult<i64> {
    value.parse::<i64>().map_err(|_| err.into())
}

fn chat_title(chat: &TelegramChat) -> String {
    chat.title
        .clone()
        .or_else(|| {
            chat.username
                .as_ref()
                .map(|username| format!("@{username}"))
        })
        .or_else(|| match (&chat.first_name, &chat.last_name) {
            (Some(first), Some(last)) => Some(format!("{first} {last}")),
            (Some(first), None) => Some(first.clone()),
            (None, Some(last)) => Some(last.clone()),
            (None, None) => None,
        })
        .unwrap_or_else(|| format!("Telegram chat {}", chat.id))
}

fn user_display_name(user: &TelegramUser) -> String {
    match (&user.last_name, &user.username) {
        (Some(last), _) => format!("{} {}", user.first_name, last),
        (None, Some(username)) if user.first_name.is_empty() => format!("@{username}"),
        _ => user.first_name.clone(),
    }
}

fn suppressed_receipt(envelope: &EgressEnvelope, reason: &str) -> DeliveryReceipt {
    DeliveryReceipt {
        egress_id: envelope.id.clone(),
        delivered: true,
        external_message_id: None,
        error: None,
        metadata: BTreeMap::from([("telegram_suppressed".to_string(), reason.to_string())]),
    }
}

fn sanitize_reqwest_error(err: reqwest::Error) -> String {
    if let Some(status) = err.status() {
        format!("status {status}")
    } else if err.is_timeout() {
        "timeout".to_string()
    } else if err.is_connect() {
        "connect error".to_string()
    } else {
        "transport error".to_string()
    }
}

fn truncate_body(body: &str) -> String {
    const MAX_BODY_CHARS: usize = 512;
    body.chars().take(MAX_BODY_CHARS).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn direct_update() -> TelegramUpdate {
        serde_json::from_value(json!({
            "update_id": 999,
            "message": {
                "message_id": 555,
                "from": {
                    "id": 42,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada"
                },
                "chat": {
                    "id": 123,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada"
                },
                "date": 1777000000,
                "text": "hello telegram"
            }
        }))
        .unwrap()
    }

    #[test]
    fn normalizes_direct_message_update_to_ingress_envelope() {
        let normalizer = TelegramNormalizer::new("main");
        let envelope = normalizer
            .normalize_update(&direct_update(), 1_777_000_000_123)
            .unwrap()
            .unwrap();

        assert_eq!(envelope.source.protocol, TELEGRAM_PROTOCOL);
        assert_eq!(envelope.source.instance_id, "main");
        assert_eq!(
            envelope.conversation.external_conversation_id,
            "telegram:chat:123"
        );
        assert_eq!(envelope.conversation.kind, ConversationKind::Direct);
        assert_eq!(
            envelope
                .actor
                .as_ref()
                .map(|actor| actor.external_actor_id.as_str()),
            Some("telegram:user:42")
        );
        assert_eq!(
            envelope.dedupe_key.as_ref().map(IoDedupeKey::stable_key),
            Some("telegram.bot:main:update:999".to_string())
        );
        assert_eq!(envelope.content.text_projection(), "hello telegram");
        assert_eq!(
            envelope
                .metadata
                .get("telegram_message_id")
                .map(String::as_str),
            Some("555")
        );
    }

    #[test]
    fn normalizes_supergroup_topic_command() {
        let update: TelegramUpdate = serde_json::from_value(json!({
            "update_id": 1000,
            "message": {
                "message_id": 556,
                "message_thread_id": 777,
                "from": {
                    "id": 43,
                    "is_bot": false,
                    "first_name": "Grace",
                    "last_name": "Hopper"
                },
                "chat": {
                    "id": -10042,
                    "type": "supergroup",
                    "title": "Cooldis HQ"
                },
                "date": 1777000001,
                "text": "/steer keep going"
            }
        }))
        .unwrap();

        let envelope = TelegramNormalizer::new("main")
            .normalize_update(&update, 1)
            .unwrap()
            .unwrap();

        assert_eq!(envelope.conversation.kind, ConversationKind::Group);
        assert_eq!(
            envelope.conversation.external_thread_id.as_deref(),
            Some("777")
        );
        assert_eq!(envelope.content.text_projection(), "/steer keep going");
        assert!(matches!(
            envelope.content,
            IngressContent::Command {
                ref name,
                ref args
            } if name == "steer" && args.as_deref() == Some("keep going")
        ));
    }

    #[test]
    fn normalizes_callback_query_as_event() {
        let update: TelegramUpdate = serde_json::from_value(json!({
            "update_id": 1001,
            "callback_query": {
                "id": "cb-1",
                "from": {
                    "id": 44,
                    "is_bot": false,
                    "first_name": "Lin"
                },
                "message": {
                    "message_id": 557,
                    "chat": {
                        "id": 123,
                        "type": "private",
                        "first_name": "Ada"
                    },
                    "date": 1777000002,
                    "text": "button source"
                },
                "data": "approve"
            }
        }))
        .unwrap();

        let envelope = TelegramNormalizer::new("main")
            .normalize_update(&update, 1)
            .unwrap()
            .unwrap();

        assert_eq!(
            envelope
                .actor
                .as_ref()
                .map(|actor| actor.external_actor_id.as_str()),
            Some("telegram:user:44")
        );
        assert!(matches!(
            envelope.content,
            IngressContent::Event { ref kind, .. } if kind == "telegram.callback_query"
        ));
        assert_eq!(
            envelope
                .metadata
                .get("telegram_update_kind")
                .map(String::as_str),
            Some("callback_query")
        );
    }

    #[test]
    fn captures_basic_document_and_photo_attachments() {
        let update: TelegramUpdate = serde_json::from_value(json!({
            "update_id": 1002,
            "message": {
                "message_id": 558,
                "chat": { "id": 123, "type": "private" },
                "date": 1777000003,
                "caption": "see attached",
                "document": {
                    "file_id": "doc-file",
                    "file_unique_id": "doc-unique",
                    "file_name": "report.pdf",
                    "mime_type": "application/pdf",
                    "file_size": 1234
                },
                "photo": [
                    {
                        "file_id": "small-photo",
                        "file_unique_id": "small",
                        "width": 64,
                        "height": 64,
                        "file_size": 512
                    },
                    {
                        "file_id": "large-photo",
                        "file_unique_id": "large",
                        "width": 1024,
                        "height": 768,
                        "file_size": 4096
                    }
                ]
            }
        }))
        .unwrap();

        let envelope = TelegramNormalizer::new("main")
            .normalize_update(&update, 1)
            .unwrap()
            .unwrap();

        assert_eq!(envelope.content.text_projection(), "see attached");
        assert_eq!(envelope.attachments.len(), 2);
        assert_eq!(envelope.attachments[0].name.as_deref(), Some("report.pdf"));
        assert_eq!(envelope.attachments[1].id, "telegram:file:large-photo");
    }

    #[test]
    fn builds_send_message_request_from_egress() {
        let conversation = IoConversation::new("telegram:chat:-10042", ConversationKind::Group)
            .with_external_thread_id("777");
        let mut target = IoTarget {
            source: IoSource::new(TELEGRAM_PROTOCOL, "main"),
            conversation,
            actor: None,
            metadata: BTreeMap::new(),
        };
        target.metadata.insert(
            "telegram_reply_to_message_id".to_string(),
            "555".to_string(),
        );
        let egress = EgressEnvelope::new(
            target,
            cooldis_io_core::EgressKind::AssistantMessage {
                text: "hello back".to_string(),
            },
            1,
        );

        let request = build_send_message_request(&egress).unwrap().unwrap();

        assert_eq!(request.chat_id, TelegramChatId::Id(-10042));
        assert_eq!(request.message_thread_id, Some(777));
        assert_eq!(request.reply_to_message_id, Some(555));
        assert_eq!(request.text, "hello back");
    }

    #[test]
    fn skips_non_visible_egress_events() {
        let egress = EgressEnvelope::new(
            IoTarget {
                source: IoSource::new(TELEGRAM_PROTOCOL, "main"),
                conversation: IoConversation::new("telegram:chat:123", ConversationKind::Direct),
                actor: None,
                metadata: BTreeMap::new(),
            },
            cooldis_io_core::EgressKind::ToolStarted {
                name: "bash".to_string(),
            },
            1,
        );

        assert_eq!(build_send_message_request(&egress).unwrap(), None);
    }

    #[test]
    fn egress_adapter_rejects_wrong_bot_instance() {
        let egress = EgressEnvelope::new(
            IoTarget {
                source: IoSource::new(TELEGRAM_PROTOCOL, "other"),
                conversation: IoConversation::new("telegram:chat:123", ConversationKind::Direct),
                actor: None,
                metadata: BTreeMap::new(),
            },
            cooldis_io_core::EgressKind::AssistantMessage {
                text: "hello".to_string(),
            },
            1,
        );

        let err = TelegramEgressAdapter::new("main", "token")
            .build_send_message(&egress)
            .unwrap_err();
        assert!(err.to_string().contains("expected"));
    }

    #[test]
    fn protocol_capabilities_are_explicit() {
        let ingress = TelegramWebhookAdapter::new("main").capabilities();
        assert!(ingress.ingress);
        assert!(!ingress.egress);
        assert!(ingress.durable_offsets);

        let egress = TelegramEgressAdapter::new("main", "token").capabilities();
        assert!(!egress.ingress);
        assert!(egress.egress);
    }
}
