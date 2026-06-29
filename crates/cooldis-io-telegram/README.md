# cooldis-io-telegram

`cooldis-io-telegram` is the first protocol adapter crate built on
`cooldis-io-core`.

It owns Telegram-specific concerns:

- parsing Telegram Bot API updates;
- normalizing messages, commands, callbacks, and basic attachments into
  `IngressEnvelope`;
- building `sendMessage` requests from visible `EgressEnvelope` output;
- optionally delivering those requests through the Telegram Bot API.

It deliberately does not own:

- tenant/user/session resolution;
- billing, auth, or product policy;
- queueing, dedupe storage, retries, or dead-letter handling;
- turn admission choices such as queue, steer, or interrupt.

Webhook HTTP routing should live in `cooldisd` or a service crate and call this
crate's `TelegramWebhookAdapter::submit_update` with the shared IO sink.
