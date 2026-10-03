//! Alert DELIVERY and the dead-man's heartbeat of the rebalancer service (W6.1 / W6.2 of
//! `product-mandate/SENIOR_RESEARCHER_GAP_CLOSURE_PLAN.md`; IMPL WP5.1 / WP5.2).
//!
//! `rebalancer_store::PgNotifier` is a PERSISTENCE sink: it INSERTs every alert the pipeline raises into
//! `rebalancer_alerts` and stops there ("was this alert ever raised", never "did a human see it"). This crate is the
//! other half:
//!
//! * [`worker::DeliveryWorker`] runs once per service tick, after the runs. It reads the alerts raised since its
//!   watermark, and for each verified, enabled channel that applies (the alert's tenant's channels for every severity;
//!   platform-scope channels for `warning` and `critical`) inserts ONE `alert_deliveries` row, insert-if-absent on
//!   `(channel_id, dedupe_key)` -- that unique key IS the de-duplication: the pipeline raises `ALERT_STILL_HALTED` on
//!   every run of a halted account with the same `dedupe_key`, and a person gets one e-mail, not one per tick. It then
//!   sends every delivery row that has neither `sent_at` nor `error` through a [`sender::Sender`] and marks the row
//!   ONCE (`sent_at` + `provider_message_id`, or `error`). A row is never deleted and never re-sent: when no sender is
//!   configured the row is marked `error = 'sender_disabled'` and stays as the audit trail of an alert nobody was
//!   told about.
//! * [`heartbeat::Heartbeat`] pings an external monitor (healthchecks.io style) after every tick that ran, including
//!   a tick with nothing due; a tick that could not run (kill flag unreadable, due scan failed) pings the monitor's
//!   fail URL instead. The ping is bounded by its transport's timeout and never decides anything: a failed ping is
//!   logged and raised once per failure streak as `ALERT_HEARTBEAT_FAILED` through the notifier.
//!
//! Both are dark by default: `ALERTS_ENABLED` (default `false`) and `HEARTBEAT_URL` (unset = disabled). The tables
//! `alert_channels` and `alert_deliveries` are a contract with `databaseschema-internal`
//! (`feat/w6-1-alert-channels`); the Postgres implementation of [`ledger::DeliveryLedger`] is
//! `rebalancer_store::PgDeliveryLedger` and fails closed with `ALERT_STORE_UNAVAILABLE` when they are absent.
//!
//! Everything in this crate is pure: the ledger and the sender are traits with in-memory / fake implementations, HTTP
//! goes through `broker_adapters::transport::HttpTransport`, and no test touches a network or a database.

#![forbid(unsafe_code)]

pub mod heartbeat;
pub mod ledger;
pub mod sender;
pub mod severity;
pub mod template;
pub mod worker;

pub use heartbeat::{Heartbeat, HeartbeatConfig, HeartbeatMonitor, HeartbeatResult, TickOutcome, DEFAULT_FAIL_SUFFIX, ENV_HEARTBEAT_FAIL_SUFFIX, ENV_HEARTBEAT_URL};
pub use ledger::{Channel, ChannelScope, DeliveryLedger, InMemoryLedger, LedgerError, NewDelivery, PendingAlert, PendingDelivery, SOURCE_REBALANCER};
pub use sender::{sender_from_lookup, DisabledSender, OutboundEmail, ResendSender, SendError, Sender, ENV_ALERT_FROM_EMAIL, ENV_RESEND_API_KEY, SENDER_DISABLED_ERROR};
pub use severity::{channel_applies, delivery_severity, Severity};
pub use template::{render, Rendered};
pub use worker::{alerts_enabled, DeliveryWorker, TickReport, DEFAULT_BACKLOG_SECS, ENV_ALERTS_ENABLED};
