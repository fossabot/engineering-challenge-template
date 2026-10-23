//! Kalshi websocket session: authentication, chunked subscriptions, sequence
//! validation, lifecycle subscription, and snapshot requests.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use async_trait::async_trait;
use reqwest::Client;
use serde_json::{Value, json};

use crate::{
    config::KalshiSettings,
    exchange::kalshi::{
        KalshiAuth,
        auth::{HEADER_KEY, HEADER_SIGNATURE, HEADER_TIMESTAMP},
        codec::{KalshiMessage, decode_kalshi_message},
    },
    model::SourceId,
    session::{MarketDataSession, SessionFactory, SessionFailure, SessionFrame},
    time::now_us,
    ws::{self, WsPayload, WsStream},
};

/// Kalshi adapter handle. It owns credentials and the REST client; websocket
/// sessions are created per source by [`SessionFactory`].
pub struct KalshiExchange {
    pub(crate) settings: KalshiSettings,
    pub(crate) auth: Option<Arc<KalshiAuth>>,
    pub(crate) client: Client,
}

pub type KalshiSessionFactory = KalshiExchange;

#[async_trait]
impl SessionFactory for KalshiExchange {
    async fn create_session(&self, source: SourceId) -> anyhow::Result<Box<dyn MarketDataSession>> {
        let auth = self.auth.as_ref().with_context(|| {
            format!(
                "Kalshi credentials are required for source {source}; provide key_id and a private key"
            )
        })?;
        Ok(Box::new(KalshiSession::new(
            self.settings.clone(),
            Arc::clone(auth),
        )))
    }
}

struct OrderbookSubscription {
    sid: i64,
    markets: BTreeSet<String>,
}

pub struct KalshiSession {
    settings: KalshiSettings,
    auth: Arc<KalshiAuth>,
    stream: Option<WsStream>,
    next_id: u64,
    desired: BTreeSet<String>,
    orderbook_subs: Vec<OrderbookSubscription>,
    pending_subscribes: BTreeMap<u64, BTreeSet<String>>,
    pending_since: Option<Instant>,
    seq_by_sid: BTreeMap<i64, u64>,
    lifecycle_sid: Option<i64>,
    lifecycle_subscribed: bool,
}

impl KalshiSession {
    fn new(settings: KalshiSettings, auth: Arc<KalshiAuth>) -> Self {
        Self {
            settings,
            auth,
            stream: None,
            next_id: 1,
            desired: BTreeSet::new(),
            orderbook_subs: Vec::new(),
            pending_subscribes: BTreeMap::new(),
            pending_since: None,
            seq_by_sid: BTreeMap::new(),
            lifecycle_sid: None,
            lifecycle_subscribed: false,
        }
    }

    fn stream_mut(&mut self) -> Result<&mut WsStream, SessionFailure> {
        self.stream.as_mut().ok_or_else(|| {
            SessionFailure::Reconnect(anyhow::anyhow!("Kalshi session is not connected"))
        })
    }

    fn next_command_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1).max(1);
        id
    }

    async fn send_json(&mut self, mut value: Value) -> Result<u64, SessionFailure> {
        let id = self.next_command_id();
        value["id"] = json!(id);
        let text = serde_json::to_string(&value)
            .context("serializing Kalshi command")
            .map_err(SessionFailure::Reconnect)?;
        ws::send_text(self.stream_mut()?, text)
            .await
            .map_err(SessionFailure::Reconnect)?;
        Ok(id)
    }

    async fn send_lifecycle_subscribe(&mut self) -> Result<(), SessionFailure> {
        self.send_json(json!({
            "cmd": "subscribe",
            "params": {"channels": ["market_lifecycle_v2"]}
        }))
        .await?;
        self.lifecycle_subscribed = true;
        Ok(())
    }

    async fn send_orderbook_subscribe(
        &mut self,
        markets: &[String],
    ) -> Result<u64, SessionFailure> {
        let id = self
            .send_json(json!({
                "cmd": "subscribe",
                "params": {
                    "channels": ["orderbook_delta"],
                    "market_tickers": markets,
                    "use_yes_price": false
                }
            }))
            .await?;
        self.pending_subscribes
            .insert(id, markets.iter().cloned().collect());
        self.pending_since.get_or_insert_with(Instant::now);
        Ok(id)
    }

    async fn send_update(
        &mut self,
        action: &str,
        sid: i64,
        markets: &[String],
    ) -> Result<(), SessionFailure> {
        if markets.is_empty() {
            return Ok(());
        }
        self.send_json(json!({
            "cmd": "update_subscription",
            "params": {
                "sids": [sid],
                "market_tickers": markets,
                "action": action
            }
        }))
        .await?;
        Ok(())
    }

    async fn reconcile(&mut self) -> Result<(), SessionFailure> {
        if !self.desired.is_empty()
            && self.orderbook_subs.is_empty()
            && self.pending_subscribes.is_empty()
        {
            let markets: Vec<String> = self.desired.iter().cloned().collect();
            for chunk in markets.chunks(self.settings.batch_size.max(1)) {
                self.send_orderbook_subscribe(chunk).await?;
            }
            return Ok(());
        }
        if !self.pending_subscribes.is_empty() {
            return Ok(());
        }

        let batch_size = self.settings.batch_size.max(1);
        let current: BTreeSet<String> = self
            .orderbook_subs
            .iter()
            .flat_map(|subscription| subscription.markets.iter().cloned())
            .collect();
        let additions: Vec<String> = self.desired.difference(&current).cloned().collect();
        let deletions: Vec<String> = current.difference(&self.desired).cloned().collect();

        for index in 0..self.orderbook_subs.len() {
            let (sid, remove) = {
                let subscription = &self.orderbook_subs[index];
                let remove: Vec<String> = deletions
                    .iter()
                    .filter(|symbol| subscription.markets.contains(*symbol))
                    .cloned()
                    .collect();
                (subscription.sid, remove)
            };
            if !remove.is_empty() {
                self.send_update("delete_markets", sid, &remove).await?;
                let subscription = &mut self.orderbook_subs[index];
                for symbol in remove {
                    subscription.markets.remove(&symbol);
                }
            }
        }

        let mut remaining = additions;
        for index in 0..self.orderbook_subs.len() {
            if remaining.is_empty() {
                break;
            }
            let (sid, capacity) = {
                let subscription = &self.orderbook_subs[index];
                (
                    subscription.sid,
                    batch_size.saturating_sub(subscription.markets.len()),
                )
            };
            if capacity == 0 {
                continue;
            }
            let take = capacity.min(remaining.len());
            let chunk: Vec<String> = remaining.drain(..take).collect();
            self.send_update("add_markets", sid, &chunk).await?;
            self.orderbook_subs[index].markets.extend(chunk);
        }
        for chunk in remaining.chunks(batch_size) {
            self.send_orderbook_subscribe(chunk).await?;
        }
        Ok(())
    }

    fn check_snapshot_sequence(&mut self, sid: i64, seq: u64) -> bool {
        match self.seq_by_sid.get(&sid).copied() {
            Some(previous) if seq < previous => false,
            Some(previous) => {
                if seq > previous {
                    self.seq_by_sid.insert(sid, seq);
                }
                true
            }
            None => {
                self.seq_by_sid.insert(sid, seq);
                true
            }
        }
    }

    fn check_delta_sequence(&mut self, sid: i64, seq: u64) -> Result<(), SessionFailure> {
        match self.seq_by_sid.get(&sid).copied() {
            Some(previous) if seq == previous.saturating_add(1) => {
                self.seq_by_sid.insert(sid, seq);
                Ok(())
            }
            Some(previous) => Err(SessionFailure::Protocol(anyhow::anyhow!(
                "Kalshi sequence gap on sid {sid}: expected {}, received {seq}",
                previous.saturating_add(1)
            ))),
            None => Err(SessionFailure::Protocol(anyhow::anyhow!(
                "Kalshi delta on sid {sid} arrived before a snapshot"
            ))),
        }
    }
}

#[async_trait]
impl MarketDataSession for KalshiSession {
    async fn connect(&mut self) -> Result<(), SessionFailure> {
        self.close().await;
        let headers = self
            .auth
            .headers("GET", "/trade-api/ws/v2")
            .context("signing Kalshi websocket handshake")
            .map_err(SessionFailure::Fatal)?;
        let handshake_headers = vec![
            (HEADER_KEY, headers.key),
            (HEADER_SIGNATURE, headers.signature),
            (HEADER_TIMESTAMP, headers.timestamp),
        ];
        let stream = ws::connect(self.settings.environment.ws_url(), &handshake_headers)
            .await
            .map_err(|error| {
                if matches!(ws::http_status(&error), Some(401 | 403)) {
                    SessionFailure::Fatal(error.context("Kalshi websocket authentication rejected"))
                } else {
                    SessionFailure::Reconnect(error.context("connecting to Kalshi websocket"))
                }
            })?;
        self.stream = Some(stream);
        self.next_id = 1;
        self.orderbook_subs.clear();
        self.pending_subscribes.clear();
        self.pending_since = None;
        self.seq_by_sid.clear();
        self.lifecycle_sid = None;
        self.lifecycle_subscribed = false;
        Ok(())
    }

    async fn apply_subscriptions(
        &mut self,
        desired: &BTreeSet<String>,
    ) -> Result<(), SessionFailure> {
        self.desired = desired.clone();
        if !self.lifecycle_subscribed {
            self.send_lifecycle_subscribe().await?;
        }
        self.reconcile().await
    }

    async fn next_frame(&mut self) -> Result<SessionFrame, SessionFailure> {
        if self.pending_subscribes.is_empty() {
            self.pending_since = None;
        } else {
            let since = *self.pending_since.get_or_insert_with(Instant::now);
            if since.elapsed() > Duration::from_secs(15) {
                return Err(SessionFailure::Protocol(anyhow::anyhow!(
                    "Kalshi orderbook subscription acknowledgement timed out"
                )));
            }
        }
        let payload = ws::next_payload(self.stream_mut()?)
            .await
            .map_err(SessionFailure::Reconnect)?;
        let text = match payload {
            WsPayload::Idle => return Ok(SessionFrame::idle(0)),
            WsPayload::Text(text) => text,
        };
        let bytes = text.len();
        let received_at_us = now_us().unwrap_or(0);
        let message = decode_kalshi_message(&text, received_at_us)
            .map_err(|error| SessionFailure::Decode(error.context("decoding Kalshi frame")))?;
        match message {
            KalshiMessage::Snapshot { sid, seq, event } => {
                if !self.check_snapshot_sequence(sid, seq) {
                    return Ok(SessionFrame::idle(bytes));
                }
                Ok(SessionFrame {
                    events: vec![event],
                    bytes,
                })
            }
            KalshiMessage::Delta { sid, seq, event } => {
                self.check_delta_sequence(sid, seq)?;
                Ok(SessionFrame {
                    events: vec![event],
                    bytes,
                })
            }
            KalshiMessage::Lifecycle { sid, event, .. } => {
                if self
                    .lifecycle_sid
                    .is_none_or(|lifecycle_sid| lifecycle_sid == sid)
                {
                    Ok(SessionFrame {
                        events: vec![event],
                        bytes,
                    })
                } else {
                    Ok(SessionFrame::idle(bytes))
                }
            }
            KalshiMessage::Subscribed { id, channel, sid } => {
                if channel == "orderbook_delta" {
                    let id = id.context(
                        "Kalshi orderbook subscription acknowledgement is missing its request id",
                    )?;
                    let markets = self.pending_subscribes.remove(&id).context(
                        "Kalshi orderbook subscription acknowledgement referenced an unknown id",
                    )?;
                    if markets.is_empty() {
                        return Err(SessionFailure::Protocol(anyhow::anyhow!(
                            "Kalshi orderbook subscription acknowledgement had an empty market set"
                        )));
                    }
                    if self
                        .orderbook_subs
                        .iter()
                        .any(|subscription| subscription.sid == sid)
                    {
                        return Err(SessionFailure::Protocol(anyhow::anyhow!(
                            "Kalshi orderbook subscription reused sid {sid}"
                        )));
                    }
                    self.orderbook_subs
                        .push(OrderbookSubscription { sid, markets });
                } else if channel == "market_lifecycle_v2" {
                    self.lifecycle_sid = Some(sid);
                }
                if self.pending_subscribes.is_empty() {
                    self.pending_since = None;
                    self.reconcile().await?;
                }
                Ok(SessionFrame::idle(bytes))
            }
            KalshiMessage::Ok { .. } => Ok(SessionFrame::idle(bytes)),
            KalshiMessage::Error { id, code, message } => {
                if let Some(id) = id {
                    self.pending_subscribes.remove(&id);
                }
                if self.pending_subscribes.is_empty() {
                    self.pending_since = None;
                }
                Err(SessionFailure::Protocol(anyhow::anyhow!(
                    "Kalshi websocket error (code {}): {message}",
                    code.map_or_else(|| "unknown".to_owned(), |code| code.to_string())
                )))
            }
            KalshiMessage::Unsubscribed { sid, .. } => {
                if let Some(sid) = sid {
                    self.orderbook_subs
                        .retain(|subscription| subscription.sid != sid);
                    self.seq_by_sid.remove(&sid);
                }
                Ok(SessionFrame::idle(bytes))
            }
            KalshiMessage::Ignore { .. } => Ok(SessionFrame::idle(bytes)),
        }
    }

    async fn send_keepalive(&mut self) -> Result<(), SessionFailure> {
        ws::send_ping(self.stream_mut()?)
            .await
            .map_err(SessionFailure::Reconnect)
    }

    async fn request_snapshot(&mut self, symbols: &[String]) -> Result<(), SessionFailure> {
        let wanted: BTreeSet<&str> = symbols.iter().map(String::as_str).collect();
        let requests: Vec<(i64, Vec<String>)> = self
            .orderbook_subs
            .iter()
            .filter_map(|subscription| {
                let markets: Vec<String> = subscription
                    .markets
                    .iter()
                    .filter(|symbol| wanted.contains(symbol.as_str()))
                    .cloned()
                    .collect();
                (!markets.is_empty()).then_some((subscription.sid, markets))
            })
            .collect();
        for (sid, markets) in requests {
            self.send_update("get_snapshot", sid, &markets).await?;
        }
        Ok(())
    }

    async fn close(&mut self) {
        if let Some(stream) = self.stream.take() {
            let _ = ws::close(stream).await;
        }
        self.orderbook_subs.clear();
        self.pending_subscribes.clear();
        self.pending_since = None;
        self.seq_by_sid.clear();
        self.lifecycle_subscribed = false;
        self.lifecycle_sid = None;
    }
}
