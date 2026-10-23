//! Polymarket CLOB websocket session.
//!
//! The public market channel uses application-level `PING`/`PONG` frames and
//! absolute level updates. There is no sequence number, so protocol faults are
//! handled by dropping the socket and resubscribing, which yields fresh books.

use std::collections::BTreeSet;

use anyhow::Context;
use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;

use crate::{
    config::PolymarketSettings,
    exchange::polymarket::codec::decode_polymarket_message,
    model::SourceId,
    session::{MarketDataSession, SessionFactory, SessionFailure, SessionFrame},
    time::now_us,
    ws::{self, WsPayload, WsStream},
};

pub struct PolymarketExchange {
    pub(crate) settings: PolymarketSettings,
    pub(crate) client: Client,
}

pub type PolymarketSessionFactory = PolymarketExchange;

#[async_trait]
impl SessionFactory for PolymarketExchange {
    async fn create_session(
        &self,
        _source: SourceId,
    ) -> anyhow::Result<Box<dyn MarketDataSession>> {
        Ok(Box::new(PolymarketSession::new(self.settings.clone())))
    }
}

pub struct PolymarketSession {
    settings: PolymarketSettings,
    stream: Option<WsStream>,
    desired: BTreeSet<String>,
    subscribed_assets: BTreeSet<String>,
    initial_sent: bool,
    /// Set before writing PONG so a cancelled write is retried instead of
    /// silently dropping the server's keepalive response.
    pong_pending: bool,
}

impl PolymarketSession {
    fn new(settings: PolymarketSettings) -> Self {
        Self {
            settings,
            stream: None,
            desired: BTreeSet::new(),
            subscribed_assets: BTreeSet::new(),
            initial_sent: false,
            pong_pending: false,
        }
    }

    fn stream_mut(&mut self) -> Result<&mut WsStream, SessionFailure> {
        self.stream.as_mut().ok_or_else(|| {
            SessionFailure::Reconnect(anyhow::anyhow!("Polymarket session is not connected"))
        })
    }

    async fn send_json(&mut self, value: serde_json::Value) -> Result<(), SessionFailure> {
        let text = serde_json::to_string(&value)
            .context("serializing Polymarket command")
            .map_err(SessionFailure::Reconnect)?;
        ws::send_text(self.stream_mut()?, text)
            .await
            .map_err(SessionFailure::Reconnect)
    }

    async fn send_initial(&mut self) -> Result<(), SessionFailure> {
        let assets: Vec<String> = self.desired.iter().cloned().collect();
        if assets.is_empty() {
            return Ok(());
        }
        // Polymarket expects one market subscription frame for the initial
        // dump. Sending several type=market frames on one socket makes the
        // server emit a non-JSON control response, so keep this frame whole.
        self.send_json(json!({
            "assets_ids": assets,
            "type": "market",
            "initial_dump": true,
            "level": 2,
            "custom_feature_enabled": true
        }))
        .await?;
        self.subscribed_assets = self.desired.clone();
        self.initial_sent = true;
        Ok(())
    }

    async fn flush_pending_pong(&mut self) -> Result<(), SessionFailure> {
        if !self.pong_pending {
            return Ok(());
        }
        ws::send_text(self.stream_mut()?, "PONG".to_owned())
            .await
            .map_err(SessionFailure::Reconnect)?;
        self.pong_pending = false;
        Ok(())
    }
}

#[async_trait]
impl MarketDataSession for PolymarketSession {
    async fn connect(&mut self) -> Result<(), SessionFailure> {
        self.close().await;
        let stream = ws::connect(&self.settings.ws_url, &[])
            .await
            .map_err(|error| {
                SessionFailure::Reconnect(error.context("connecting to Polymarket websocket"))
            })?;
        self.stream = Some(stream);
        self.subscribed_assets.clear();
        self.initial_sent = false;
        self.pong_pending = false;
        Ok(())
    }

    async fn apply_subscriptions(
        &mut self,
        desired: &BTreeSet<String>,
    ) -> Result<(), SessionFailure> {
        self.desired = desired.clone();
        if !self.initial_sent {
            return self.send_initial().await;
        }
        let additions: Vec<String> = desired
            .difference(&self.subscribed_assets)
            .cloned()
            .collect();
        let deletions: Vec<String> = self
            .subscribed_assets
            .difference(desired)
            .cloned()
            .collect();
        for chunk in additions.chunks(self.settings.batch_size.max(1)) {
            self.send_json(json!({
                "operation": "subscribe",
                "assets_ids": chunk
            }))
            .await?;
            self.subscribed_assets.extend(chunk.iter().cloned());
        }
        for chunk in deletions.chunks(self.settings.batch_size.max(1)) {
            self.send_json(json!({
                "operation": "unsubscribe",
                "assets_ids": chunk
            }))
            .await?;
            for asset in chunk {
                self.subscribed_assets.remove(asset);
            }
        }
        Ok(())
    }

    async fn next_frame(&mut self) -> Result<SessionFrame, SessionFailure> {
        self.flush_pending_pong().await?;
        let payload = ws::next_payload(self.stream_mut()?)
            .await
            .map_err(SessionFailure::Reconnect)?;
        let text = match payload {
            WsPayload::Idle => return Ok(SessionFrame::idle(0)),
            WsPayload::Text(text) => text,
        };
        let bytes = text.len();
        let control = text.trim();
        if control.is_empty() {
            return Ok(SessionFrame::idle(bytes));
        }
        if control.eq_ignore_ascii_case("PING") {
            self.pong_pending = true;
            self.flush_pending_pong().await?;
            return Ok(SessionFrame::idle(bytes));
        }
        if control.eq_ignore_ascii_case("PONG") {
            return Ok(SessionFrame::idle(bytes));
        }
        let received_at_us = now_us().unwrap_or(0);
        let events = decode_polymarket_message(&text, received_at_us).map_err(|error| {
            let preview: String = text.chars().take(160).collect();
            SessionFailure::Decode(error.context(format!(
                "decoding Polymarket frame ({} bytes, preview={preview:?})",
                text.len()
            )))
        })?;
        if events.is_empty() {
            Ok(SessionFrame::idle(bytes))
        } else {
            Ok(SessionFrame { events, bytes })
        }
    }

    async fn send_keepalive(&mut self) -> Result<(), SessionFailure> {
        ws::send_text(self.stream_mut()?, "PING".to_owned())
            .await
            .map_err(SessionFailure::Reconnect)
    }

    async fn request_snapshot(&mut self, _symbols: &[String]) -> Result<(), SessionFailure> {
        // Polymarket has no get_snapshot command. Returning Reconnect makes the
        // supervisor drop this socket and resubscribe with `initial_dump`, which
        // is the protocol's only way to obtain a fresh full book.
        Err(SessionFailure::Reconnect(anyhow::anyhow!(
            "Polymarket has no snapshot request; reconnecting for a fresh initial dump"
        )))
    }

    async fn close(&mut self) {
        if let Some(stream) = self.stream.take() {
            let _ = ws::close(stream).await;
        }
        self.subscribed_assets.clear();
        self.initial_sent = false;
        self.pong_pending = false;
    }
}
