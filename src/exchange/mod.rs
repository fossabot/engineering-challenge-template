//! Exchange adapters and their common discovery/session interface.

pub mod kalshi;
pub mod polymarket;

use std::sync::Arc;

use async_trait::async_trait;

use crate::{model::MarketInfo, session::SessionFactory};

/// One exchange's discovery and websocket implementation.
#[async_trait]
pub trait Exchange: Send + Sync + 'static {
    /// Stable lowercase exchange name used in SBE symbols and reports.
    fn name(&self) -> &'static str;

    /// Human-readable environment (`production`, `demo`, or `public`).
    fn environment(&self) -> &'static str;

    /// Fetch the current live, non-MVE market set. Implementations cap the
    /// result when `max_markets` is non-zero.
    async fn discover_markets(&self, max_markets: usize) -> anyhow::Result<Vec<MarketInfo>>;

    /// Build the factory used by websocket source workers.
    fn session_factory(self: Arc<Self>) -> Arc<dyn SessionFactory>;
}

pub use kalshi::{KalshiExchange, KalshiSessionFactory};
pub use polymarket::{PolymarketExchange, PolymarketSessionFactory};
