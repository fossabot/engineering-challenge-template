//! Kalshi market discovery.
//!
//! Kalshi's `/markets` endpoint is paginated by an opaque cursor. We request
//! open markets while asking the server to exclude multivariate events, then
//! apply the same filter locally as a defensive check. Discovery is deliberately
//! independent of websocket subscriptions so a transient REST failure never
//! tears down live books.

use std::collections::BTreeSet;

use anyhow::{Context, bail};
use serde::Deserialize;
use serde_json::Value;
use tokio::time::sleep;
use tracing::{debug, warn};
use url::Url;

use crate::{
    exchange::kalshi::KalshiExchange,
    fixed::parse_micros,
    model::{MarketInfo, PriceRange},
    time::parse_rfc3339_us,
};

/// Convert one discovery page, isolating malformed records.
///
/// A record that fails conversion is counted and skipped with a warning so a
/// single bad entry cannot abort reconciliation for every valid market. If
/// every record on a non-empty page fails, the response is untrustworthy and
/// an error is returned so the round is retried instead of silently
/// reconciling a partial set. Records filtered out by design (MVE, inactive)
/// are not failures.
fn convert_kalshi_page(
    page: Vec<RawMarket>,
    max_markets: usize,
    markets: &mut Vec<MarketInfo>,
    skipped: &mut u64,
) -> anyhow::Result<()> {
    let mut processed = 0usize;
    let mut page_skipped = 0usize;
    for raw in page {
        match raw.into_market() {
            Ok(Some(info)) => {
                processed += 1;
                markets.push(info);
                if max_markets != 0 && markets.len() >= max_markets {
                    break;
                }
            }
            // Filtered by design (MVE, inactive): processed, not a failure.
            Ok(None) => processed += 1,
            Err(error) => {
                page_skipped += 1;
                warn!(error = %format!("{error:#}"), "skipping malformed Kalshi market record");
            }
        }
    }
    *skipped += page_skipped as u64;
    if processed == 0 && page_skipped > 0 {
        bail!(
            "every convertible Kalshi market on a discovery page ({page_skipped} records) \
             failed to decode"
        );
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct MarketsPage {
    #[serde(default)]
    markets: Vec<RawMarket>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawMarket {
    ticker: String,
    #[serde(default)]
    event_ticker: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    yes_sub_title: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    market_type: Option<String>,
    #[serde(default)]
    close_time: Option<String>,
    #[serde(default)]
    mve_collection_ticker: Option<String>,
    #[serde(default)]
    mve_selected_legs: Option<Value>,
    #[serde(default)]
    price_ranges: Option<Vec<RawPriceRange>>,
    #[serde(default)]
    price_level_structure: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawPriceRange {
    #[serde(default)]
    start: Option<String>,
    #[serde(default)]
    end: Option<String>,
    #[serde(default)]
    step: Option<String>,
}

impl KalshiExchange {
    pub(crate) async fn fetch_markets(
        &self,
        max_markets: usize,
    ) -> anyhow::Result<Vec<MarketInfo>> {
        let base = Url::parse(&format!(
            "{}/",
            self.settings()
                .environment
                .rest_base()
                .trim_end_matches('/')
        ))
        .context("invalid Kalshi REST base URL")?;
        let mut cursor: Option<String> = None;
        let mut seen_cursors = BTreeSet::new();
        let mut markets = Vec::new();
        let mut pages = 0usize;
        let mut skipped = 0u64;
        loop {
            if max_markets != 0 && markets.len() >= max_markets {
                break;
            }
            let mut url = base
                .join("markets")
                .context("building Kalshi markets URL")?;
            {
                let mut query = url.query_pairs_mut();
                query
                    .append_pair("status", "open")
                    .append_pair("mve_filter", "exclude")
                    .append_pair("limit", &self.settings().discovery_page_size.to_string());
                if let Some(cursor) = &cursor {
                    query.append_pair("cursor", cursor);
                }
            }
            let page = self.fetch_page(&url).await?;
            pages = pages.saturating_add(1);
            debug!(
                page = pages,
                count = page.markets.len(),
                cursor = page.cursor.as_deref().unwrap_or(""),
                "Kalshi discovery page"
            );
            let page_cursor = page.cursor;
            convert_kalshi_page(page.markets, max_markets, &mut markets, &mut skipped)?;
            let Some(next) = page_cursor.filter(|cursor| !cursor.is_empty()) else {
                break;
            };
            if !seen_cursors.insert(next.clone()) {
                // A repeated cursor means the page set is incomplete. Returning
                // it would make every symbol outside the partial set look
                // removed, so fail the whole discovery round instead.
                bail!("Kalshi discovery cursor repeated; refusing partial market set");
            }
            cursor = Some(next);
            if self.settings().discovery_page_delay_ms > 0 {
                sleep(std::time::Duration::from_millis(
                    self.settings().discovery_page_delay_ms,
                ))
                .await;
            }
        }
        markets.sort_by(|left, right| left.native_symbol.cmp(&right.native_symbol));
        if let Some(pair) = markets
            .windows(2)
            .find(|pair| pair[0].native_symbol == pair[1].native_symbol)
        {
            bail!(
                "Kalshi discovery returned duplicate ticker `{}`",
                pair[0].native_symbol
            );
        }
        if skipped > 0 {
            warn!(
                skipped,
                "Kalshi discovery skipped malformed market records this round"
            );
        }
        Ok(markets)
    }

    async fn fetch_page(&self, url: &Url) -> anyhow::Result<MarketsPage> {
        let mut last_error = None;
        for attempt in 0..5u32 {
            let mut request = self.client().get(url.clone());
            if let Some(auth) = self.auth() {
                let headers = auth
                    .headers("GET", url.path())
                    .context("signing Kalshi discovery request")?;
                request = headers.apply_rest(request);
            }
            match request.send().await {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        let body = response
                            .text()
                            .await
                            .context("reading Kalshi discovery response")?;
                        return serde_json::from_str(&body)
                            .with_context(|| format!("decoding Kalshi discovery page from {url}"));
                    }
                    let retryable = status.as_u16() == 429 || status.is_server_error();
                    let body = response.text().await.unwrap_or_default();
                    let message = format!(
                        "Kalshi discovery HTTP {}: {}",
                        status.as_u16(),
                        body.chars().take(240).collect::<String>()
                    );
                    if !retryable {
                        bail!("{message}");
                    }
                    last_error = Some(message);
                }
                Err(error) => {
                    last_error = Some(format!("Kalshi discovery request failed: {error}"));
                }
            }
            let backoff = 500u64.saturating_mul(1u64 << attempt.min(4));
            sleep(std::time::Duration::from_millis(backoff)).await;
        }
        Err(anyhow::anyhow!(
            "{}",
            last_error.unwrap_or_else(|| "Kalshi discovery failed".to_owned())
        ))
    }
}

impl RawMarket {
    fn into_market(self) -> anyhow::Result<Option<MarketInfo>> {
        if self.market_type.as_deref() == Some("mve")
            || self
                .mve_collection_ticker
                .as_deref()
                .is_some_and(|value| !value.is_empty())
            || self
                .mve_selected_legs
                .as_ref()
                .is_some_and(|legs| match legs {
                    Value::Array(legs) => !legs.is_empty(),
                    Value::Null => false,
                    _ => true,
                })
        {
            return Ok(None);
        }
        let status = self
            .status
            .as_deref()
            .context("Kalshi market discovery entry is missing status")?;
        if status != "active" {
            return Ok(None);
        }
        if self.ticker.trim().is_empty() {
            bail!("Kalshi market discovery entry has an empty ticker");
        }
        let title = self
            .title
            .filter(|title| !title.trim().is_empty())
            .or_else(|| self.yes_sub_title.filter(|title| !title.trim().is_empty()));
        let close_time_us = self
            .close_time
            .as_deref()
            .map(parse_rfc3339_us)
            .transpose()
            .context("invalid Kalshi close_time")?;
        let price_ranges = parse_price_ranges(self.price_ranges.as_deref())?;
        let tick_size_us = price_ranges.iter().map(|range| range.step_us).min();
        let info = MarketInfo {
            exchange: "kalshi".to_owned(),
            native_symbol: self.ticker.clone(),
            symbol: self.ticker,
            title,
            outcome: None,
            exchange_market_id: self.event_ticker,
            close_time_us,
            tick_size_us,
            price_ranges,
            price_level_structure: self.price_level_structure,
        };
        info.validate()?;
        Ok(Some(info))
    }
}

fn parse_price_ranges(ranges: Option<&[RawPriceRange]>) -> anyhow::Result<Vec<PriceRange>> {
    let Some(ranges) = ranges else {
        // Older API responses may omit price_ranges. Do not guess a tick size
        // from the human-readable structure label.
        return Ok(Vec::new());
    };
    if ranges.is_empty() {
        bail!("Kalshi price_ranges is present but empty");
    }
    ranges
        .iter()
        .map(|range| {
            let start = range
                .start
                .as_deref()
                .context("Kalshi price range missing start")?;
            let end = range
                .end
                .as_deref()
                .context("Kalshi price range missing end")?;
            let step = range
                .step
                .as_deref()
                .context("Kalshi price range missing step")?;
            let range = PriceRange {
                start_us: parse_micros(start).context("invalid Kalshi price range start")?,
                end_us: parse_micros(end).context("invalid Kalshi price range end")?,
                step_us: parse_micros(step).context("invalid Kalshi price range step")?,
            };
            range.validate()?;
            Ok(range)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw() -> RawMarket {
        RawMarket {
            ticker: "KXTEST-26-T1".to_owned(),
            event_ticker: Some("KXTEST-26".to_owned()),
            title: Some("Test market".to_owned()),
            yes_sub_title: None,
            status: Some("active".to_owned()),
            market_type: Some("binary".to_owned()),
            close_time: Some("2026-10-24T00:00:00Z".to_owned()),
            mve_collection_ticker: None,
            mve_selected_legs: None,
            price_ranges: Some(vec![RawPriceRange {
                start: Some("0.00".to_owned()),
                end: Some("1.00".to_owned()),
                step: Some("0.0100".to_owned()),
            }]),
            price_level_structure: Some("linear_cent".to_owned()),
        }
    }

    #[test]
    fn converts_active_non_mve_market() {
        let info = raw().into_market().unwrap().unwrap();
        assert_eq!(info.symbol, "KXTEST-26-T1");
        assert_eq!(info.tick_size_us, Some(10_000));
        assert_eq!(info.exchange_market_id.as_deref(), Some("KXTEST-26"));
    }

    #[test]
    fn rejects_inactive_and_mve_markets() {
        let mut market = raw();
        market.status = Some("closed".to_owned());
        assert!(market.into_market().unwrap().is_none());

        let mut market = raw();
        market.mve_collection_ticker = Some("KXMVECROSS".to_owned());
        assert!(market.into_market().unwrap().is_none());

        let mut market = raw();
        market.mve_selected_legs = Some(serde_json::json!([{"market":"X"}]));
        assert!(market.into_market().unwrap().is_none());
    }

    #[test]
    fn preserves_tapered_price_ranges_instead_of_label_fallback() {
        let mut market = raw();
        market.price_level_structure = Some("tapered_deci_cent".to_owned());
        market.price_ranges = Some(vec![
            RawPriceRange {
                start: Some("0.00".to_owned()),
                end: Some("0.10".to_owned()),
                step: Some("0.001".to_owned()),
            },
            RawPriceRange {
                start: Some("0.10".to_owned()),
                end: Some("0.90".to_owned()),
                step: Some("0.01".to_owned()),
            },
            RawPriceRange {
                start: Some("0.90".to_owned()),
                end: Some("1.00".to_owned()),
                step: Some("0.001".to_owned()),
            },
        ]);
        let info = market.into_market().unwrap().unwrap();
        assert_eq!(info.tick_size_us, Some(1_000));
        assert_eq!(info.price_ranges.len(), 3);
        assert_eq!(info.price_ranges[1].step_us, 10_000);
        assert_eq!(
            info.price_level_structure.as_deref(),
            Some("tapered_deci_cent")
        );
    }

    #[test]
    fn does_not_guess_tick_size_without_price_ranges() {
        let mut market = raw();
        market.price_ranges = None;
        market.price_level_structure = Some("center_whole_edge_half_cent".to_owned());
        let info = market.into_market().unwrap().unwrap();
        assert_eq!(info.tick_size_us, None);
        assert!(info.price_ranges.is_empty());
    }

    #[test]
    fn rejects_malformed_price_ranges() {
        let mut market = raw();
        market.price_ranges = Some(vec![RawPriceRange {
            start: Some("0.00".to_owned()),
            end: Some("1.00".to_owned()),
            step: None,
        }]);
        assert!(market.into_market().is_err());
    }

    #[test]
    fn one_malformed_record_does_not_abort_the_page() {
        let mut valid = raw();
        valid.ticker = "KXVALID-26-T1".to_owned();
        let mut malformed = raw();
        malformed.ticker = String::new();
        let mut markets = Vec::new();
        let mut skipped = 0u64;
        convert_kalshi_page(vec![malformed, valid], 0, &mut markets, &mut skipped).unwrap();
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].symbol, "KXVALID-26-T1");
        assert_eq!(skipped, 1);
    }

    #[test]
    fn filtered_records_are_not_failures() {
        let mut inactive = raw();
        inactive.status = Some("settled".to_owned());
        let mut markets = Vec::new();
        let mut skipped = 0u64;
        convert_kalshi_page(vec![inactive], 0, &mut markets, &mut skipped).unwrap();
        assert!(markets.is_empty());
        assert_eq!(skipped, 0);
    }

    #[test]
    fn a_page_of_only_malformed_records_fails_the_round() {
        let mut malformed_a = raw();
        malformed_a.status = None;
        let mut malformed_b = raw();
        malformed_b.price_ranges = Some(vec![RawPriceRange {
            start: None,
            end: None,
            step: None,
        }]);
        let mut markets = Vec::new();
        let mut skipped = 0u64;
        let error = convert_kalshi_page(
            vec![malformed_a, malformed_b],
            0,
            &mut markets,
            &mut skipped,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("failed to decode"),
            "unexpected error: {error:#}"
        );
        assert!(markets.is_empty());
        assert_eq!(skipped, 2);
    }
}
