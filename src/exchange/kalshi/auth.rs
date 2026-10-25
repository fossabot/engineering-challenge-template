//! Kalshi request signing.
//!
//! Kalshi signs `timestamp_ms + METHOD + path` (query string excluded) with
//! either an Ed25519 PKCS#8 key or a 2048-bit RSA key using RSASSA-PSS/SHA-256.
//! The same headers are used for REST and the websocket handshake.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use ed25519_dalek::{Signer as _, SigningKey, pkcs8::DecodePrivateKey as _};
use http::{HeaderName, HeaderValue};
use rand::rngs::OsRng;
use rsa::{
    RsaPrivateKey,
    pkcs1::DecodeRsaPrivateKey as _,
    pkcs8::DecodePrivateKey as _,
    pss::SigningKey as RsaPssSigningKey,
    signature::{RandomizedSigner as _, SignatureEncoding as _},
};
use sha2::Sha256;

use crate::config::KalshiSettings;

pub const HEADER_KEY: HeaderName = HeaderName::from_static("kalshi-access-key");
pub const HEADER_SIGNATURE: HeaderName = HeaderName::from_static("kalshi-access-signature");
pub const HEADER_TIMESTAMP: HeaderName = HeaderName::from_static("kalshi-access-timestamp");

/// Private signing material. `Debug` is not implemented so key bytes are
/// never logged.
enum KeyMaterial {
    Ed25519(Box<SigningKey>),
    Rsa(Box<RsaPssSigningKey<Sha256>>),
}

/// Kalshi API credentials.
pub struct KalshiAuth {
    key_id: String,
    key: KeyMaterial,
}

impl KalshiAuth {
    /// Build credentials from the configured key id and PEM path, or from
    /// `KALSHI_PRIVATE_KEY_PEM` when no path is configured.
    pub fn from_settings(settings: &KalshiSettings) -> anyhow::Result<Self> {
        let key_id = settings.key_id.trim();
        if key_id.is_empty() {
            bail!(
                "Kalshi key id is missing; set kalshi.key_id, KALSHI_API_KEY_ID, \
                 or --kalshi-key-id"
            );
        }
        let pem = if let Some(path) = &settings.private_key_path {
            std::fs::read_to_string(path)
                .with_context(|| format!("reading Kalshi private key `{}`", path.display()))?
        } else if let Ok(pem) = std::env::var("KALSHI_PRIVATE_KEY_PEM") {
            pem
        } else {
            bail!(
                "Kalshi private key is missing; set kalshi.private_key_path, \
                 KALSHI_PRIVATE_KEY_PATH, or KALSHI_PRIVATE_KEY_PEM"
            );
        };
        Self::from_pem(key_id.to_owned(), &pem)
    }

    /// Parse a PEM private key. Ed25519 PKCS#8, RSA PKCS#8, and RSA PKCS#1 are
    /// supported; Kalshi's dashboard emits one of these three forms.
    pub fn from_pem(key_id: String, pem: &str) -> anyhow::Result<Self> {
        if key_id.trim().is_empty() {
            bail!("Kalshi key id is empty");
        }
        if pem.trim().is_empty() {
            bail!("Kalshi private key PEM is empty");
        }
        if let Ok(key) = SigningKey::from_pkcs8_pem(pem) {
            return Ok(Self {
                key_id,
                key: KeyMaterial::Ed25519(Box::new(key)),
            });
        }
        if let Ok(key) = RsaPrivateKey::from_pkcs8_pem(pem) {
            return Ok(Self {
                key_id,
                key: KeyMaterial::Rsa(Box::new(RsaPssSigningKey::new(key))),
            });
        }
        if let Ok(key) = RsaPrivateKey::from_pkcs1_pem(pem) {
            return Ok(Self {
                key_id,
                key: KeyMaterial::Rsa(Box::new(RsaPssSigningKey::new(key))),
            });
        }
        bail!(
            "unsupported Kalshi private key PEM; expected Ed25519 PKCS#8, RSA PKCS#8, \
             or RSA PKCS#1"
        )
    }

    /// Build the three authentication headers for a REST or websocket request.
    /// `path` must include the API prefix and exclude the query string.
    pub fn headers(&self, method: &str, path: &str) -> anyhow::Result<AuthHeaders> {
        let timestamp = unix_millis()?;
        self.headers_at(method, path, timestamp)
    }

    /// Deterministic variant used by tests and clock-sensitive callers.
    pub fn headers_at(
        &self,
        method: &str,
        path: &str,
        timestamp_ms: i64,
    ) -> anyhow::Result<AuthHeaders> {
        anyhow::ensure!(!method.is_empty(), "empty HTTP method");
        anyhow::ensure!(
            path.starts_with('/'),
            "Kalshi signing path must start with '/'"
        );
        anyhow::ensure!(
            !path.contains('?'),
            "Kalshi signing path must exclude query"
        );
        let timestamp = timestamp_ms.to_string();
        let message = format!("{timestamp}{method}{path}");
        let signature = match &self.key {
            KeyMaterial::Ed25519(key) => {
                let signature = key.sign(message.as_bytes());
                BASE64.encode(signature.to_bytes())
            }
            KeyMaterial::Rsa(key) => {
                let signature = key
                    .try_sign_with_rng(&mut OsRng, message.as_bytes())
                    .context("signing Kalshi request with RSA-PSS")?;
                BASE64.encode(signature.to_vec())
            }
        };
        let header = |value: &str, name: &'static str| -> anyhow::Result<HeaderValue> {
            HeaderValue::from_str(value).with_context(|| format!("invalid {name} header value"))
        };
        Ok(AuthHeaders {
            key: header(&self.key_id, "Kalshi key id")?,
            signature: header(&signature, "Kalshi signature")?,
            timestamp: header(&timestamp, "Kalshi timestamp")?,
        })
    }
}

/// Owned header values ready to attach to a request.
pub struct AuthHeaders {
    pub key: HeaderValue,
    pub signature: HeaderValue,
    pub timestamp: HeaderValue,
}

impl AuthHeaders {
    pub fn apply_rest(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        builder
            .header(HEADER_KEY, self.key.clone())
            .header(HEADER_SIGNATURE, self.signature.clone())
            .header(HEADER_TIMESTAMP, self.timestamp.clone())
    }
}

fn unix_millis() -> anyhow::Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock predates the Unix epoch")?;
    i64::try_from(elapsed.as_millis()).context("timestamp exceeds i64 milliseconds")
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Verifier as _, pkcs8::EncodePrivateKey as _};
    use rsa::{
        RsaPublicKey, pkcs1::EncodeRsaPrivateKey as _, pkcs8::EncodePrivateKey as _,
        pss::VerifyingKey as RsaPssVerifyingKey, signature::Verifier as _,
    };
    fn ed25519_pem() -> String {
        let key = SigningKey::from_bytes(&[7u8; 32]);
        key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap()
            .to_string()
    }

    #[test]
    fn ed25519_signatures_verify() {
        let pem = ed25519_pem();
        let auth = KalshiAuth::from_pem("key-id".to_owned(), &pem).unwrap();
        let headers = auth
            .headers_at("GET", "/trade-api/v2/markets", 1_700_000_000_000)
            .unwrap();
        assert_eq!(headers.timestamp, "1700000000000");
        assert_eq!(headers.key, "key-id");
        let signature = BASE64.decode(headers.signature.as_bytes()).unwrap();
        let signature = ed25519_dalek::Signature::from_slice(&signature).unwrap();
        let key = SigningKey::from_pkcs8_pem(&pem).unwrap();
        key.verifying_key()
            .verify(b"1700000000000GET/trade-api/v2/markets", &signature)
            .unwrap();
    }

    #[test]
    fn rsa_pkcs8_and_pkcs1_signatures_verify() {
        let key = RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
        let public = RsaPublicKey::from(&key);
        let verifying = RsaPssVerifyingKey::<Sha256>::new(public);
        let message = b"1700000000000GET/trade-api/v2/markets";

        for pem in [
            key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
                .unwrap()
                .to_string(),
            key.to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
                .unwrap()
                .to_string(),
        ] {
            let auth = KalshiAuth::from_pem("key-id".to_owned(), &pem).unwrap();
            let headers = auth
                .headers_at("GET", "/trade-api/v2/markets", 1_700_000_000_000)
                .unwrap();
            let signature = BASE64.decode(headers.signature.as_bytes()).unwrap();
            let signature = rsa::pss::Signature::try_from(signature.as_slice()).unwrap();
            verifying.verify(message, &signature).unwrap();
        }
    }

    #[test]
    fn rejects_missing_or_invalid_keys() {
        assert!(KalshiAuth::from_pem(String::new(), "x").is_err());
        assert!(KalshiAuth::from_pem("id".to_owned(), "").is_err());
        assert!(KalshiAuth::from_pem("id".to_owned(), "not a pem").is_err());
    }
}
