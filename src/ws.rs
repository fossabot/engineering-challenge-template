//! Small helpers shared by the exchange websocket sessions.

use anyhow::{Context, bail};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{self, Message},
};

pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// One application payload read from a websocket.
#[derive(Debug)]
pub enum WsPayload {
    Text(String),
    /// Control/heartbeat frame; the connection is still alive.
    Idle,
}

/// Open a websocket, optionally adding handshake headers.
pub async fn connect(
    url: &str,
    headers: &[(http::HeaderName, http::HeaderValue)],
) -> anyhow::Result<WsStream> {
    let mut request = url
        .into_client_request()
        .with_context(|| format!("building websocket request for {url}"))?;
    for (name, value) in headers {
        request.headers_mut().insert(name, value.clone());
    }
    let (stream, response) = connect_async(request)
        .await
        .with_context(|| format!("websocket handshake with {url}"))?;
    let status = response.status();
    if status != http::StatusCode::SWITCHING_PROTOCOLS {
        bail!("websocket handshake returned HTTP {status}");
    }
    Ok(stream)
}

/// Extract an HTTP status from a failed websocket handshake, if the underlying
/// tungstenite error carried one. Authentication failures can then be treated as
/// fatal instead of being retried forever.
pub fn http_status(error: &anyhow::Error) -> Option<u16> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<tungstenite::Error>()
            .and_then(|error| match error {
                tungstenite::Error::Http(response) => Some(response.status().as_u16()),
                _ => None,
            })
    })
}

/// Read the next data payload, transparently answering control-frame pings.
pub async fn next_payload(stream: &mut WsStream) -> anyhow::Result<WsPayload> {
    loop {
        let message = stream.next().await.context("websocket stream ended")??;
        match message {
            Message::Text(text) => return Ok(WsPayload::Text(text.to_string())),
            Message::Binary(bytes) => {
                let text = String::from_utf8(bytes.to_vec())
                    .context("binary websocket frame was not UTF-8 JSON")?;
                return Ok(WsPayload::Text(text));
            }
            Message::Ping(payload) => {
                stream.send(Message::Pong(payload)).await?;
                return Ok(WsPayload::Idle);
            }
            Message::Pong(_) => return Ok(WsPayload::Idle),
            Message::Close(frame) => {
                let detail = frame.map_or_else(
                    || "without frame".to_owned(),
                    |f| format!("code={} reason={}", f.code, f.reason),
                );
                bail!("websocket closed by peer ({detail})");
            }
            Message::Frame(_) => {}
        }
    }
}

pub async fn send_text(stream: &mut WsStream, text: String) -> anyhow::Result<()> {
    stream.send(Message::Text(text.into())).await?;
    Ok(())
}

pub async fn send_ping(stream: &mut WsStream) -> anyhow::Result<()> {
    stream.send(Message::Ping(Vec::new().into())).await?;
    Ok(())
}

pub async fn close(mut stream: WsStream) -> anyhow::Result<()> {
    // Best effort: a peer that already vanished must not turn shutdown into an
    // error path.
    let _ = stream.close(None).await;
    Ok(())
}
