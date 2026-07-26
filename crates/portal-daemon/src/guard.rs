//! Remote Steam Guard code entry.
//!
//! When a bot's Steam login is challenged for a Guard code and no TOTP
//! shared secret is configured, the login parks on a [`GuardGate`] while a
//! loopback HTTP listener (`GUARD_ADDR`) serves a small code-entry page.
//! Operator access happens via Tailscale Serve proxying the box's ts.net
//! name to this loopback port — the daemon itself never binds a routable
//! interface, same invariant as the `/metrics` listener.
//!
//! Flow: the login side calls [`GuardGate::wait_for_code`], which arms the
//! gate and sets `<prefix>_awaiting_guard_code` to 1 (alert on this). The
//! page shows the pending prompt and POSTs the code, fulfilling the wait.
//! On timeout the gate disarms and the login aborts into its caller's
//! reconnect/backoff loop, which re-arms on the next attempt — a missed
//! code is never fatal, just another cycle.

use http_body_util::{BodyExt, Full, Limited};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{header, Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::info;

use crate::unix_now;

/// Codes are 5 chars today; allow headroom without accepting junk.
const MAX_CODE_LEN: usize = 16;
/// POST bodies are a single short form field.
const MAX_BODY_BYTES: usize = 4096;

/// Shared state between a parked Steam login and the code-entry page.
pub struct GuardGate {
    pending: Mutex<Option<PendingPrompt>>,
    notice: Mutex<Option<String>>,
    awaiting_gauge: String,
    submissions_counter: String,
}

struct PendingPrompt {
    account: String,
    prompt: String,
    since: u64,
    tx: oneshot::Sender<String>,
}

/// Snapshot of the pending prompt, for the status page.
pub struct GuardPromptStatus {
    pub account: String,
    pub prompt: String,
    pub waiting_secs: u64,
}

/// Result of submitting a code through the page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardSubmitOutcome {
    /// Delivered to the waiting login.
    Accepted,
    /// No login is currently waiting for a code.
    NoPrompt,
    /// Not a plausible Guard code (empty, too long, non-alphanumeric).
    Invalid,
}

impl GuardSubmitOutcome {
    fn label(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::NoPrompt => "no-prompt",
            Self::Invalid => "invalid",
        }
    }
}

impl GuardGate {
    /// Create the gate. `metric_prefix` scopes the metrics, e.g.
    /// `cs2_enricher` → `cs2_enricher_awaiting_guard_code`.
    #[must_use]
    pub fn new(metric_prefix: &str) -> Arc<Self> {
        Arc::new(Self {
            pending: Mutex::new(None),
            notice: Mutex::new(None),
            awaiting_gauge: format!("{metric_prefix}_awaiting_guard_code"),
            submissions_counter: format!("{metric_prefix}_guard_code_submissions_total"),
        })
    }

    /// Park until the operator submits a code or `timeout` elapses.
    ///
    /// `prompt` is the human-readable challenge (e.g. "email: enter the
    /// code sent to a***@example.com") shown on the page. Returns `None`
    /// on timeout; a prompt armed while another is pending replaces it
    /// (the replaced waiter resolves as timed out).
    pub async fn wait_for_code(
        &self,
        account: &str,
        prompt: &str,
        timeout: Duration,
    ) -> Option<String> {
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock().expect("guard gate lock");
            *pending = Some(PendingPrompt {
                account: account.to_string(),
                prompt: prompt.to_string(),
                since: unix_now(),
                tx,
            });
        }
        metrics::gauge!(self.awaiting_gauge.clone()).set(1.0);

        let code = tokio::time::timeout(timeout, rx)
            .await
            .ok()
            .and_then(Result::ok);

        {
            let mut pending = self.pending.lock().expect("guard gate lock");
            *pending = None;
        }
        metrics::gauge!(self.awaiting_gauge.clone()).set(0.0);
        code
    }

    /// Deliver a code to the waiting login. Trims and uppercases first —
    /// Steam Guard codes are case-insensitive alphanumerics.
    pub fn submit(&self, raw: &str) -> GuardSubmitOutcome {
        let code = raw.trim().to_ascii_uppercase();
        let outcome = if code.is_empty()
            || code.len() > MAX_CODE_LEN
            || !code.bytes().all(|b| b.is_ascii_alphanumeric())
        {
            GuardSubmitOutcome::Invalid
        } else {
            let taken = self.pending.lock().expect("guard gate lock").take();
            match taken {
                // send() fails if the waiter timed out between our take and
                // its cleanup — the prompt is gone either way.
                Some(p) => match p.tx.send(code) {
                    Ok(()) => GuardSubmitOutcome::Accepted,
                    Err(_) => GuardSubmitOutcome::NoPrompt,
                },
                None => GuardSubmitOutcome::NoPrompt,
            }
        };
        metrics::counter!(self.submissions_counter.clone(), "outcome" => outcome.label())
            .increment(1);
        outcome
    }

    /// Current prompt, if a login is parked waiting for a code.
    #[must_use]
    pub fn status(&self) -> Option<GuardPromptStatus> {
        let pending = self.pending.lock().expect("guard gate lock");
        pending.as_ref().map(|p| GuardPromptStatus {
            account: p.account.clone(),
            prompt: p.prompt.clone(),
            waiting_secs: unix_now().saturating_sub(p.since),
        })
    }

    /// Set the operator-facing notice shown on the page (progress info,
    /// final instructions). Persists until replaced or cleared.
    pub fn set_notice(&self, notice: impl Into<String>) {
        *self.notice.lock().expect("guard gate lock") = Some(notice.into());
    }

    /// Remove the notice from the page.
    pub fn clear_notice(&self) {
        *self.notice.lock().expect("guard gate lock") = None;
    }

    /// The current notice, if one is set.
    #[must_use]
    pub fn notice(&self) -> Option<String> {
        self.notice.lock().expect("guard gate lock").clone()
    }
}

/// Spawn the loopback code-entry listener when `GUARD_ADDR` is set
/// (empty/unset = disabled). Returns whether the page is enabled.
///
/// # Panics
/// Panics on an unparseable address — same fail-loudly policy as
/// `METRICS_ADDR`.
pub fn start_guard_from_env(gate: Arc<GuardGate>) -> bool {
    let Some(addr) = std::env::var("GUARD_ADDR")
        .ok()
        .filter(|v| !v.trim().is_empty())
    else {
        info!("GUARD_ADDR not set — Steam Guard code-entry page disabled");
        return false;
    };
    let addr: SocketAddr = addr
        .parse()
        .unwrap_or_else(|e| panic!("invalid GUARD_ADDR {addr:?}: {e}"));
    tokio::spawn(serve_guard(addr, gate));
    true
}

async fn serve_guard(addr: SocketAddr, gate: Arc<GuardGate>) {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind guard listener {addr}: {e}"));
    info!("Steam Guard code-entry page listening on http://{addr}");
    serve_guard_on(listener, gate).await;
}

async fn serve_guard_on(listener: tokio::net::TcpListener, gate: Arc<GuardGate>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let gate = Arc::clone(&gate);
        tokio::spawn(async move {
            let service = service_fn(move |req: Request<Incoming>| {
                let gate = Arc::clone(&gate);
                async move { Ok::<_, std::convert::Infallible>(handle_guard(req, &gate).await) }
            });
            let _ = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
    }
}

async fn handle_guard(req: Request<Incoming>, gate: &GuardGate) -> Response<Full<Bytes>> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => html_response(StatusCode::OK, &render_page(gate)),
        (&Method::GET, "/status") => {
            let body = match gate.status() {
                Some(s) => format!(
                    "{{\"pending\":true,\"account\":\"{}\",\"prompt\":\"{}\",\"waiting_secs\":{}}}\n",
                    json_escape(&s.account),
                    json_escape(&s.prompt),
                    s.waiting_secs
                ),
                None => "{\"pending\":false}\n".to_string(),
            };
            Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(Full::new(Bytes::from(body)))
                .expect("status response")
        }
        (&Method::POST, "/code") => {
            let wants_html = req
                .headers()
                .get(header::ACCEPT)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.contains("text/html"));
            let body = match Limited::new(req.into_body(), MAX_BODY_BYTES)
                .collect()
                .await
            {
                Ok(collected) => collected.to_bytes(),
                Err(_) => return text_response(StatusCode::BAD_REQUEST, "unreadable body\n"),
            };
            let body = String::from_utf8_lossy(&body);
            let Some(code) = extract_code(&body) else {
                return text_response(StatusCode::BAD_REQUEST, "missing code\n");
            };
            match gate.submit(&code) {
                GuardSubmitOutcome::Accepted if wants_html => Response::builder()
                    .status(StatusCode::SEE_OTHER)
                    .header(header::LOCATION, "/")
                    .body(Full::new(Bytes::new()))
                    .expect("redirect response"),
                GuardSubmitOutcome::Accepted => text_response(StatusCode::OK, "accepted\n"),
                GuardSubmitOutcome::NoPrompt => {
                    text_response(StatusCode::CONFLICT, "no Steam Guard prompt pending\n")
                }
                GuardSubmitOutcome::Invalid => {
                    text_response(StatusCode::BAD_REQUEST, "not a valid Steam Guard code\n")
                }
            }
        }
        _ => text_response(StatusCode::NOT_FOUND, "not found\n"),
    }
}

/// Pull the code out of a POST body: an HTML form (`code=XYZ12`) or a bare
/// code (`curl -d 'XYZ12'` degenerates to form syntax without a `code` key,
/// so treat a body with no `code=` pair as the code itself).
fn extract_code(body: &str) -> Option<String> {
    for pair in body.split('&') {
        if let Some(value) = pair.strip_prefix("code=") {
            return Some(percent_decode(value));
        }
    }
    let bare = body.trim();
    (!bare.is_empty() && !bare.contains('=')).then(|| percent_decode(bare))
}

/// Minimal `application/x-www-form-urlencoded` value decoding. Guard codes
/// are plain alphanumerics, so this only needs to not mangle them.
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' => {
                let hex = bytes
                    .get(i + 1..i + 3)
                    .and_then(|h| u8::from_str_radix(std::str::from_utf8(h).ok()?, 16).ok());
                match hex {
                    Some(b) => {
                        out.push(b);
                        i += 2;
                    }
                    None => out.push(b'%'),
                }
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn text_response(status: StatusCode, body: &'static str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Full::new(Bytes::from_static(body.as_bytes())))
        .expect("text response")
}

fn html_response(status: StatusCode, body: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .body(Full::new(Bytes::from(body.to_string())))
        .expect("html response")
}

fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

const PAGE_STYLE: &str = "\
:root{color-scheme:light dark;font-family:system-ui,sans-serif}\
body{display:flex;justify-content:center;margin:15vh 1rem 0}\
main{max-width:26rem;width:100%}\
h1{font-size:1.1rem}\
.card{border:1px solid color-mix(in srgb,currentColor 25%,transparent);\
border-radius:8px;padding:1rem 1.25rem}\
.muted{opacity:.7;font-size:.9rem}\
.notice{white-space:pre-wrap;overflow-wrap:break-word;margin-bottom:1rem}\
input[name=code]{font-size:1.4rem;letter-spacing:.3em;text-transform:uppercase;\
width:9ch;text-align:center;padding:.3rem}\
button{font-size:1rem;padding:.35rem 1rem;margin-left:.5rem}";

fn render_page(gate: &GuardGate) -> String {
    let notice = gate
        .notice()
        .map(|n| format!("<div class=\"card notice\">{}</div>", html_escape(&n)))
        .unwrap_or_default();
    match gate.status() {
        Some(s) => format!(
            "<!doctype html><html><head><meta charset=\"utf-8\">\
             <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
             <title>Steam Guard</title><style>{PAGE_STYLE}</style></head><body><main>\
             <h1>Steam Guard code required</h1>{notice}<div class=\"card\">\
             <p>Account <strong>{account}</strong> &mdash; waiting {waiting}s</p>\
             <p class=\"muted\">{prompt}</p>\
             <form method=\"post\" action=\"/code\">\
             <input name=\"code\" autofocus autocomplete=\"one-time-code\" \
             maxlength=\"{max_len}\" pattern=\"[A-Za-z0-9]+\" required>\
             <button type=\"submit\">Submit</button></form>\
             <p class=\"muted\">Codes rotate every 30&nbsp;s &mdash; use the current one. \
             A wrong or late code just retries on the next login attempt.</p>\
             </div></main></body></html>",
            account = html_escape(&s.account),
            waiting = s.waiting_secs,
            prompt = html_escape(&s.prompt),
            max_len = MAX_CODE_LEN,
        ),
        None => format!(
            "<!doctype html><html><head><meta charset=\"utf-8\">\
             <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
             <meta http-equiv=\"refresh\" content=\"5\">\
             <title>Steam Guard</title><style>{PAGE_STYLE}</style></head><body><main>\
             <h1>Steam Guard</h1>{notice}<div class=\"card\">\
             <p>No code prompt pending.</p>\
             <p class=\"muted\">This page activates when the bot's Steam login is \
             challenged for a Guard code. It refreshes automatically.</p>\
             </div></main></body></html>",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn submit_without_prompt_is_rejected() {
        let gate = GuardGate::new("test_no_prompt");
        assert_eq!(gate.submit("ABC12"), GuardSubmitOutcome::NoPrompt);
    }

    #[tokio::test]
    async fn code_round_trip_normalizes_and_disarms() {
        let gate = GuardGate::new("test_round_trip");
        let waiter = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                gate.wait_for_code("bot", "device code", Duration::from_secs(5))
                    .await
            })
        };
        while gate.status().is_none() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(gate.status().unwrap().account, "bot");
        assert_eq!(gate.submit("  abc12 "), GuardSubmitOutcome::Accepted);
        assert_eq!(waiter.await.unwrap(), Some("ABC12".to_string()));
        assert!(gate.status().is_none());
        assert_eq!(gate.submit("ABC12"), GuardSubmitOutcome::NoPrompt);
    }

    #[tokio::test]
    async fn notice_is_rendered_escaped_and_clearable() {
        let gate = GuardGate::new("test_notice");
        assert!(!render_page(&gate).contains("card notice"));
        gate.set_notice("Revocation code: R12345 <keep it safe>");
        let page = render_page(&gate);
        assert!(page.contains("card notice"));
        assert!(page.contains("&lt;keep it safe&gt;"));
        gate.clear_notice();
        assert!(!render_page(&gate).contains("card notice"));
    }

    #[tokio::test]
    async fn timeout_disarms_the_gate() {
        let gate = GuardGate::new("test_timeout");
        let code = gate
            .wait_for_code("bot", "email", Duration::from_millis(10))
            .await;
        assert_eq!(code, None);
        assert!(gate.status().is_none());
    }

    #[tokio::test]
    async fn implausible_codes_are_rejected_without_consuming_the_prompt() {
        let gate = GuardGate::new("test_invalid");
        let waiter = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                gate.wait_for_code("bot", "device code", Duration::from_secs(5))
                    .await
            })
        };
        while gate.status().is_none() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(gate.submit(""), GuardSubmitOutcome::Invalid);
        assert_eq!(gate.submit("has spaces in it"), GuardSubmitOutcome::Invalid);
        assert_eq!(
            gate.submit("waytoolongtobeacode99"),
            GuardSubmitOutcome::Invalid
        );
        // The prompt survives invalid submissions.
        assert_eq!(gate.submit("XYZ99"), GuardSubmitOutcome::Accepted);
        assert_eq!(waiter.await.unwrap(), Some("XYZ99".to_string()));
    }

    async fn http_request(addr: SocketAddr, raw: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(raw.as_bytes()).await.unwrap();
        let mut out = Vec::new();
        // `Connection: close` in every request → server closes after the
        // response, so read-to-EOF terminates.
        stream.read_to_end(&mut out).await.unwrap();
        String::from_utf8_lossy(&out).into_owned()
    }

    #[tokio::test]
    async fn http_listener_serves_page_and_accepts_codes() {
        let gate = GuardGate::new("test_http");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_guard_on(listener, Arc::clone(&gate)));

        let resp = http_request(
            addr,
            "GET / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(resp.starts_with("HTTP/1.1 200"), "idle page: {resp}");
        assert!(resp.contains("No code prompt pending"));

        let resp = http_request(
            addr,
            "POST /code HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\
             Content-Type: application/x-www-form-urlencoded\r\n\
             Content-Length: 10\r\n\r\ncode=ABC12",
        )
        .await;
        assert!(resp.starts_with("HTTP/1.1 409"), "no prompt: {resp}");

        // Park a login, then drive it through the page.
        let waiter = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                gate.wait_for_code("bot", "email code", Duration::from_secs(5))
                    .await
            })
        };
        while gate.status().is_none() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let resp = http_request(
            addr,
            "GET / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(
            resp.contains("bot") && resp.contains("email code"),
            "pending page: {resp}"
        );

        let resp = http_request(
            addr,
            "GET /status HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(resp.contains("\"pending\":true"), "status: {resp}");

        // A browser form post is redirected back to the page.
        let resp = http_request(
            addr,
            "POST /code HTTP/1.1\r\nHost: t\r\nConnection: close\r\nAccept: text/html\r\n\
             Content-Type: application/x-www-form-urlencoded\r\n\
             Content-Length: 10\r\n\r\ncode=abc12",
        )
        .await;
        assert!(resp.starts_with("HTTP/1.1 303"), "form post: {resp}");
        assert_eq!(waiter.await.unwrap(), Some("ABC12".to_string()));
    }

    #[test]
    fn extract_code_handles_form_and_bare_bodies() {
        assert_eq!(extract_code("code=ABC12"), Some("ABC12".to_string()));
        assert_eq!(
            extract_code("other=x&code=AB%43D2&more=y"),
            Some("ABCD2".to_string())
        );
        assert_eq!(extract_code("ABC12"), Some("ABC12".to_string()));
        assert_eq!(extract_code("ABC12\n"), Some("ABC12".to_string()));
        assert_eq!(extract_code(""), None);
        assert_eq!(extract_code("notcode=ABC12"), None);
    }
}
