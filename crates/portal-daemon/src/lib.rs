//! Shared daemon plumbing for the portal bots (deploy docs:
//! observability-design.md §4.4): a loopback HTTP listener serving
//! `/metrics` + `/healthz`, graceful-shutdown signalling, sd_notify READY,
//! and a jittered ticker helper.
//!
//! No framework — one hyper connection handler, ~100 lines, shared by
//! cs2-poller and cs2-enricher so neither grows its own copy.

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Response, StatusCode};
use hyper_util::rt::TokioIo;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use rand::Rng;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::info;

/// Current Unix time in whole seconds.
#[must_use]
pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Current Unix time in seconds, for `*_last_success_timestamp_seconds`.
#[must_use]
pub fn unix_now_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Liveness state behind `/healthz`: healthy while the last successful
/// cycle is younger than `stale_after` (the design's "200 iff last cycle
/// < 3×interval ago"). Starts from process boot, so a bot that never
/// completes a first cycle goes unhealthy after the same window.
pub struct Health {
    last_success: AtomicU64,
    stale_after_secs: u64,
}

impl Health {
    /// Create the shared health state.
    #[must_use]
    pub fn new(stale_after: Duration) -> Arc<Self> {
        Arc::new(Self {
            last_success: AtomicU64::new(unix_now()),
            stale_after_secs: stale_after.as_secs().max(1),
        })
    }

    /// Record a successful cycle.
    pub fn mark_success(&self) {
        self.last_success.store(unix_now(), Ordering::SeqCst);
    }

    /// Whether the last success is within the staleness window.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        unix_now().saturating_sub(self.last_success.load(Ordering::SeqCst)) < self.stale_after_secs
    }
}

/// Install the Prometheus recorder and spawn the loopback `/metrics` +
/// `/healthz` listener when `METRICS_ADDR` is set (empty/unset = fully
/// disabled). Returns whether metrics are enabled.
///
/// `buckets` overrides histogram buckets per metric name.
///
/// # Panics
/// Panics on an unparseable address or bucket config — a misconfigured
/// exporter should fail loudly at startup, not silently monitor nothing.
pub fn start_from_env(
    build_info_metric: &'static str,
    version: &'static str,
    buckets: &[(&str, &[f64])],
    health: Arc<Health>,
) -> bool {
    let Some(addr) = std::env::var("METRICS_ADDR")
        .ok()
        .filter(|v| !v.trim().is_empty())
    else {
        info!("METRICS_ADDR not set — metrics exporter disabled");
        return false;
    };
    let addr: SocketAddr = addr
        .parse()
        .unwrap_or_else(|e| panic!("invalid METRICS_ADDR {addr:?}: {e}"));

    let mut builder = PrometheusBuilder::new();
    for (name, values) in buckets {
        builder = builder
            .set_buckets_for_metric(Matcher::Full((*name).to_string()), values)
            .expect("bucket config");
    }
    let handle = builder
        .install_recorder()
        .expect("install Prometheus recorder");

    metrics::gauge!(build_info_metric, "version" => version).set(1.0);

    // The standalone recorder needs periodic upkeep to drain histogram
    // samples (the bundled exporter listener would do this for us, but it
    // cannot serve /healthz — hence this hand-rolled pair).
    let upkeep = handle.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            upkeep.run_upkeep();
        }
    });
    tokio::spawn(serve(addr, handle, health));
    true
}

async fn serve(addr: SocketAddr, handle: PrometheusHandle, health: Arc<Health>) {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind metrics listener {addr}: {e}"));
    info!("metrics + healthz listening on http://{addr}");
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let handle = handle.clone();
        let health = Arc::clone(&health);
        tokio::spawn(async move {
            let service = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                let handle = handle.clone();
                let health = Arc::clone(&health);
                async move {
                    let resp = match req.uri().path() {
                        "/healthz" => {
                            if health.is_healthy() {
                                Response::new(Full::new(Bytes::from_static(b"ok\n")))
                            } else {
                                Response::builder()
                                    .status(StatusCode::SERVICE_UNAVAILABLE)
                                    .body(Full::new(Bytes::from_static(b"stale\n")))
                                    .expect("static response")
                            }
                        }
                        _ => Response::builder()
                            .header("content-type", "text/plain; version=0.0.4")
                            .body(Full::new(Bytes::from(handle.render())))
                            .expect("metrics response"),
                    };
                    Ok::<_, std::convert::Infallible>(resp)
                }
            });
            let _ = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
    }
}

/// Resolves when the process is asked to terminate: SIGINT (Ctrl-C) on all
/// platforms, SIGTERM additionally on Unix. The in-flight cycle finishes
/// before the caller exits, so `TimeoutStopSec` stops being a race.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => info!("received SIGINT"),
        () = terminate => info!("received SIGTERM"),
    }
}

/// Tell systemd we're up (`Type=notify`). A no-op outside systemd.
pub fn notify_ready() {
    let _ = sd_notify::notify(false, &[sd_notify::NotifyState::Ready]);
}

/// Tell systemd we're shutting down. A no-op outside systemd.
pub fn notify_stopping() {
    let _ = sd_notify::notify(false, &[sd_notify::NotifyState::Stopping]);
}

/// Uniform jitter in `[0, max)` for the ticker — avoids thundering-herd
/// against Steam when several bots share a box.
#[must_use]
pub fn jitter(max: Duration) -> Duration {
    if max.is_zero() {
        return Duration::ZERO;
    }
    rand::rng().random_range(Duration::ZERO..max)
}
