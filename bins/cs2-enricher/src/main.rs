//! CS2 Match Enricher Bot
//!
//! Fetches full match data from the CS2 Game Coordinator for pending
//! discovered matches, then submits the enriched data back to the Portal API.

use clap::Parser;
use cs2_demo_rank::RankUpdate;
use cs2_gc::{Cs2GcClient, GcTransportError};
use parallel_bzip2_decoder::{decompress_block, scan_blocks};
use portal_daemon::GuardGate;
use rayon::prelude::*;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use steam_vent::{ConnectionError, LoginError, NetworkError, ServerList};
use tracing::{error, info, warn};

/// CS2 match enricher bot.
#[derive(Parser)]
#[command(name = "cs2-enricher")]
struct Args {
    /// Portal API base URL.
    #[arg(
        long,
        env = "PORTAL_API_URL",
        default_value = "http://localhost:3000/v1"
    )]
    portal_api_url: String,

    /// Portal API key for this bot.
    #[arg(long, env = "PORTAL_API_KEY")]
    portal_api_key: String,

    /// Steam account username.
    #[arg(long, env = "STEAM_USERNAME")]
    username: String,

    /// Steam account password (prompted if not set).
    #[arg(long, env = "STEAM_PASSWORD")]
    password: Option<String>,

    /// Steam Guard shared secret for TOTP (optional — falls back to console prompt).
    #[arg(long, env = "STEAM_SHARED_SECRET")]
    shared_secret: Option<String>,

    /// Batch size — how many pending matches to fetch per cycle.
    #[arg(long, env = "BATCH_SIZE", default_value = "5")]
    batch_size: i64,

    /// How many demo-extraction jobs to lease per cycle.
    ///
    /// Smaller than `batch_size`: a demo is a multi-hundred-megabyte download
    /// followed by a CPU-bound parse, where a GC call is one round trip.
    #[arg(long, env = "DEMO_BATCH_SIZE", default_value = "2")]
    demo_batch_size: i64,

    /// Enrichment interval in seconds.
    #[arg(long, env = "ENRICH_INTERVAL_SECS", default_value = "30")]
    enrich_interval: u64,

    /// Game slug to enrich for.
    #[arg(long, env = "GAME_SLUG", default_value = "cs2")]
    game_slug: String,

    /// Skip the demo-extraction stage entirely.
    ///
    /// Enrichment still records the demo URL, so the jobs queue up and are
    /// picked up whenever an enricher runs without this set.
    #[arg(long, env = "SKIP_DEMO_RANK", default_value = "false")]
    skip_demo_rank: bool,
}

// =============================================================================
// Portal API client types
// =============================================================================

#[derive(Debug, Deserialize)]
struct PendingMatch {
    id: String,
    share_code: String,
    match_id: i64,
    outcome_id: i64,
    token: i32,
    retry_count: i32,
}

#[derive(Debug, Serialize)]
struct PlayerRatingEntry {
    account_id: u32,
    rank_id: i32,
    rank_type_id: u32,
    wins: u32,
    rank_change: f32,
}

#[derive(Serialize)]
struct EnrichedMatchRequest<'a, T: Serialize> {
    gc_data: &'a T,
    demo_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    player_ratings: Option<Vec<PlayerRatingEntry>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    map_name: Option<String>,
}

#[derive(Debug, Serialize)]
struct FailedMatchRequest {
    error: String,
}

/// One demo-extraction job leased from the portal.
///
/// The lease already banked an attempt against this row before we saw it, so
/// dying here costs one attempt rather than looping forever.
#[derive(Debug, Deserialize)]
struct DemoJob {
    id: String,
    share_code: String,
    demo_url: String,
    attempt: i32,
    max_attempts: i32,
}

#[derive(Debug, Serialize)]
struct DemoResultRequest<'a> {
    outcome: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    player_ratings: Option<Vec<PlayerRatingEntry>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    map_name: Option<String>,
}

// =============================================================================
// Portal API client
// =============================================================================

struct PortalClient {
    http: Client,
    base_url: String,
    api_key: String,
}

impl PortalClient {
    fn new(base_url: &str, api_key: &str) -> Self {
        Self {
            http: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
        }
    }

    async fn get_pending_matches(
        &self,
        game: &str,
        limit: i64,
    ) -> Result<Vec<PendingMatch>, reqwest::Error> {
        self.http
            .get(format!(
                "{}/internal/discovered-matches/pending",
                self.base_url
            ))
            .header("X-API-Key", &self.api_key)
            .query(&[("game", game), ("limit", &limit.to_string())])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
    }

    async fn claim_match(&self, match_id: &str) -> Result<bool, reqwest::Error> {
        let resp = self
            .http
            .post(format!(
                "{}/internal/discovered-matches/{match_id}/claim",
                self.base_url
            ))
            .header("X-API-Key", &self.api_key)
            .send()
            .await?;

        // 200 = claimed, 409 = already claimed by another worker
        Ok(resp.status().is_success())
    }

    async fn submit_enriched<T: Serialize>(
        &self,
        match_id: &str,
        req: &EnrichedMatchRequest<'_, T>,
    ) -> Result<(), reqwest::Error> {
        self.http
            .post(format!(
                "{}/internal/discovered-matches/{match_id}/enriched",
                self.base_url
            ))
            .header("X-API-Key", &self.api_key)
            .json(req)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }

    async fn mark_failed(&self, match_id: &str, error: &str) -> Result<(), reqwest::Error> {
        self.http
            .post(format!(
                "{}/internal/discovered-matches/{match_id}/failed",
                self.base_url
            ))
            .header("X-API-Key", &self.api_key)
            .json(&FailedMatchRequest {
                error: error.to_string(),
            })
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }

    /// Lease demo-extraction jobs.
    ///
    /// POST because it mutates: the portal increments each row's attempt
    /// counter and holds a lease on it, in the same statement that selects it.
    /// That is what makes the attempt count survive this process dying.
    async fn lease_demo_jobs(
        &self,
        game: &str,
        limit: i64,
    ) -> Result<Vec<DemoJob>, reqwest::Error> {
        self.http
            .post(format!(
                "{}/internal/discovered-matches/demo-jobs",
                self.base_url
            ))
            .header("X-API-Key", &self.api_key)
            .query(&[("game", game), ("limit", &limit.to_string())])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
    }

    async fn submit_demo_result(
        &self,
        match_id: &str,
        req: &DemoResultRequest<'_>,
    ) -> Result<(), reqwest::Error> {
        self.http
            .post(format!(
                "{}/internal/discovered-matches/{match_id}/demo-result",
                self.base_url
            ))
            .header("X-API-Key", &self.api_key)
            .json(req)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }
}

// =============================================================================
// Steam / GC connection
// =============================================================================

/// Classify a `connect_gc` failure for `cs2_enricher_logon_failures_total`.
/// Each reason needs a different human response: bad-creds → fix the vault,
/// steam-guard → re-auth the bot account, rate-limit → wait.
fn classify_logon_failure(e: &(dyn std::error::Error + 'static)) -> &'static str {
    if let Some(conn_err) = e.downcast_ref::<ConnectionError>() {
        return match conn_err {
            ConnectionError::LoginError(login) => match login {
                LoginError::InvalidCredentials => "bad-creds",
                LoginError::SteamGuardRequired => "steam-guard",
                LoginError::RateLimited => "rate-limit",
                _ => "login-other",
            },
            ConnectionError::Network(net) => classify_network_failure(net),
            // The guard gate aborts the login when no code arrives in time;
            // the next reconnect pass re-arms the code-entry page.
            ConnectionError::Aborted => "guard-timeout",
            _ => "other",
        };
    }
    if e.downcast_ref::<cs2_gc::Error>().is_some() {
        return "gc-handshake";
    }
    "other"
}

/// Steam reports most login refusals as an EResult on the message header,
/// not as a [`LoginError`] — steam-vent surfaces those as
/// `Network(ApiError(..))`, so matching only on `LoginError` files a
/// throttle under "network" and leaves the rate-limit series flat during
/// exactly the incident it exists to catch. Reasons here must agree with
/// the `LoginError` arm above: the same condition cannot have two names.
fn classify_network_failure(e: &NetworkError) -> &'static str {
    let NetworkError::ApiError(result) = e else {
        return "network";
    };
    match *result as i32 {
        // RateLimitExceeded, then AccountLoginDeniedThrottle once Steam
        // escalates. Both mean the same thing: stop logging in and wait.
        84 | 87 => "rate-limit",
        // Guard needed but unmet — a missing/incorrect shared secret, or a
        // code prompt nobody answered.
        63 | 85 => "steam-guard",
        5 => "bad-creds",
        51 => "suspended",
        _ => "network",
    }
}

async fn connect_gc(
    args: &Args,
    guard: Option<&Arc<GuardGate>>,
) -> Result<Cs2GcClient, Box<dyn std::error::Error>> {
    let password = match args.password {
        Some(ref p) => p.clone(),
        None => rpassword::prompt_password("Steam password: ")?,
    };

    info!("Discovering Steam CM servers...");
    let server_list = ServerList::discover().await?;

    info!(username = %args.username, "Logging in to Steam...");

    // An empty STEAM_SHARED_SECRET (vault placeholder) means "not set" —
    // silently generating codes from an empty secret would soft-lock login.
    let shared_secret = args
        .shared_secret
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let connection = steam_login_gate::login(
        &server_list,
        &args.username,
        &password,
        shared_secret,
        guard,
        steam_login_gate::DEFAULT_GUARD_CODE_WAIT,
    )
    .await?;

    info!("Logged in. Connecting to CS2 Game Coordinator...");
    let mut cs2 = Cs2GcClient::connect(connection).await?;

    // hello() is optional but useful for confirming GC is responsive
    match cs2.hello().await {
        Ok(profile) => {
            info!(
                account_id = profile.account_id,
                ranks = profile.rankings.len(),
                "CS2 GC connected, got profile"
            );
        }
        Err(e) => {
            warn!("CS2 hello failed ({e}), continuing without profile");
        }
    }

    Ok(cs2)
}

// =============================================================================
// Demo rank extraction
// =============================================================================

/// How long to wait on one demo download before giving up on this attempt.
///
/// Generous — these are hundreds of megabytes — but bounded, because the whole
/// point of the retry stage is that abandoning an attempt is now cheap.
const DEMO_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(180);

/// The outcome of one demo attempt, in the vocabulary the portal's retry stage
/// speaks.
///
/// Classification decides whether the portal schedules another attempt, so the
/// bias throughout is toward *retryable*. Calling a transient failure permanent
/// throws the match's rank data away for good — precisely the bug this stage
/// exists to fix — whereas calling a permanent failure transient costs a
/// handful of cheap 404s before the budget settles it anyway.
enum DemoAttempt {
    /// Parsed with rank updates.
    Succeeded {
        ranks: Vec<RankUpdate>,
        map_name: Option<String>,
    },
    /// Parsed cleanly, no rank updates. Casual and deathmatch demos are
    /// legitimately empty; the map name is still worth keeping.
    Empty { map_name: Option<String> },
    /// Not on the CDN yet, or no longer. Retryable.
    Unavailable(String),
    /// Any other transient failure. Retryable.
    Failed(String),
    /// Explicitly retired by Valve (410). Terminal.
    Gone(String),
}

impl DemoAttempt {
    /// Wire name for the portal's `demo-result` endpoint.
    const fn outcome(&self) -> &'static str {
        match self {
            Self::Succeeded { .. } => "succeeded",
            Self::Empty { .. } => "empty",
            Self::Unavailable(_) => "unavailable",
            Self::Failed(_) => "failed",
            Self::Gone(_) => "gone",
        }
    }
}

/// Download a `.dem.bz2` demo from the Valve CDN, decompress, and extract rank
/// updates + metadata.
///
/// Returns a classified [`DemoAttempt`] rather than a `Result`: every failure
/// mode here is a normal, expected state of the pipeline that the portal knows
/// how to schedule around, not an error for the caller to handle.
async fn attempt_demo_extraction(http: &Client, demo_url: &str) -> DemoAttempt {
    info!(url = %demo_url, "Downloading demo for rank extraction");

    let response = match http
        .get(demo_url)
        .timeout(DEMO_DOWNLOAD_TIMEOUT)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => return DemoAttempt::Failed(format!("download failed: {e}")),
    };

    let status = response.status();
    if !status.is_success() {
        return match status.as_u16() {
            // Valve serves 404 both for "not published yet" and "past
            // retention". Indistinguishable from here, so retry and let the
            // budget settle it — a match that ended two minutes ago and one
            // that ended three weeks ago look identical at this layer.
            404 => DemoAttempt::Unavailable("demo not present on the CDN (404)".to_string()),
            410 => DemoAttempt::Gone("demo retired by Valve (410)".to_string()),
            code => DemoAttempt::Failed(format!("CDN returned HTTP {code}")),
        };
    }

    let compressed = match response.bytes().await {
        Ok(b) => b,
        Err(e) => return DemoAttempt::Failed(format!("reading demo body failed: {e}")),
    };
    info!(
        size_bytes = compressed.len(),
        "Demo downloaded, decompressing bzip2"
    );

    let blocks: Vec<(u64, u64)> = scan_blocks(&compressed).into_iter().collect();
    if blocks.is_empty() {
        // A 200 carrying no bzip2 blocks is the CDN handing back a placeholder
        // or an error page, not a demo. Same situation as a 404.
        return DemoAttempt::Unavailable(format!(
            "response contained no bzip2 blocks ({} bytes) — not a demo",
            compressed.len()
        ));
    }

    let decompressed = match blocks
        .par_iter()
        .map(|&(start, end)| decompress_block(&compressed, start, end))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(parts) => {
            let total_size: usize = parts.iter().map(Vec::len).sum();
            let mut out = Vec::with_capacity(total_size);
            for part in parts {
                out.extend_from_slice(&part);
            }
            out
        }
        // Usually a truncated download rather than a corrupt demo, so this is
        // retryable.
        Err(e) => return DemoAttempt::Failed(format!("bzip2 decompression failed: {e}")),
    };

    info!(
        decompressed_bytes = decompressed.len(),
        "Decompressed, scanning for rank updates"
    );

    let ranks = match cs2_demo_rank::extract_rank_updates(&decompressed) {
        Ok(r) => r,
        Err(e) => return DemoAttempt::Failed(format!("rank extraction failed: {e}")),
    };

    // Metadata is a bonus: a demo whose ranks parsed but whose header did not
    // is still a successful attempt.
    let map_name = cs2_demo_rank::extract_demo_metadata(&decompressed)
        .unwrap_or_else(|e| {
            warn!("Failed to extract demo metadata: {e}");
            cs2_demo_rank::DemoMetadata { map_name: None }
        })
        .map_name;

    info!(
        rank_count = ranks.len(),
        map_name = ?map_name,
        "Demo extraction complete"
    );

    if ranks.is_empty() {
        DemoAttempt::Empty { map_name }
    } else {
        DemoAttempt::Succeeded { ranks, map_name }
    }
}

// =============================================================================
// Main loop
// =============================================================================

/// Enrichment of one batch can legitimately take minutes (GC calls at 2s
/// spacing + up to 120s per demo download), so the cycle deadline is a
/// generous backstop, not the poll interval.
const CYCLE_DEADLINE: Duration = Duration::from_secs(600);

/// Deadline for one demo-extraction pass.
///
/// Must stay below the portal's demo lease (20 min) so an overrunning pass is
/// abandoned *before* the rows it holds become eligible for another worker —
/// otherwise two enrichers download the same demo.
const DEMO_CYCLE_DEADLINE: Duration = Duration::from_secs(900);

/// Coarse buckets for per-match enrichment. Tighter than they were: enrichment
/// is now just the GC round trip, with the demo download moved to its own stage.
const ENRICH_DURATION_BUCKETS: &[f64] = &[0.5, 1.0, 2.5, 5.0, 15.0, 30.0, 60.0, 120.0];

/// Buckets for one demo attempt (CDN fetch + bzip2 + rank scan). Reaches
/// further out than enrichment — these are hundreds of megabytes.
const DEMO_ATTEMPT_DURATION_BUCKETS: &[f64] =
    &[1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 180.0, 300.0, 600.0];

/// Reconnect backoff bounds for the GC session.
const RECONNECT_BACKOFF_INITIAL: Duration = Duration::from_secs(5);
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(300);
/// Flat wait when Steam says we are logging in too often.
///
/// A throttle is not a transport blip: climbing the ordinary ladder from
/// 5 s spends a dozen login attempts inside the window Steam wants quiet,
/// which is how `RateLimitExceeded` escalates into
/// `AccountLoginDeniedThrottle` and turns a short cooldown into a long one.
/// Hold flat and long instead — the failure mode of waiting too long is a
/// late recovery, and of waiting too little, no recovery at all.
const RECONNECT_BACKOFF_RATE_LIMITED: Duration = Duration::from_secs(900);

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "cs2_enricher=info".into()),
        )
        .init();

    let args = Args::parse();

    info!(
        portal_url = %args.portal_api_url,
        game = %args.game_slug,
        batch_size = args.batch_size,
        enrich_interval = args.enrich_interval,
        "Starting CS2 enricher bot"
    );

    let interval = Duration::from_secs(args.enrich_interval);
    // /healthz goes stale (503) when no cycle has succeeded for 3×interval
    // (bounded below by the cycle deadline — a busy cycle is not "stale").
    let health = portal_daemon::Health::new((interval * 3).max(CYCLE_DEADLINE));
    portal_daemon::start_from_env(
        "cs2_enricher_build_info",
        env!("CARGO_PKG_VERSION"),
        &[
            (
                "cs2_enricher_enrich_duration_seconds",
                ENRICH_DURATION_BUCKETS,
            ),
            (
                "cs2_enricher_demo_attempt_duration_seconds",
                DEMO_ATTEMPT_DURATION_BUCKETS,
            ),
        ],
        std::sync::Arc::clone(&health),
    );

    // Remote Steam Guard code entry (GUARD_ADDR, tailnet-only ingress via
    // Tailscale Serve). Only consulted when no shared secret is configured
    // and the stored machine token doesn't satisfy the login.
    let guard_gate = GuardGate::new("cs2_enricher");
    let guard_gate =
        portal_daemon::start_guard_from_env(Arc::clone(&guard_gate)).then_some(guard_gate);

    let portal = PortalClient::new(&args.portal_api_url, &args.portal_api_key);

    // The GC session is the fragile part — it is now a first-class state
    // (gc_session_up gauge) with reconnect instead of the old
    // connect-once-then-exit shape that let a dead session mark every
    // pending match failed.
    let mut gc: Option<Cs2GcClient> = None;
    let mut backoff = RECONNECT_BACKOFF_INITIAL;

    portal_daemon::notify_ready();
    let shutdown = portal_daemon::shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        // (Re)establish the GC session when absent.
        if gc.is_none() {
            match connect_gc(&args, guard_gate.as_ref()).await {
                Ok(client) => {
                    metrics::gauge!("cs2_enricher_gc_session_up").set(1.0);
                    gc = Some(client);
                    backoff = RECONNECT_BACKOFF_INITIAL;
                }
                Err(e) => {
                    let reason = classify_logon_failure(e.as_ref());
                    metrics::gauge!("cs2_enricher_gc_session_up").set(0.0);
                    metrics::counter!("cs2_enricher_logon_failures_total", "reason" => reason)
                        .increment(1);
                    let rate_limited = reason == "rate-limit";
                    let wait = if rate_limited {
                        RECONNECT_BACKOFF_RATE_LIMITED
                    } else {
                        backoff
                    };
                    error!(
                        reason,
                        backoff_secs = wait.as_secs(),
                        "Failed to connect to Steam GC: {e}"
                    );
                    tokio::select! {
                        () = tokio::time::sleep(wait) => {}
                        () = &mut shutdown => break,
                    }
                    // A throttle holds flat at its own floor; only the
                    // ordinary ladder climbs, and it keeps its own position
                    // so a cleared throttle does not inherit a 15 min wait.
                    if !rate_limited {
                        backoff = (backoff * 2).min(RECONNECT_BACKOFF_MAX);
                    }
                    continue;
                }
            }
        }
        let Some(gc_client) = gc.as_mut() else {
            unreachable!("gc is Some after successful connect")
        };

        tokio::select! {
            cycle = tokio::time::timeout(
                CYCLE_DEADLINE,
                enrich_cycle(&portal, gc_client, &args.game_slug, args.batch_size),
            ) => {
                match cycle {
                    Ok(Ok(())) => {
                        health.mark_success();
                        metrics::gauge!("cs2_enricher_last_success_timestamp_seconds")
                            .set(portal_daemon::unix_now_f64());
                    }
                    Ok(Err(CycleError::GcSession(e))) => {
                        // Session-fatal: drop the client and reconnect next
                        // pass rather than failing every pending match.
                        metrics::gauge!("cs2_enricher_gc_session_up").set(0.0);
                        metrics::counter!("cs2_enricher_gc_reconnects_total", "reason" => "stream-closed")
                            .increment(1);
                        warn!("GC session lost, reconnecting: {e}");
                        gc = None;
                        continue;
                    }
                    Ok(Err(CycleError::Portal(e))) => {
                        error!("Enrich cycle error: {e}");
                    }
                    Err(_) => {
                        warn!(
                            deadline_secs = CYCLE_DEADLINE.as_secs(),
                            "Enrich cycle exceeded its deadline; skipping to next tick"
                        );
                    }
                }
            }
            () = &mut shutdown => break,
        }

        // Demo extraction runs after enrichment but is not gated on it: it
        // needs no GC session, so it keeps draining while the GC is down or
        // reconnecting. Its failures are all handled per-job against the
        // portal's retry state, so there is nothing for this loop to react to.
        if !args.skip_demo_rank {
            tokio::select! {
                r = tokio::time::timeout(
                    DEMO_CYCLE_DEADLINE,
                    demo_cycle(&portal, &args.game_slug, args.demo_batch_size),
                ) => {
                    if r.is_err() {
                        warn!(
                            deadline_secs = DEMO_CYCLE_DEADLINE.as_secs(),
                            "Demo cycle exceeded its deadline; leases will expire and requeue"
                        );
                    }
                }
                () = &mut shutdown => break,
            }
        }

        let sleep = interval + portal_daemon::jitter(interval / 10);
        tokio::select! {
            () = tokio::time::sleep(sleep) => {}
            () = &mut shutdown => break,
        }
    }

    portal_daemon::notify_stopping();
    info!("shutdown complete");
}

/// Cycle-level failures, split by required reaction.
enum CycleError {
    /// Portal API unreachable — retry next tick.
    Portal(reqwest::Error),
    /// GC session died — reconnect before the next cycle.
    GcSession(cs2_gc::Error),
}

impl std::fmt::Display for CycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Portal(e) => write!(f, "portal API: {e}"),
            Self::GcSession(e) => write!(f, "GC session: {e}"),
        }
    }
}

async fn enrich_cycle(
    portal: &PortalClient,
    gc: &mut Cs2GcClient,
    game_slug: &str,
    batch_size: i64,
) -> Result<(), CycleError> {
    let pending = portal
        .get_pending_matches(game_slug, batch_size)
        .await
        .map_err(CycleError::Portal)?;

    // Clamped by batch_size, so this saturates rather than showing the true
    // backlog — still the right signal for "work keeps arriving faster than
    // we drain it".
    metrics::gauge!("cs2_enricher_queue_depth").set(pending.len() as f64);

    if pending.is_empty() {
        return Ok(());
    }

    info!(count = pending.len(), "Fetched pending matches");

    let mut last_gc_call: Option<tokio::time::Instant> = None;

    for m in &pending {
        // Rate-limit GC calls: ensure 2s since last call
        if let Some(last) = last_gc_call {
            let elapsed = last.elapsed();
            if elapsed < Duration::from_secs(2) {
                tokio::time::sleep(Duration::from_secs(2) - elapsed).await;
            }
        }

        // Claim atomically — another enricher instance may grab it first
        let claimed = match portal.claim_match(&m.id).await {
            Ok(c) => c,
            Err(e) => {
                warn!(match_id = %m.id, "Failed to claim match: {e}");
                continue;
            }
        };

        if !claimed {
            info!(match_id = %m.id, "Match already claimed, skipping");
            continue;
        }

        info!(
            match_id = %m.id,
            share_code = %m.share_code,
            gc_match_id = m.match_id,
            retry = m.retry_count,
            "Enriching match"
        );

        // Call GC for full match data
        last_gc_call = Some(tokio::time::Instant::now());
        let enrich_start = tokio::time::Instant::now();
        match gc
            .match_info(m.match_id as u64, m.outcome_id as u64, m.token as u32)
            .await
        {
            Ok(matches) => {
                // Extract demo URL from the first match result (if available)
                let demo_url = matches
                    .first()
                    .and_then(|mi| mi.demo.as_ref())
                    .and_then(|d| d.download_url());

                // The demo is deliberately NOT fetched here. Recording the URL
                // is what opens the demo stage on the portal side, and
                // `demo_cycle` drains that stage with its own budget, its own
                // backoff and a durable attempt count.
                //
                // Doing it inline is what lost data in production: a demo Valve
                // had not published yet failed its one and only attempt, the
                // match was written as `enriched` with no ratings and no map
                // name, and nothing anywhere recorded that it should ever be
                // tried again.
                info!(
                    match_id = %m.id,
                    has_demo = demo_url.is_some(),
                    match_count = matches.len(),
                    "Got GC data, submitting enriched result"
                );

                match portal
                    .submit_enriched(
                        &m.id,
                        &EnrichedMatchRequest {
                            gc_data: &matches,
                            demo_url,
                            player_ratings: None,
                            map_name: None,
                        },
                    )
                    .await
                {
                    Ok(()) => {
                        metrics::counter!("cs2_enricher_matches_enriched_total", "outcome" => "ok")
                            .increment(1);
                    }
                    Err(e) => {
                        metrics::counter!(
                            "cs2_enricher_matches_enriched_total",
                            "outcome" => "submit-error"
                        )
                        .increment(1);
                        error!(match_id = %m.id, "Failed to submit enriched data: {e}");
                    }
                }
                metrics::histogram!("cs2_enricher_enrich_duration_seconds")
                    .record(enrich_start.elapsed().as_secs_f64());
            }
            Err(e) => {
                // A closed GC stream is session death, not a bad match:
                // reconnect instead of marking every pending match failed
                // at batch_size per cycle (the old crash-the-budget bug).
                if matches!(&e, cs2_gc::Error::Transport(GcTransportError::StreamClosed)) {
                    return Err(CycleError::GcSession(e));
                }

                metrics::counter!("cs2_enricher_matches_enriched_total", "outcome" => "gc-error")
                    .increment(1);
                warn!(
                    match_id = %m.id,
                    share_code = %m.share_code,
                    error = %e,
                    "GC match_info failed"
                );

                if let Err(e2) = portal.mark_failed(&m.id, &e.to_string()).await {
                    error!(match_id = %m.id, "Failed to report failure: {e2}");
                }
            }
        }
    }

    Ok(())
}

/// Drain a batch of demo-extraction jobs.
///
/// Independent of the GC session on purpose: a demo download needs no GC, so a
/// dead or reconnecting session must not stop demos being fetched, and a slow
/// CDN must not eat into the GC's rate-limited budget.
///
/// Errors are per-job and never propagate — a portal round trip that fails
/// leaves the row leased, and the lease expiring is exactly the recovery path
/// that already exists for a crashed worker.
async fn demo_cycle(portal: &PortalClient, game_slug: &str, limit: i64) {
    let jobs = match portal.lease_demo_jobs(game_slug, limit).await {
        Ok(j) => j,
        Err(e) => {
            error!("Failed to lease demo jobs: {e}");
            return;
        }
    };

    metrics::gauge!("cs2_enricher_demo_jobs_leased").set(jobs.len() as f64);

    if jobs.is_empty() {
        return;
    }

    info!(count = jobs.len(), "Leased demo extraction jobs");

    for job in &jobs {
        info!(
            match_id = %job.id,
            share_code = %job.share_code,
            attempt = job.attempt,
            max = job.max_attempts,
            "Attempting demo extraction"
        );

        let started = tokio::time::Instant::now();
        let attempt = attempt_demo_extraction(&portal.http, &job.demo_url).await;
        metrics::histogram!("cs2_enricher_demo_attempt_duration_seconds")
            .record(started.elapsed().as_secs_f64());
        metrics::counter!(
            "cs2_enricher_demo_attempts_total",
            "outcome" => attempt.outcome(),
        )
        .increment(1);

        let request = match attempt {
            DemoAttempt::Succeeded { ranks, map_name } => DemoResultRequest {
                outcome: "succeeded",
                error: None,
                player_ratings: Some(
                    ranks
                        .into_iter()
                        .map(|r| PlayerRatingEntry {
                            account_id: r.account_id,
                            rank_id: r.rank_id,
                            rank_type_id: r.rank_type_id,
                            wins: r.wins,
                            rank_change: r.rank_change,
                        })
                        .collect(),
                ),
                map_name,
            },
            DemoAttempt::Empty { map_name } => DemoResultRequest {
                outcome: "empty",
                error: None,
                player_ratings: None,
                map_name,
            },
            ref failure @ (DemoAttempt::Unavailable(ref error)
            | DemoAttempt::Failed(ref error)
            | DemoAttempt::Gone(ref error)) => {
                // Not an error log: "the demo is not up yet" is the expected
                // state for a match that just ended, and the portal decides
                // whether it is worth another attempt.
                warn!(
                    match_id = %job.id,
                    attempt = job.attempt,
                    max = job.max_attempts,
                    outcome = failure.outcome(),
                    error,
                    "Demo attempt did not produce ranks"
                );
                DemoResultRequest {
                    outcome: failure.outcome(),
                    error: Some(error.clone()),
                    player_ratings: None,
                    map_name: None,
                }
            }
        };

        if let Err(e) = portal.submit_demo_result(&job.id, &request).await {
            // The row stays leased; it becomes eligible again when the lease
            // expires, having already spent this attempt.
            error!(match_id = %job.id, "Failed to report demo result: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use steam_vent::EResult;

    /// The throttle that actually took the bot down arrives as an EResult on
    /// the header, not as a `LoginError` — it must not be filed as "network".
    #[test]
    fn login_throttles_classify_as_rate_limit() {
        for result in [
            EResult::RateLimitExceeded,
            EResult::AccountLoginDeniedThrottle,
        ] {
            let e = ConnectionError::Network(NetworkError::ApiError(result));
            assert_eq!(classify_logon_failure(&e), "rate-limit", "{result:?}");
        }
    }

    #[test]
    fn guard_and_credential_eresults_keep_their_own_names() {
        let cases = [
            (EResult::AccountLoginDeniedNeedTwoFactor, "steam-guard"),
            (EResult::AccountLogonDenied, "steam-guard"),
            (EResult::InvalidPassword, "bad-creds"),
            (EResult::Suspended, "suspended"),
        ];
        for (result, expected) in cases {
            let e = ConnectionError::Network(NetworkError::ApiError(result));
            assert_eq!(classify_logon_failure(&e), expected, "{result:?}");
        }
    }

    /// Transport failures with no EResult stay "network", and the
    /// `LoginError` arm keeps working — the new arm must not shadow it.
    #[test]
    fn non_eresult_failures_are_unchanged() {
        assert_eq!(
            classify_logon_failure(&ConnectionError::Network(NetworkError::Timeout)),
            "network"
        );
        assert_eq!(
            classify_logon_failure(&ConnectionError::LoginError(LoginError::InvalidCredentials)),
            "bad-creds"
        );
        assert_eq!(
            classify_logon_failure(&ConnectionError::Aborted),
            "guard-timeout"
        );
    }
}
