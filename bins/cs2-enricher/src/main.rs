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
use steam_vent::{ConnectionError, LoginError, ServerList};
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

    /// Enrichment interval in seconds.
    #[arg(long, env = "ENRICH_INTERVAL_SECS", default_value = "30")]
    enrich_interval: u64,

    /// Game slug to enrich for.
    #[arg(long, env = "GAME_SLUG", default_value = "cs2")]
    game_slug: String,

    /// Skip demo download and rank extraction.
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
            ConnectionError::Network(_) => "network",
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

/// Result of downloading and parsing a demo file.
struct DemoExtraction {
    ranks: Vec<RankUpdate>,
    map_name: Option<String>,
}

/// Download a `.dem.bz2` demo from Valve CDN, decompress, and extract rank updates + metadata.
async fn download_and_extract_demo(
    http: &Client,
    demo_url: &str,
) -> Result<DemoExtraction, Box<dyn std::error::Error>> {
    info!(url = %demo_url, "Downloading demo for rank extraction");

    let response = http
        .get(demo_url)
        .timeout(Duration::from_secs(120))
        .send()
        .await?
        .error_for_status()?;

    let compressed = response.bytes().await?;
    info!(
        size_bytes = compressed.len(),
        "Demo downloaded, decompressing bzip2"
    );

    let blocks: Vec<(u64, u64)> = scan_blocks(&compressed).into_iter().collect();
    let decompressed_parts: Vec<Vec<u8>> = blocks
        .par_iter()
        .map(|&(start, end)| decompress_block(&compressed, start, end))
        .collect::<Result<Vec<_>, _>>()?;
    let total_size: usize = decompressed_parts.iter().map(|p| p.len()).sum();
    let mut decompressed = Vec::with_capacity(total_size);
    for part in decompressed_parts {
        decompressed.extend_from_slice(&part);
    }

    info!(
        decompressed_bytes = decompressed.len(),
        "Decompressed, scanning for rank updates"
    );

    let ranks = cs2_demo_rank::extract_rank_updates(&decompressed)?;
    let metadata = cs2_demo_rank::extract_demo_metadata(&decompressed).unwrap_or_else(|e| {
        warn!("Failed to extract demo metadata: {e}");
        cs2_demo_rank::DemoMetadata { map_name: None }
    });

    info!(
        rank_count = ranks.len(),
        map_name = ?metadata.map_name,
        "Demo extraction complete"
    );

    Ok(DemoExtraction {
        ranks,
        map_name: metadata.map_name,
    })
}

// =============================================================================
// Main loop
// =============================================================================

/// Enrichment of one batch can legitimately take minutes (GC calls at 2s
/// spacing + up to 120s per demo download), so the cycle deadline is a
/// generous backstop, not the poll interval.
const CYCLE_DEADLINE: Duration = Duration::from_secs(600);

/// Coarse buckets for per-match enrichment (GC fetch + demo download +
/// bzip2 + rank scan).
const ENRICH_DURATION_BUCKETS: &[f64] = &[1.0, 2.5, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0];

/// Reconnect backoff bounds for the GC session.
const RECONNECT_BACKOFF_INITIAL: Duration = Duration::from_secs(5);
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(300);

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
        &[(
            "cs2_enricher_enrich_duration_seconds",
            ENRICH_DURATION_BUCKETS,
        )],
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
                    error!(
                        reason,
                        backoff_secs = backoff.as_secs(),
                        "Failed to connect to Steam GC: {e}"
                    );
                    tokio::select! {
                        () = tokio::time::sleep(backoff) => {}
                        () = &mut shutdown => break,
                    }
                    backoff = (backoff * 2).min(RECONNECT_BACKOFF_MAX);
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
                enrich_cycle(&portal, gc_client, &args.game_slug, args.batch_size, args.skip_demo_rank),
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
    skip_demo_rank: bool,
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

                // Attempt to download demo and extract rank data + metadata
                let (player_ratings, map_name) = if !skip_demo_rank {
                    if let Some(ref url) = demo_url {
                        match download_and_extract_demo(&portal.http, url).await {
                            Ok(extraction) => {
                                let entries: Vec<PlayerRatingEntry> = extraction
                                    .ranks
                                    .into_iter()
                                    .map(|r| PlayerRatingEntry {
                                        account_id: r.account_id,
                                        rank_id: r.rank_id,
                                        rank_type_id: r.rank_type_id,
                                        wins: r.wins,
                                        rank_change: r.rank_change,
                                    })
                                    .collect();
                                // "empty" is a real third outcome: casual/DM
                                // demos parse fine but carry no rank updates.
                                let outcome = if entries.is_empty() { "empty" } else { "ok" };
                                metrics::counter!(
                                    "cs2_enricher_rank_extractions_total",
                                    "outcome" => outcome
                                )
                                .increment(1);
                                let ratings = if entries.is_empty() {
                                    None
                                } else {
                                    Some(entries)
                                };
                                (ratings, extraction.map_name)
                            }
                            Err(e) => {
                                metrics::counter!(
                                    "cs2_enricher_rank_extractions_total",
                                    "outcome" => "error"
                                )
                                .increment(1);
                                warn!(
                                    match_id = %m.id,
                                    error = %e,
                                    "Demo extraction failed, submitting without ratings"
                                );
                                (None, None)
                            }
                        }
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                };

                info!(
                    match_id = %m.id,
                    has_demo = demo_url.is_some(),
                    has_ratings = player_ratings.is_some(),
                    map_name = ?map_name,
                    match_count = matches.len(),
                    "Got GC data, submitting enriched result"
                );

                match portal
                    .submit_enriched(
                        &m.id,
                        &EnrichedMatchRequest {
                            gc_data: &matches,
                            demo_url,
                            player_ratings,
                            map_name,
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
