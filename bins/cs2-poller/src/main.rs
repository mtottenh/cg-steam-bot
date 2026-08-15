//! CS2 Share Code Poller Bot
//!
//! Polls the Steam Web API for new match share codes on behalf of tracked players,
//! then submits discovered codes to the Portal API.

use clap::Parser;
use cs2_webapi::Cs2WebApiClient;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// CS2 match poller bot.
#[derive(Parser)]
#[command(name = "cs2-poller")]
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

    /// Steam Web API key.
    #[arg(long, env = "STEAM_WEB_API_KEY")]
    steam_api_key: String,

    /// Poll interval in seconds.
    #[arg(long, env = "POLL_INTERVAL_SECS", default_value = "60")]
    poll_interval: u64,

    /// Game slug to poll for.
    #[arg(long, env = "GAME_SLUG", default_value = "cs2")]
    game_slug: String,
}

// =============================================================================
// Portal API client types
// =============================================================================

#[derive(Debug, Deserialize)]
struct TrackingEntry {
    id: String,
    steam_id_64: i64,
    game_auth_code: String,
    last_known_code: Option<String>,
    /// Consecutive transient failures, for logging only.
    ///
    /// The poller no longer acts on this. Deciding when to retry from a
    /// counter it held in a request body is exactly what went wrong: the
    /// threshold lived here, the state lived in the portal, and nothing could
    /// reconcile them. The portal now returns only entries that are due.
    #[allow(dead_code)]
    poll_errors: i32,
}

#[derive(Debug, Serialize)]
struct SubmitMatchesRequest {
    tracking_id: String,
    game: String,
    matches: Vec<MatchEntry>,
}

#[derive(Debug, Serialize)]
struct MatchEntry {
    share_code: String,
    match_id: i64,
    outcome_id: i64,
    token: i32,
}

#[derive(Debug, Serialize)]
struct PollResultRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    last_known_code: Option<String>,
    /// How the poll ended. The portal schedules from this — a revoked token
    /// and a network blip need opposite responses, and reporting only an
    /// error string cannot tell them apart.
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Map a Steam Web API failure onto the portal's scheduling vocabulary.
///
/// This classification is the whole fix. The old code counted every failure
/// identically and abandoned the entry at ten, which meant a rate-limit storm
/// could permanently kill every tracked player, while a genuinely revoked
/// token was retried nine pointless times before going quiet with no
/// explanation of what a human should do about it.
const fn classify(error: &cs2_webapi::Error) -> &'static str {
    match error {
        // Steam rejected the auth code. Retrying cannot help; only the player
        // supplying a new one can.
        cs2_webapi::Error::BadAuthCode(_) => "auth-expired",
        // Steam rejected our cursor. Retrying with the same cursor cannot help.
        cs2_webapi::Error::BadKnownCode(_) => "cursor-invalid",
        // Ours, not theirs: back the entry off without holding it responsible.
        cs2_webapi::Error::RateLimited => "rate-limited",
        // Network, 5xx, decode failures — worth trying again, indefinitely.
        _ => "transient",
    }
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

    async fn get_active_tracking(&self, game: &str) -> Result<Vec<TrackingEntry>, reqwest::Error> {
        self.http
            .get(format!("{}/internal/steam-tracking/active", self.base_url))
            .header("X-API-Key", &self.api_key)
            .query(&[("game", game)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
    }

    async fn submit_matches(&self, req: &SubmitMatchesRequest) -> Result<(), reqwest::Error> {
        self.http
            .post(format!("{}/internal/discovered-matches", self.base_url))
            .header("X-API-Key", &self.api_key)
            .json(req)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }

    async fn update_poll_result(
        &self,
        tracking_id: &str,
        req: &PollResultRequest,
    ) -> Result<(), reqwest::Error> {
        self.http
            .patch(format!(
                "{}/internal/steam-tracking/{tracking_id}/poll-result",
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
// Main loop
// =============================================================================

/// Coarse buckets for whole poll cycles: 1 req/s Steam rate limiting means
/// a cycle scales with tracked players × new codes.
const CYCLE_DURATION_BUCKETS: &[f64] = &[0.1, 0.5, 1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0];

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "cs2_poller=info".into()),
        )
        .init();

    let args = Args::parse();

    info!(
        portal_url = %args.portal_api_url,
        game = %args.game_slug,
        poll_interval = args.poll_interval,
        "Starting CS2 poller bot"
    );

    let portal = PortalClient::new(&args.portal_api_url, &args.portal_api_key);
    let steam = Cs2WebApiClient::new(&args.steam_api_key);
    let interval = Duration::from_secs(args.poll_interval);

    // /healthz goes stale (503) when no cycle has succeeded for 3×interval —
    // the same window the StaleLoop alert uses on the metric.
    let health = portal_daemon::Health::new(interval * 3);
    portal_daemon::start_from_env(
        "cs2_poller_build_info",
        env!("CARGO_PKG_VERSION"),
        &[("cs2_poller_cycle_duration_seconds", CYCLE_DURATION_BUCKETS)],
        std::sync::Arc::clone(&health),
    );
    portal_daemon::notify_ready();

    let shutdown = portal_daemon::shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        let start = std::time::Instant::now();
        // Per-cycle deadline = poll_interval: an overrunning cycle is
        // cancelled-and-counted rather than queueing behind the ticker.
        // Shutdown also interrupts mid-cycle — the poll cursor only
        // advances after successful submission, so nothing is lost.
        let outcome = tokio::select! {
            cycle = tokio::time::timeout(interval, poll_cycle(&portal, &steam, &args.game_slug)) => {
                match cycle {
                    Ok(Ok(())) => {
                        health.mark_success();
                        metrics::gauge!("cs2_poller_last_success_timestamp_seconds")
                            .set(portal_daemon::unix_now_f64());
                        "ok"
                    }
                    Ok(Err(e)) => {
                        error!("Poll cycle error: {e}");
                        "error"
                    }
                    Err(_) => {
                        warn!(
                            deadline_secs = interval.as_secs(),
                            "Poll cycle exceeded its deadline; skipping to next tick"
                        );
                        "deadline-exceeded"
                    }
                }
            }
            () = &mut shutdown => break,
        };
        metrics::histogram!("cs2_poller_cycle_duration_seconds")
            .record(start.elapsed().as_secs_f64());
        metrics::counter!("cs2_poller_cycles_total", "outcome" => outcome).increment(1);

        // Jittered sleep: avoids thundering-herd against Steam when several
        // bots share a box.
        let sleep = interval + portal_daemon::jitter(interval / 10);
        tokio::select! {
            () = tokio::time::sleep(sleep) => {}
            () = &mut shutdown => break,
        }
    }

    portal_daemon::notify_stopping();
    info!("shutdown complete");
}

async fn poll_cycle(
    portal: &PortalClient,
    steam: &Cs2WebApiClient,
    game_slug: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let entries = portal.get_active_tracking(game_slug).await?;
    // Idle-cycle logging stays at debug (observability-design.md §5): the
    // count lives in the gauge; Loki's error-rate signal stays meaningful.
    debug!(count = entries.len(), "Fetched active tracking entries");
    metrics::gauge!("cs2_poller_tracked_players").set(entries.len() as f64);

    for entry in &entries {
        // No client-side skipping. The portal returns only entries that are
        // active, unpaused and due — it owns the schedule, because it is the
        // only side that can record one durably. The old `poll_errors >= 10`
        // check here was a one-way door: a skipped entry never got a
        // successful poll, `poll_errors` only reset on success, so nothing the
        // system could do would ever bring it back.

        let Some(known_code) = &entry.last_known_code else {
            // No cursor yet — user needs to set initial share code or we need
            // a different discovery method. Skip for now.
            debug!(
                tracking_id = %entry.id,
                steam_id = entry.steam_id_64,
                "No last_known_code — skipping (needs initial share code)"
            );
            continue;
        };

        // The walk returns codes AND an error, not one or the other: codes
        // found before a mid-walk failure are still new matches, and banking
        // them is what lets the cursor advance past them.
        let walk = steam
            .codes_since(entry.steam_id_64 as u64, &entry.game_auth_code, known_code)
            .await;

        let outcome = walk.error.as_ref().map_or("ok", classify);
        metrics::counter!("cs2_poller_steam_requests_total", "outcome" => outcome).increment(1);

        // Submit whatever the walk found, regardless of how it ended.
        let mut newest_code = None;
        if !walk.codes.is_empty() {
            info!(
                tracking_id = %entry.id,
                steam_id = entry.steam_id_64,
                count = walk.codes.len(),
                partial = walk.error.is_some(),
                "Discovered new share codes"
            );
            metrics::counter!("cs2_poller_sharecodes_discovered_total")
                .increment(walk.codes.len() as u64);

            let match_entries: Vec<MatchEntry> = walk
                .codes
                .iter()
                .map(|sc| MatchEntry {
                    share_code: sc.to_string(),
                    match_id: sc.match_id as i64,
                    outcome_id: sc.outcome_id as i64,
                    token: sc.token as i32,
                })
                .collect();

            match portal
                .submit_matches(&SubmitMatchesRequest {
                    tracking_id: entry.id.clone(),
                    game: game_slug.to_string(),
                    matches: match_entries,
                })
                .await
            {
                Ok(()) => {
                    metrics::counter!("cs2_poller_submissions_total", "outcome" => "ok")
                        .increment(1);
                    // Only advance the cursor once the codes are safely
                    // submitted — advancing first would skip them for good.
                    newest_code = walk.codes.last().map(ToString::to_string);
                }
                Err(e) => {
                    metrics::counter!("cs2_poller_submissions_total", "outcome" => "error")
                        .increment(1);
                    error!(tracking_id = %entry.id, "Failed to submit matches: {e}");
                    // Leave the cursor alone and re-walk next cycle. Skip the
                    // poll-result write entirely: reporting success would move
                    // the entry on from codes that never landed.
                    continue;
                }
            }
        }

        if let Some(error) = &walk.error {
            warn!(
                tracking_id = %entry.id,
                steam_id = entry.steam_id_64,
                outcome,
                codes_banked = walk.codes.len(),
                error = %error,
                "Poll failed"
            );
        }

        if let Err(e) = portal
            .update_poll_result(
                &entry.id,
                &PollResultRequest {
                    last_known_code: newest_code,
                    outcome,
                    error: walk.error.as_ref().map(ToString::to_string),
                },
            )
            .await
        {
            warn!(tracking_id = %entry.id, "Failed to update poll result: {e}");
        }

        // Steam is refusing us, so the rest of this cycle would be a queue of
        // guaranteed 429s. Stop here: every entry we would have touched is
        // still due next cycle, and hammering through them only deepens the
        // rate limit we are already in.
        if matches!(walk.error, Some(cs2_webapi::Error::RateLimited)) {
            warn!("Steam rate-limited us; abandoning the rest of this cycle");
            metrics::counter!("cs2_poller_cycles_cut_short_total").increment(1);
            break;
        }
    }

    Ok(())
}
