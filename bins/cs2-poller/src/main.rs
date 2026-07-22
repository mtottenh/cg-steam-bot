//! CS2 Share Code Poller Bot
//!
//! Polls the Steam Web API for new match share codes on behalf of tracked players,
//! then submits discovered codes to the Portal API.

use clap::Parser;
use cs2_webapi::Cs2WebApiClient;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{error, info, warn};

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
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
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

    loop {
        if let Err(e) = poll_cycle(&portal, &steam, &args.game_slug).await {
            error!("Poll cycle error: {e}");
        }

        tokio::time::sleep(interval).await;
    }
}

async fn poll_cycle(
    portal: &PortalClient,
    steam: &Cs2WebApiClient,
    game_slug: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let entries = portal.get_active_tracking(game_slug).await?;
    info!(count = entries.len(), "Fetched active tracking entries");

    for entry in &entries {
        // Skip entries with too many errors (backoff)
        if entry.poll_errors >= 10 {
            continue;
        }

        let Some(known_code) = &entry.last_known_code else {
            // No cursor yet — user needs to set initial share code or we need
            // a different discovery method. Skip for now.
            info!(
                tracking_id = %entry.id,
                steam_id = entry.steam_id_64,
                "No last_known_code — skipping (needs initial share code)"
            );
            continue;
        };

        match steam
            .codes_since(entry.steam_id_64 as u64, &entry.game_auth_code, known_code)
            .await
        {
            Ok(codes) if codes.is_empty() => {
                // No new matches — report success
                if let Err(e) = portal
                    .update_poll_result(
                        &entry.id,
                        &PollResultRequest {
                            last_known_code: None,
                            error: None,
                        },
                    )
                    .await
                {
                    warn!(tracking_id = %entry.id, "Failed to update poll result: {e}");
                }
            }
            Ok(codes) => {
                info!(
                    tracking_id = %entry.id,
                    steam_id = entry.steam_id_64,
                    count = codes.len(),
                    "Discovered new share codes"
                );

                let newest_code = codes.last().expect("non-empty").to_string();

                // Convert share codes to match entries
                let match_entries: Vec<MatchEntry> = codes
                    .iter()
                    .map(|sc| MatchEntry {
                        share_code: sc.to_string(),
                        match_id: sc.match_id as i64,
                        outcome_id: sc.outcome_id as i64,
                        token: sc.token as i32,
                    })
                    .collect();

                // Submit to Portal API
                if let Err(e) = portal
                    .submit_matches(&SubmitMatchesRequest {
                        tracking_id: entry.id.clone(),
                        game: game_slug.to_string(),
                        matches: match_entries,
                    })
                    .await
                {
                    error!(tracking_id = %entry.id, "Failed to submit matches: {e}");
                    continue;
                }

                // Update cursor
                if let Err(e) = portal
                    .update_poll_result(
                        &entry.id,
                        &PollResultRequest {
                            last_known_code: Some(newest_code),
                            error: None,
                        },
                    )
                    .await
                {
                    warn!(tracking_id = %entry.id, "Failed to update poll cursor: {e}");
                }
            }
            Err(e) => {
                warn!(
                    tracking_id = %entry.id,
                    steam_id = entry.steam_id_64,
                    error = %e,
                    "Poll failed"
                );

                if let Err(e2) = portal
                    .update_poll_result(
                        &entry.id,
                        &PollResultRequest {
                            last_known_code: None,
                            error: Some(e.to_string()),
                        },
                    )
                    .await
                {
                    warn!(tracking_id = %entry.id, "Failed to report poll error: {e2}");
                }
            }
        }
    }

    Ok(())
}
