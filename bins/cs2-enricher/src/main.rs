//! CS2 Match Enricher Bot
//!
//! Fetches full match data from the CS2 Game Coordinator for pending
//! discovered matches, then submits the enriched data back to the Portal API.

use clap::Parser;
use cs2_demo_rank::RankUpdate;
use cs2_gc::Cs2GcClient;
use parallel_bzip2_decoder::{decompress_block, scan_blocks};
use rayon::prelude::*;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use steam_vent::auth::{
    ConsoleAuthConfirmationHandler, FileGuardDataStore, SharedSecretAuthConfirmationHandler,
};
use steam_vent::{Connection, ServerList};
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

async fn connect_gc(args: &Args) -> Result<Cs2GcClient, Box<dyn std::error::Error>> {
    let password = match args.password {
        Some(ref p) => p.clone(),
        None => rpassword::prompt_password("Steam password: ")?,
    };

    info!("Discovering Steam CM servers...");
    let server_list = ServerList::discover().await?;

    info!(username = %args.username, "Logging in to Steam...");
    let guard_data = FileGuardDataStore::user_cache();

    let connection = if let Some(ref secret) = args.shared_secret {
        Connection::login(
            &server_list,
            &args.username,
            &password,
            guard_data,
            SharedSecretAuthConfirmationHandler::new(secret),
        )
        .await?
    } else {
        Connection::login(
            &server_list,
            &args.username,
            &password,
            guard_data,
            ConsoleAuthConfirmationHandler::default(),
        )
        .await?
    };

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

    let mut gc = match connect_gc(&args).await {
        Ok(gc) => gc,
        Err(e) => {
            error!("Failed to connect to Steam GC: {e}");
            std::process::exit(1);
        }
    };

    let portal = PortalClient::new(&args.portal_api_url, &args.portal_api_key);
    let interval = Duration::from_secs(args.enrich_interval);

    loop {
        if let Err(e) = enrich_cycle(
            &portal,
            &mut gc,
            &args.game_slug,
            args.batch_size,
            args.skip_demo_rank,
        )
        .await
        {
            error!("Enrich cycle error: {e}");
        }

        tokio::time::sleep(interval).await;
    }
}

async fn enrich_cycle(
    portal: &PortalClient,
    gc: &mut Cs2GcClient,
    game_slug: &str,
    batch_size: i64,
    skip_demo_rank: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let pending = portal.get_pending_matches(game_slug, batch_size).await?;

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
                                let ratings = if entries.is_empty() {
                                    None
                                } else {
                                    Some(entries)
                                };
                                (ratings, extraction.map_name)
                            }
                            Err(e) => {
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

                if let Err(e) = portal
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
                    error!(match_id = %m.id, "Failed to submit enriched data: {e}");
                }
            }
            Err(e) => {
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
