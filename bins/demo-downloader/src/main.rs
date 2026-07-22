//! Dev tool: download recent CS2 demos for a player by SteamID64.
//!
//! Queries the Portal API's internal endpoint for recent enriched matches
//! with demo URLs, downloads each `.dem.bz2`, decompresses, and saves
//! the raw `.dem` files to an output directory.
//!
//! Usage:
//!   demo-downloader --steam-id 76561198012345678
//!   demo-downloader --steam-id 76561198012345678 --limit 3 --output ./demos

use clap::Parser;
use parallel_bzip2_decoder::{decompress_block, scan_blocks};
use rayon::prelude::*;
use reqwest::Client;
use serde::Deserialize;
use std::path::PathBuf;
use tracing::{error, info, warn};

#[derive(Parser)]
#[command(name = "demo-downloader")]
#[command(about = "Download recent CS2 demos for a player by SteamID64")]
struct Args {
    /// SteamID64 of the player.
    #[arg(long)]
    steam_id: i64,

    /// Portal API base URL.
    #[arg(
        long,
        env = "PORTAL_API_URL",
        default_value = "http://localhost:3000/v1"
    )]
    portal_api_url: String,

    /// Portal API key.
    #[arg(long, env = "PORTAL_API_KEY")]
    portal_api_key: String,

    /// Game slug.
    #[arg(long, default_value = "cs2")]
    game: String,

    /// Number of demos to download.
    #[arg(long, default_value = "5")]
    limit: i64,

    /// Output directory for downloaded demos.
    #[arg(long, default_value = "./demos")]
    output: PathBuf,

    /// Keep the compressed .dem.bz2 files alongside the decompressed .dem files.
    #[arg(long, default_value = "false")]
    keep_bz2: bool,
}

#[derive(Debug, Deserialize)]
struct EnrichedMatchResponse {
    id: String,
    share_code: String,
    demo_url: String,
    // Present in the API payload; retained for documentation, not consumed here.
    #[allow(dead_code)]
    enriched_at: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "demo_downloader=info".into()),
        )
        .init();

    let args = Args::parse();
    let http = Client::new();

    // Create output directory
    std::fs::create_dir_all(&args.output)?;

    // Query the Portal API for recent demos
    info!(
        steam_id = args.steam_id,
        game = %args.game,
        limit = args.limit,
        "Fetching recent demo URLs from Portal API"
    );

    let base_url = args.portal_api_url.trim_end_matches('/');
    let matches: Vec<EnrichedMatchResponse> = http
        .get(format!(
            "{base_url}/internal/discovered-matches/recent-demos"
        ))
        .header("X-API-Key", &args.portal_api_key)
        .query(&[
            ("game", args.game.as_str()),
            ("steam_id_64", &args.steam_id.to_string()),
            ("limit", &args.limit.to_string()),
        ])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    if matches.is_empty() {
        warn!("No enriched matches with demo URLs found for this player");
        return Ok(());
    }

    info!(count = matches.len(), "Found matches with demo URLs");

    for m in &matches {
        let file_stem = format!("{}_{}", m.share_code, m.id);
        // Sanitize share_code for filesystem
        let file_stem = file_stem.replace(['/', '\\', ':'], "_");
        let dem_path = args.output.join(format!("{file_stem}.dem"));

        if dem_path.exists() {
            info!(path = %dem_path.display(), "Already exists, skipping");
            continue;
        }

        info!(
            share_code = %m.share_code,
            url = %m.demo_url,
            "Downloading demo"
        );

        let response = match http
            .get(&m.demo_url)
            .timeout(std::time::Duration::from_secs(180))
            .send()
            .await
        {
            Ok(r) => match r.error_for_status() {
                Ok(r) => r,
                Err(e) => {
                    error!(share_code = %m.share_code, error = %e, "HTTP error downloading demo");
                    continue;
                }
            },
            Err(e) => {
                error!(share_code = %m.share_code, error = %e, "Failed to download demo");
                continue;
            }
        };

        let compressed = response.bytes().await?;
        info!(
            compressed_bytes = compressed.len(),
            "Downloaded, decompressing bzip2"
        );

        // Optionally save compressed file
        if args.keep_bz2 {
            let bz2_path = args.output.join(format!("{file_stem}.dem.bz2"));
            std::fs::write(&bz2_path, &compressed)?;
            info!(path = %bz2_path.display(), "Saved compressed demo");
        }

        // Decompress
        let decompressed = match (|| -> Result<Vec<u8>, parallel_bzip2_decoder::Bz2Error> {
            let blocks: Vec<(u64, u64)> = scan_blocks(&compressed).into_iter().collect();
            let parts: Vec<Vec<u8>> = blocks
                .par_iter()
                .map(|&(start, end)| decompress_block(&compressed, start, end))
                .collect::<Result<Vec<_>, _>>()?;
            let total_size: usize = parts.iter().map(|p| p.len()).sum();
            let mut decompressed = Vec::with_capacity(total_size);
            for part in parts {
                decompressed.extend_from_slice(&part);
            }
            Ok(decompressed)
        })() {
            Ok(d) => d,
            Err(e) => {
                error!(share_code = %m.share_code, error = %e, "Failed to decompress demo");
                continue;
            }
        };

        std::fs::write(&dem_path, &decompressed)?;
        info!(
            path = %dem_path.display(),
            decompressed_bytes = decompressed.len(),
            "Saved decompressed demo"
        );
    }

    info!("Done!");
    Ok(())
}
