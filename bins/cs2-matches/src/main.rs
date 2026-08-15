//! CS2 Match History CLI.
//!
//! Fetches recent matches for yourself or a friend via the CS2 Game
//! Coordinator. Requires a dedicated bot Steam account.
//!
//! ## Usage
//!
//! ```bash
//! # Fully automated (no prompts)
//! STEAM_PASSWORD=xxx STEAM_SHARED_SECRET=xxx cs2-matches --username bot_account
//!
//! # Interactive (prompts for password + email/TOTP code)
//! cs2-matches --username bot_account
//!
//! # A friend's matches (they must be on the bot's friends list)
//! cs2-matches --username bot_account --target 52079950
//!
//! # Look up a match by share code
//! cs2-matches --username bot_account --sharecode CSGO-xxxxx-xxxxx-xxxxx-xxxxx-xxxxx
//!
//! # JSON output for programmatic consumption
//! cs2-matches --username bot_account --json
//!
//! # With debug logging
//! RUST_LOG=debug cs2-matches --username bot_account
//! ```
//!
//! ## Account Setup
//!
//! 1. Create a free Steam account for the bot
//! 2. Add CS2 to its library (free)
//! 3. Add yourself (and anyone you want to look up) as a friend
//! 4. Ideally get Prime status on the bot account
//! 5. Set up Steam Mobile Authenticator to get a shared secret for TOTP

use clap::Parser;
use cs2_gc::types::{MatchInfo, OwnProfile};
use cs2_gc::Cs2GcClient;
use cs2_sharecode::ShareCode;
use cs2_webapi::Cs2WebApiClient;
use steam_vent::auth::{
    ConsoleAuthConfirmationHandler, FileGuardDataStore, SharedSecretAuthConfirmationHandler,
};
use steam_vent::{Connection, ServerList};
use tracing::{info, warn};

#[derive(Parser)]
#[command(name = "cs2-matches", about = "Fetch CS2 recent matches via GC")]
struct Args {
    /// Steam username for the bot account
    #[arg(short, long)]
    username: String,

    /// Steam password. Falls back to STEAM_PASSWORD env var,
    /// then interactive prompt.
    #[arg(long, env = "STEAM_PASSWORD", hide_env_values = true)]
    password: Option<String>,

    /// TOTP shared secret (base64) for automatic login.
    /// Set up Steam Mobile Authenticator to get this.
    /// Falls back to STEAM_SHARED_SECRET env var.
    #[arg(long, env = "STEAM_SHARED_SECRET", hide_env_values = true)]
    shared_secret: Option<String>,

    /// Player to look up (SteamID64 or 32-bit Account ID).
    /// Omit to fetch your own matches.
    ///
    /// Values above 76561197960265728 are treated as SteamID64 and
    /// automatically converted.
    #[arg(short, long)]
    target: Option<u64>,

    /// Look up a match by share code (e.g. CSGO-xxxxx-xxxxx-xxxxx-xxxxx-xxxxx)
    #[arg(short, long)]
    sharecode: Option<String>,

    /// Steam Web API key (enables share code polling via Web API).
    #[arg(long, env = "STEAM_WEB_API_KEY", hide_env_values = true)]
    api_key: Option<String>,

    /// Player's game auth code (for Web API share code polling).
    /// Format: AAAA-AAAAA-AAAA
    #[arg(long, env = "STEAM_AUTH_CODE", hide_env_values = true)]
    auth_code: Option<String>,

    /// Output results as JSON instead of human-readable text
    #[arg(long)]
    json: bool,

    /// Enable verbose debug logging (shows steam-vent message flow)
    #[arg(short, long)]
    verbose: bool,
}

fn print_profile(p: &OwnProfile) {
    println!("  Account ID:   {}", p.account_id);
    println!("  Player Level: {}", p.player_level);
    println!("  XP:           {}", p.player_cur_xp);
    if p.rankings.is_empty() {
        println!("  Rankings:     (none)");
    } else {
        for r in &p.rankings {
            println!(
                "  {:12}  {} ({} wins)",
                r.rank_type.name(),
                r.display(),
                r.wins
            );
        }
    }
}

fn print_match(m: &MatchInfo, i: usize) {
    println!("Match #{}", i + 1);
    println!("  ID:       {}", m.match_id);
    println!("  Time:     {}", m.time_display());
    println!(
        "  Map:      {}",
        if m.map.is_empty() { "?" } else { &m.map }
    );
    println!("  Score:    {}", m.score_display());
    println!(
        "  Duration: {}m {}s",
        m.match_duration_secs / 60,
        m.match_duration_secs % 60
    );

    if let Some(ref parts) = m.share_code_parts {
        println!("  Share:    {parts}");
    }

    if let Some(ref demo) = m.demo {
        if let Some(url) = demo.download_url() {
            println!("  Demo:     {url}");
        }
    }

    if !m.players.is_empty() {
        println!(
            "  {:>12}  {:>3} {:>3} {:>3} {:>5} {:>2} {:>3}",
            "Account", "K", "A", "D", "Score", "HS", "MVP"
        );
        println!("  {}", "─".repeat(42));
        for p in &m.players {
            println!(
                "  {:>12}  {:>3} {:>3} {:>3} {:>5} {:>2} {:>3}",
                p.account_id, p.kills, p.assists, p.deaths, p.score, p.headshots, p.mvps
            );
        }
    }
    println!();
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

async fn run() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Parse share code early so we fail fast on bad input
    let share_code = args
        .sharecode
        .as_deref()
        .map(ShareCode::decode)
        .transpose()?;

    let webapi_mode = args.api_key.is_some() && args.auth_code.is_some() && share_code.is_some();

    // In JSON mode, suppress log output so stdout is clean JSON
    if !args.json {
        let default_filter = if args.verbose {
            "steam_vent=debug,cs2_gc=debug,cs2_matches=debug,cs2_webapi=debug"
        } else {
            "cs2_gc=info,cs2_matches=info,cs2_webapi=info"
        };
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| default_filter.into()),
            )
            .init();
    }

    if webapi_mode {
        return run_webapi_mode(&args, share_code.unwrap()).await;
    }

    // ── GC mode: requires Steam login ──
    let (mut cs2, profile) = connect_gc(&args).await?;

    let matches = if let Some(sc) = share_code {
        if !args.json {
            println!("[*] Looking up match from share code...");
        }
        cs2.match_info(sc.match_id, sc.outcome_id, sc.token as u32)
            .await?
    } else {
        let target = args
            .target
            .map(to_account_id)
            .or(profile.as_ref().map(|p| p.account_id));

        let target = target
            .ok_or("--target is required when CS2 hello fails (can't resolve own account ID)")?;

        let is_self = profile.as_ref().is_some_and(|p| p.account_id == target);

        if !args.json {
            println!(
                "\n[*] Fetching recent matches for {}{}...",
                target,
                if is_self { " (you)" } else { "" }
            );
        }
        cs2.recent_matches(target).await?
    };

    // ── Output ──
    output_results(args.json, profile.as_ref(), &matches);
    Ok(())
}

/// Web API mode: discover via Web API first, then connect to GC only if
/// there are new codes to enrich.
async fn run_webapi_mode(
    args: &Args,
    known_code: ShareCode,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let api_key = args.api_key.as_deref().unwrap();
    let auth_code_str = args.auth_code.as_deref().unwrap();

    // The target must be specified as a SteamID64 or account_id when using
    // Web API mode without a GC connection (we don't have our own profile yet).
    let target_id = args
        .target
        .map(to_steam_id64)
        .ok_or("--target is required in Web API mode (no GC to resolve own account ID)")?;

    if !args.json {
        println!("[*] Discovering new matches via Steam Web API...");
    }

    let webapi = Cs2WebApiClient::new(api_key);
    let walk = webapi
        .codes_since(target_id, auth_code_str, &known_code.encode())
        .await;

    // A walk that stopped early still found real matches, so use them rather
    // than discarding the lot. But an empty result from a *failed* walk is not
    // "no new matches" — reporting it as such would be a lie to the caller.
    let new_codes = match (walk.error, walk.codes.is_empty()) {
        (Some(e), true) => return Err(e.into()),
        (Some(e), false) => {
            eprintln!(
                "[!] Walk stopped early after {} match(es): {e}",
                walk.codes.len()
            );
            walk.codes
        }
        (None, _) => walk.codes,
    };

    if !args.json {
        println!("[+] Found {} new match(es).", new_codes.len());
    }

    if new_codes.is_empty() {
        output_results(args.json, None, &[]);
        return Ok(());
    }

    // New codes found — connect to GC to enrich with full match data
    let (mut cs2, profile) = connect_gc(args).await?;

    let mut matches = Vec::new();
    for sc in &new_codes {
        info!(code = %sc, "Enriching match via GC");
        if !args.json {
            println!("[*] Fetching match details for {sc}...");
        }
        let mut info = cs2
            .match_info(sc.match_id, sc.outcome_id, sc.token as u32)
            .await?;
        matches.append(&mut info);
    }

    output_results(args.json, profile.as_ref(), &matches);
    Ok(())
}

/// Connect to Steam and the CS2 Game Coordinator.
///
/// Returns the GC client and optionally the bot's profile (if the CS2
/// hello succeeds). The GC is usable for match queries either way.
async fn connect_gc(
    args: &Args,
) -> std::result::Result<(Cs2GcClient, Option<OwnProfile>), Box<dyn std::error::Error>> {
    let password = match args.password {
        Some(ref p) => p.clone(),
        None => rpassword::prompt_password("Steam password: ")?,
    };

    if !args.json {
        println!("[*] Discovering Steam CM servers...");
    }
    let server_list = ServerList::discover().await?;

    if !args.json {
        println!("[*] Logging in as '{}'...", args.username);
    }

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

    if !args.json {
        println!("[+] Logged in.");
    }

    if !args.json {
        println!("[*] Connecting to CS2 Game Coordinator...");
    }
    let mut cs2 = Cs2GcClient::connect(connection).await?;

    if !args.json {
        println!("[*] Requesting CS2 profile (may take a few attempts)...");
    }
    let profile = match cs2.hello().await {
        Ok(p) => Some(p),
        Err(e) => {
            if !args.json {
                println!("[!] CS2 hello failed ({e}), continuing without profile.");
            }
            warn!("CS2 hello failed: {e}");
            None
        }
    };

    Ok((cs2, profile))
}

fn output_results(json: bool, profile: Option<&OwnProfile>, matches: &[MatchInfo]) {
    if json {
        let output = serde_json::json!({
            "profile": profile,
            "matches": matches,
        });
        println!("{}", serde_json::to_string_pretty(&output).unwrap());
    } else {
        if let Some(p) = profile {
            println!("\n═══ YOUR PROFILE ═══");
            print_profile(p);
        }

        if matches.is_empty() {
            println!("\n  No matches found.");
        } else {
            println!("\n═══ MATCHES ({}) ═══\n", matches.len());
            for (i, m) in matches.iter().enumerate() {
                print_match(m, i);
            }
        }

        println!("[+] Done.");
    }
}

const STEAM_ID64_BASE: u64 = 76561197960265728;

/// Normalize a target value to a SteamID64.
/// Values >= STEAM_ID64_BASE are already SteamID64; smaller values are account IDs.
fn to_steam_id64(value: u64) -> u64 {
    if value >= STEAM_ID64_BASE {
        value
    } else {
        STEAM_ID64_BASE + value
    }
}

/// Normalize a target value to a 32-bit account ID.
/// Values >= STEAM_ID64_BASE are SteamID64 and get the base subtracted.
fn to_account_id(value: u64) -> u32 {
    if value >= STEAM_ID64_BASE {
        (value - STEAM_ID64_BASE) as u32
    } else {
        value as u32
    }
}
