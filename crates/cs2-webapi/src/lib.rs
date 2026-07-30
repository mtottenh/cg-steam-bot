//! # cs2-webapi
//!
//! Lightweight Steam Web API client for discovering CS2 match share codes.
//!
//! Uses the `GetNextMatchSharingCode` endpoint to walk forward through a
//! player's match history. No Game Coordinator connection required — just a
//! Steam Web API key and the player's Game Authentication Code.
//!
//! ## Usage
//!
//! ```no_run
//! use cs2_webapi::Cs2WebApiClient;
//!
//! # async fn example() -> Result<(), cs2_webapi::Error> {
//! let client = Cs2WebApiClient::new("your-steam-api-key");
//!
//! // Single step: get the next share code after a known one
//! let next = client.next_share_code(
//!     76561198012345678,
//!     "AAAA-AAAAA-AAAA",
//!     "CSGO-xxxxx-xxxxx-xxxxx-xxxxx-xxxxx",
//! ).await?;
//!
//! // Walk: collect all codes since a known one. Returns codes AND any error
//! // together, because a walk that failed partway still found real matches
//! // and throwing them away means re-walking the same prefix forever.
//! let walk = client.codes_since(
//!     76561198012345678,
//!     "AAAA-AAAAA-AAAA",
//!     "CSGO-xxxxx-xxxxx-xxxxx-xxxxx-xxxxx",
//! ).await;
//! for code in &walk.codes {
//!     println!("{code}");
//! }
//! if let Some(e) = &walk.error {
//!     eprintln!("walk stopped early: {e}");
//! }
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;

use cs2_provider::{MatchProvider, ShareCode};
use cs2_sharecode::ShareCodeError;
pub use governor::Quota;
use governor::RateLimiter;
use tracing::debug;

const BASE_URL: &str = "https://api.steampowered.com/ICSGOPlayers_730/GetNextMatchSharingCode/v1";

/// Default quota: 1 request per second.
///
/// Well under Steam's 100,000 requests / 24h limit (~86.4k/day at this rate)
/// and conservative enough to avoid per-user 429s when polling sequentially.
fn default_quota() -> Quota {
    Quota::per_second(NonZeroU32::new(1).unwrap())
}

/// Cap on how many codes one [`Cs2WebApiClient::codes_since`] walk collects.
///
/// The walk is one serialised request per code at 1 req/s, so a player
/// returning after a long break could otherwise hold the poll cycle for
/// minutes and blow its deadline — starving every other tracked player. The
/// cursor advances by whatever was collected, so the next cycle picks up
/// exactly where this one stopped.
const MAX_CODES_PER_WALK: usize = 50;

/// Result of walking forward from a cursor.
///
/// Carries codes and error together rather than being a `Result`, because a
/// partial walk is a normal outcome with real value in it: the codes found
/// before the failure are still new matches, and banking them is what lets the
/// cursor advance.
#[derive(Debug)]
pub struct CodeWalk {
    /// Codes discovered this walk, oldest first. May be non-empty even when
    /// `error` is set.
    pub codes: Vec<ShareCode>,
    /// Why the walk stopped early, if it did. `None` means it caught up.
    pub error: Option<Error>,
}

impl CodeWalk {
    /// The newest code found, which becomes the new cursor.
    #[must_use]
    pub fn newest(&self) -> Option<&ShareCode> {
        self.codes.last()
    }
}

type Limiter = RateLimiter<
    governor::state::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::DefaultClock,
>;

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Error
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("player {0} not registered — call register_player() first")]
    PlayerNotRegistered(u64),

    #[error("bad auth code for player {0} (HTTP 403)")]
    BadAuthCode(u64),

    #[error("bad known share code for player {0} (HTTP 412)")]
    BadKnownCode(u64),

    #[error("rate limited by Steam Web API (HTTP 429)")]
    RateLimited,

    #[error("unexpected HTTP {status}: {body}")]
    UnexpectedStatus { status: u16, body: String },

    #[error("failed to parse share code: {0}")]
    ShareCodeParse(#[from] ShareCodeError),
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Cs2WebApiClient — raw, stateless HTTP client
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Raw Steam Web API client for CS2 share code discovery.
///
/// Rate-limited via [`governor`] — all requests through this client share a
/// single token bucket. Default: 1 req/s (~86.4k/day, well under Steam's
/// 100k/day key quota). Customize with [`Cs2WebApiClient::with_quota`].
pub struct Cs2WebApiClient {
    api_key: String,
    http: reqwest::Client,
    limiter: Arc<Limiter>,
}

impl Cs2WebApiClient {
    /// Create a new client with the given Steam Web API key.
    ///
    /// Uses the default rate limit of 1 request per second.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_quota(api_key, default_quota())
    }

    /// Create a new client with a custom rate limit quota.
    ///
    /// See [`governor::Quota`] for constructors like `Quota::per_second()`,
    /// `Quota::per_minute()`, etc.
    pub fn with_quota(api_key: impl Into<String>, quota: Quota) -> Self {
        Self {
            api_key: api_key.into(),
            http: reqwest::Client::new(),
            limiter: Arc::new(RateLimiter::direct(quota)),
        }
    }

    /// Fetch the next share code after `known_code` for the given player.
    ///
    /// - `steam_id`: player's SteamID64
    /// - `auth_code`: player's Game Authentication Code (format: `"AAAA-AAAAA-AAAA"`)
    /// - `known_code`: the share code string to advance from
    ///
    /// Returns `Ok(Some(code))` if a newer match exists,
    /// `Ok(None)` if no newer matches (HTTP 202).
    pub async fn next_share_code(
        &self,
        steam_id: u64,
        auth_code: &str,
        known_code: &str,
    ) -> Result<Option<ShareCode>, Error> {
        debug!(
            steam_id,
            auth_code,
            known_code,
            url = BASE_URL,
            method = "GET",
            "Fetching next share code — params: key=<redacted>, steamid={steam_id}, steamidkey={auth_code}, knowncode={known_code}"
        );

        self.limiter.until_ready().await;

        let resp = self
            .http
            .get(BASE_URL)
            .query(&[
                ("key", self.api_key.as_str()),
                ("steamid", &steam_id.to_string()),
                ("steamidkey", auth_code),
                ("knowncode", known_code),
            ])
            .send()
            .await?;

        let status = resp.status();

        match status.as_u16() {
            200 => {
                let json: serde_json::Value = resp.json().await?;
                let next_code = json["result"]["nextcode"].as_str().unwrap_or("n/a");

                if next_code == "n/a" {
                    // 200 with "n/a" means no more matches (same as 202)
                    Ok(None)
                } else {
                    let sc = ShareCode::decode(next_code)?;
                    debug!(steam_id, code = %sc, "Got next share code");
                    Ok(Some(sc))
                }
            }
            202 => {
                debug!(steam_id, "No newer matches (HTTP 202)");
                Ok(None)
            }
            403 => Err(Error::BadAuthCode(steam_id)),
            412 => Err(Error::BadKnownCode(steam_id)),
            429 => Err(Error::RateLimited),
            _ => {
                let body = resp.text().await.unwrap_or_default();
                Err(Error::UnexpectedStatus {
                    status: status.as_u16(),
                    body,
                })
            }
        }
    }

    /// Walk forward from `known_code`, collecting all subsequent share codes.
    ///
    /// Rate limiting is handled by the client's [`governor`] limiter — each
    /// call to [`next_share_code`](Self::next_share_code) awaits a token.
    /// Returns codes oldest-first.
    ///
    /// **Partial progress is returned, not discarded.** This used to be
    /// `Result<Vec<ShareCode>, Error>`, so a failure on the fourth request
    /// threw away the three codes already found AND left the cursor where it
    /// started — meaning the next cycle re-walked the same prefix and, for a
    /// player whose walk reliably broke partway, made no progress ever.
    pub async fn codes_since(&self, steam_id: u64, auth_code: &str, known_code: &str) -> CodeWalk {
        let mut codes = Vec::new();
        let mut current = known_code.to_string();
        let mut error = None;

        while codes.len() < MAX_CODES_PER_WALK {
            match self.next_share_code(steam_id, auth_code, &current).await {
                // Caught up.
                Ok(None) => break,
                Ok(Some(sc)) => {
                    codes.push(sc);
                    current = sc.encode();
                }
                Err(e) => {
                    error = Some(e);
                    break;
                }
            }
        }

        debug!(
            steam_id,
            count = codes.len(),
            truncated = codes.len() >= MAX_CODES_PER_WALK,
            failed = error.is_some(),
            "Finished walking share codes"
        );

        CodeWalk { codes, error }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// WebApiProvider — MatchProvider implementation
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// [`MatchProvider`] backed by the Steam Web API.
///
/// Wraps [`Cs2WebApiClient`] with a player registry so callers can
/// use the generic trait interface. Register each player's auth code
/// (typically loaded from a database at startup) before polling.
///
/// Shares the underlying client's rate limiter — all requests from all
/// players are governed by the same token bucket.
pub struct WebApiProvider {
    client: Cs2WebApiClient,
    /// steam_id → auth_code
    players: HashMap<u64, String>,
}

impl WebApiProvider {
    /// Create a new provider with the given Steam Web API key.
    ///
    /// Uses the default rate limit of 1 request per second.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            client: Cs2WebApiClient::new(api_key),
            players: HashMap::new(),
        }
    }

    /// Create a new provider with a custom rate limit quota.
    pub fn with_quota(api_key: impl Into<String>, quota: Quota) -> Self {
        Self {
            client: Cs2WebApiClient::with_quota(api_key, quota),
            players: HashMap::new(),
        }
    }

    /// Register a player's auth code. Call this for each player the service
    /// wants to poll. Typically loaded from a database at startup.
    pub fn register_player(&mut self, steam_id: u64, auth_code: impl Into<String>) {
        self.players.insert(steam_id, auth_code.into());
    }

    /// Remove a player's auth code.
    pub fn unregister_player(&mut self, steam_id: u64) {
        self.players.remove(&steam_id);
    }

    /// Borrow the underlying [`Cs2WebApiClient`].
    pub fn client(&self) -> &Cs2WebApiClient {
        &self.client
    }
}

impl MatchProvider for WebApiProvider {
    type Error = Error;

    async fn poll_codes(
        &mut self,
        steam_id: u64,
        known_code: &ShareCode,
    ) -> Result<Vec<ShareCode>, Self::Error> {
        let auth_code = self
            .players
            .get(&steam_id)
            .ok_or(Error::PlayerNotRegistered(steam_id))?
            .clone();

        let known_str = known_code.encode();
        let walk = self
            .client
            .codes_since(steam_id, &auth_code, &known_str)
            .await;

        // The trait is all-or-nothing, so a partial walk has to surface as an
        // error here — there is nowhere to put the codes. The poller does not
        // go through this path precisely because it needs to bank them; it
        // calls `codes_since` directly.
        match walk.error {
            Some(e) => Err(e),
            None => Ok(walk.codes),
        }
    }
}
