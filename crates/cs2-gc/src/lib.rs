//! # cs2-gc
//!
//! High-level CS2 Game Coordinator client library.
//!
//! Follows the same pattern as [`steam-vent-chat`](https://codeberg.org/steam-vent/chat):
//! a thin, typed wrapper over a `steam_vent::Connection` that handles
//! protocol details internally.
//!
//! ## Usage
//!
//! ```no_run
//! use cs2_gc::Cs2GcClient;
//! use steam_vent::Connection;
//!
//! # async fn example(connection: Connection) -> Result<(), Box<dyn std::error::Error>> {
//! let mut cs2 = Cs2GcClient::connect(connection).await?;
//!
//! // CS2-specific handshake — also returns your own rank
//! let profile = cs2.hello().await?;
//! println!("My rank: {:?}", profile.rankings);
//!
//! // Recent matches for a friend (must be on your friends list)
//! let friend_account_id: u32 = 12345678;
//! let matches = cs2.recent_matches(friend_account_id).await?;
//! for m in &matches {
//!     println!("{} on {} — {}", m.time_display(), m.map, m.score_display());
//! }
//! # Ok(())
//! # }
//! ```

pub mod provider;
pub(crate) mod transport;
pub mod types;

use std::time::Duration;

use tracing::{debug, info, warn};

use steam_vent_proto_csgo::cstrike15_gcmessages as pb;

pub use crate::provider::GcProvider;
use crate::transport::{GcTransport, GcTransportError};
use crate::types::{MatchInfo, OwnProfile};

/// How long to wait for a GC response before giving up.
const DEFAULT_GC_TIMEOUT: Duration = Duration::from_secs(15);

/// How long to wait for each hello attempt before retrying.
const HELLO_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// How many times to send the CS2 hello before giving up.
const HELLO_MAX_ATTEMPTS: u32 = 10;

/// Errors from the CS2 GC client.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Transport(#[from] GcTransportError),
}

pub type Result<T> = std::result::Result<T, Error>;

/// High-level CS2 Game Coordinator client.
///
/// ## Lifecycle
///
/// 1. Create with `Cs2GcClient::connect(connection).await?`
/// 2. Optionally call `.hello()` to get your own profile / rank
/// 3. Use `.recent_matches()`, `.match_info()`, etc.
///
/// The GC is ready for queries after `connect()` completes (the generic
/// handshake). `hello()` is only needed to fetch the bot's own profile
/// data (rank, level, XP). Match queries work without it.
///
/// The bot account must have CS2 in its library (free) and ideally Prime
/// status for full access to match data.
pub struct Cs2GcClient {
    transport: GcTransport,
}

impl Cs2GcClient {
    /// Connect to the CS2 Game Coordinator.
    ///
    /// Performs the generic GC handshake (set playing CS2, CMsgClientHello →
    /// CMsgClientWelcome). Call [`hello()`](Self::hello) next for the
    /// CS2-specific handshake that returns your profile.
    pub async fn connect(connection: steam_vent::Connection) -> Result<Self> {
        Ok(Self {
            transport: GcTransport::connect(connection).await?,
        })
    }

    /// Fetch the bot account's own profile (rank, level, XP).
    ///
    /// Sends `CMsgGCCStrike15_v2_MatchmakingClient2GCHello` and waits for
    /// `CMsgGCCStrike15_v2_MatchmakingGC2ClientHello`. The CS2 GC often
    /// ignores the first few hellos, so this retries up to 10 times.
    ///
    /// This is **optional** — match queries work without it. The GC is
    /// notoriously flaky about responding to this message.
    pub async fn hello(&mut self) -> Result<OwnProfile> {
        for attempt in 1..=HELLO_MAX_ATTEMPTS {
            info!(attempt, "Sending CS2 hello...");

            let hello = pb::CMsgGCCStrike15_v2_MatchmakingClient2GCHello::new();
            self.transport.send(hello).await?;

            match tokio::time::timeout(
                HELLO_ATTEMPT_TIMEOUT,
                self.transport
                    .one::<pb::CMsgGCCStrike15_v2_MatchmakingGC2ClientHello>(),
            )
            .await
            {
                Ok(Ok(gc_hello)) => {
                    let profile = OwnProfile::from_proto(&gc_hello);

                    info!(
                        account_id = profile.account_id,
                        ranks = profile.rankings.len(),
                        attempt,
                        "Got CS2 profile"
                    );

                    return Ok(profile);
                }
                Ok(Err(e)) => {
                    return Err(e.into());
                }
                Err(_) => {
                    warn!(
                        attempt,
                        max = HELLO_MAX_ATTEMPTS,
                        "CS2 GC did not respond, retrying..."
                    );
                }
            }
        }

        Err(GcTransportError::from(steam_vent::NetworkError::Timeout).into())
    }

    /// Get recent matches for an account.
    ///
    /// Returns up to ~8 most recent competitive matches with scoreboard data.
    ///
    /// - Pass your own `account_id` for your matches
    /// - Pass a friend's `account_id` for theirs (they must be on your
    ///   Steam friends list — this is Valve's restriction)
    pub async fn recent_matches(&mut self, account_id: u32) -> Result<Vec<MatchInfo>> {
        debug!(account_id, "Requesting recent matches");

        let mut req = pb::CMsgGCCStrike15_v2_MatchListRequestRecentUserGames::new();
        req.set_accountid(account_id);

        self.transport.send(req).await?;

        let match_list = tokio::time::timeout(
            DEFAULT_GC_TIMEOUT,
            self.transport.one::<pb::CMsgGCCStrike15_v2_MatchList>(),
        )
        .await
        .map_err(|_| GcTransportError::from(steam_vent::NetworkError::Timeout))??;

        info!(count = match_list.matches.len(), "Received match list");

        Ok(match_list
            .matches
            .iter()
            .map(MatchInfo::from_proto)
            .collect())
    }

    /// Get full match info from share code components.
    ///
    /// A share code `CSGO-xxxxx-xxxxx-xxxxx-xxxxx-xxxxx` decodes to
    /// `(match_id, outcome_id, token)`. Pass those components here.
    pub async fn match_info(
        &mut self,
        match_id: u64,
        outcome_id: u64,
        token: u32,
    ) -> Result<Vec<MatchInfo>> {
        debug!(match_id, outcome_id, token, "Requesting match info");

        let mut req = pb::CMsgGCCStrike15_v2_MatchListRequestFullGameInfo::new();
        req.set_matchid(match_id);
        req.set_outcomeid(outcome_id);
        req.set_token(token);

        self.transport.send(req).await?;

        let match_list = tokio::time::timeout(
            DEFAULT_GC_TIMEOUT,
            self.transport.one::<pb::CMsgGCCStrike15_v2_MatchList>(),
        )
        .await
        .map_err(|_| GcTransportError::from(steam_vent::NetworkError::Timeout))??;

        Ok(match_list
            .matches
            .iter()
            .map(MatchInfo::from_proto)
            .collect())
    }
}
