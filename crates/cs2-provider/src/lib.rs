pub use cs2_sharecode::ShareCode;

/// Discovers new CS2 match share codes for a player.
///
/// Implementors query a backend (Game Coordinator, Steam Web API, etc.)
/// and return share codes for matches newer than a known code.
///
/// Per-player configuration (auth codes, friend lists) is the caller's
/// responsibility — pass it at construction time or via backend-specific
/// setup methods, not through this trait.
pub trait MatchProvider: Send {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Return share codes for matches after `known_code`, oldest-first.
    ///
    /// - `steam_id`: the player's SteamID64
    /// - `known_code`: the most recent share code the caller has stored
    ///
    /// Returns an empty `Vec` if no new matches exist.
    fn poll_codes(
        &mut self,
        steam_id: u64,
        known_code: &ShareCode,
    ) -> impl std::future::Future<Output = Result<Vec<ShareCode>, Self::Error>> + Send;
}
