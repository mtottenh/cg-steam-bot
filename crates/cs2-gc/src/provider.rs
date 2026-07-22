//! [`MatchProvider`] implementation backed by the CS2 Game Coordinator.

use cs2_provider::{MatchProvider, ShareCode};

use crate::{Cs2GcClient, Error};

/// [`MatchProvider`] backed by the CS2 Game Coordinator.
///
/// Uses [`Cs2GcClient::recent_matches`] to discover new share codes. Note:
/// the GC only returns the ~8 most recent matches. If more than 8 new matches
/// occurred since the last poll, intermediate matches will be missed. Use
/// `WebApiProvider` (from `cs2-webapi`) for gap-free history.
pub struct GcProvider {
    client: Cs2GcClient,
}

impl GcProvider {
    pub fn new(client: Cs2GcClient) -> Self {
        Self { client }
    }

    /// Borrow the underlying [`Cs2GcClient`].
    pub fn client(&self) -> &Cs2GcClient {
        &self.client
    }

    /// Mutably borrow the underlying [`Cs2GcClient`].
    pub fn client_mut(&mut self) -> &mut Cs2GcClient {
        &mut self.client
    }

    /// Consume the provider and return the underlying [`Cs2GcClient`].
    pub fn into_client(self) -> Cs2GcClient {
        self.client
    }
}

impl MatchProvider for GcProvider {
    type Error = Error;

    async fn poll_codes(
        &mut self,
        steam_id: u64,
        known_code: &ShareCode,
    ) -> Result<Vec<ShareCode>, Self::Error> {
        let account_id = (steam_id & 0xFFFF_FFFF) as u32;
        let matches = self.client.recent_matches(account_id).await?;

        let mut codes: Vec<ShareCode> = matches
            .iter()
            .filter_map(|m| {
                let parts = m.share_code_parts.as_ref()?;
                // Only include matches newer than the known code
                if parts.match_id > known_code.match_id {
                    Some(parts.to_share_code())
                } else {
                    None
                }
            })
            .collect();

        // Sort oldest-first by match_id
        codes.sort_by_key(|sc| sc.match_id);
        Ok(codes)
    }
}
