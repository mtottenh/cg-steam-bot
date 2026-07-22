//! Friendly Rust types wrapping CS2 GC protobuf data.
//!
//! These are our own types — clean Rust structs that don't leak protobuf
//! implementation details. Conversion from the raw protobuf types happens
//! in this module.
//!
//! # Protobuf Style Note
//!
//! `steam-vent-proto-csgo` may use either `prost` (struct fields are
//! `Option<T>`, direct access) or `protobuf` v2 (accessor methods like
//! `.field()`, `.set_field()`, `.has_field()`). The conversion code below
//! shows both styles — use whichever matches your version.

use chrono::{DateTime, Utc};
use serde::Serialize;

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Rank types
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// A player's ranking in a specific game mode.
#[derive(Debug, Clone, Serialize)]
pub struct Ranking {
    pub rank_type: RankType,
    /// For Comp/Wingman: 1–18 (Silver I → Global Elite).
    /// For Premier: CS Rating (e.g. 15000).
    pub rank_id: u32,
    /// Total wins in this mode.
    pub wins: u32,
}

/// CS2 game mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum RankType {
    Competitive,
    Wingman,
    Premier,
    Unknown(u32),
}

impl RankType {
    pub fn from_id(id: u32) -> Self {
        match id {
            6 => Self::Competitive,
            7 => Self::Wingman,
            11 => Self::Premier,
            other => Self::Unknown(other),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Competitive => "Competitive",
            Self::Wingman => "Wingman",
            Self::Premier => "Premier",
            Self::Unknown(_) => "Unknown",
        }
    }
}

/// Competitive/Wingman rank tier name (1–18).
pub fn rank_name(rank_id: u32) -> &'static str {
    match rank_id {
        0 => "Unranked",
        1 => "Silver I",
        2 => "Silver II",
        3 => "Silver III",
        4 => "Silver IV",
        5 => "Silver Elite",
        6 => "Silver Elite Master",
        7 => "Gold Nova I",
        8 => "Gold Nova II",
        9 => "Gold Nova III",
        10 => "Gold Nova Master",
        11 => "Master Guardian I",
        12 => "Master Guardian II",
        13 => "Master Guardian Elite",
        14 => "Distinguished Master Guardian",
        15 => "Legendary Eagle",
        16 => "Legendary Eagle Master",
        17 => "Supreme Master First Class",
        18 => "The Global Elite",
        _ => "Unknown",
    }
}

impl Ranking {
    pub fn display(&self) -> String {
        match self.rank_type {
            RankType::Premier => format!("CS Rating {}", self.rank_id),
            _ => rank_name(self.rank_id).to_string(),
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Own profile (from GC2ClientHello)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Your own profile data from the GC welcome message.
#[derive(Debug, Clone, Serialize)]
pub struct OwnProfile {
    pub account_id: u32,
    pub player_level: i32,
    pub player_cur_xp: i32,
    pub rankings: Vec<Ranking>,
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Match data (from MatchList)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// A single match from the match history.
#[derive(Debug, Clone, Serialize)]
pub struct MatchInfo {
    pub match_id: u64,
    pub match_time: Option<DateTime<Utc>>,
    pub map: String,
    /// Typically [ct_score, t_score].
    pub team_scores: Vec<i32>,
    pub match_result: i32,
    pub match_duration_secs: i32,
    pub players: Vec<PlayerStats>,
    /// Components for constructing a share code.
    pub share_code_parts: Option<ShareCodeParts>,
    /// Demo recording info (from WatchableMatchInfo).
    pub demo: Option<DemoInfo>,
}

/// Demo recording metadata from `WatchableMatchInfo` or round stats.
#[derive(Debug, Clone, Serialize)]
pub struct DemoInfo {
    /// Server IP as a packed u32 (network byte order).
    pub server_ip: Option<std::net::Ipv4Addr>,
    pub tv_port: u32,
    pub match_id: u64,
    pub reservation_id: Option<u64>,
    /// Raw demo URL extracted from round stats (fallback when
    /// `WatchableMatchInfo` is absent, e.g. `match_info()` lookups).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_url: Option<String>,
}

impl DemoInfo {
    /// Valve demo download URL, if server IP and reservation ID are available.
    ///
    /// Format: `http://replay{ip}.valve.net/730/{match_id}_{reservation_id}.dem.bz2`
    pub fn download_url(&self) -> Option<String> {
        // If we have a raw URL from round stats, return it directly
        if let Some(ref url) = self.raw_url {
            return Some(url.clone());
        }
        let ip = self.server_ip?;
        let reservation_id = self.reservation_id?;
        Some(format!(
            "http://replay{ip}.valve.net/730/{}_{reservation_id}.dem.bz2",
            self.match_id,
        ))
    }
}

/// Components of a CS2 share code: `CSGO-xxxxx-xxxxx-xxxxx-xxxxx-xxxxx`.
#[derive(Debug, Clone, Serialize)]
pub struct ShareCodeParts {
    pub match_id: u64,
    pub outcome_id: u64,
    pub token: u32,
}

impl ShareCodeParts {
    /// Convert to a `ShareCode` for encoding.
    pub fn to_share_code(&self) -> cs2_sharecode::ShareCode {
        cs2_sharecode::ShareCode {
            match_id: self.match_id,
            outcome_id: self.outcome_id,
            token: self.token as u16,
        }
    }
}

impl std::fmt::Display for ShareCodeParts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_share_code().fmt(f)
    }
}

/// One player's scoreboard line for a match.
#[derive(Debug, Clone, Serialize)]
pub struct PlayerStats {
    pub account_id: u32,
    /// Team number: 1 or 2, derived from the player's position in the
    /// protobuf reservation (indices 0–4 → team 1, 5–9 → team 2).
    pub team: u8,
    pub kills: i32,
    pub assists: i32,
    pub deaths: i32,
    pub score: i32,
    pub headshots: i32,
    pub mvps: i32,
    pub entry_3k: i32,
    pub entry_4k: i32,
    pub entry_5k: i32,
}

impl MatchInfo {
    pub fn score_display(&self) -> String {
        if self.team_scores.len() >= 2 {
            format!("{} – {}", self.team_scores[0], self.team_scores[1])
        } else {
            "? – ?".to_string()
        }
    }

    pub fn time_display(&self) -> String {
        self.match_time
            .map(|dt| dt.format("%Y-%m-%d %H:%M UTC").to_string())
            .unwrap_or_else(|| "Unknown".to_string())
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Protobuf conversion
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//
// Below are conversion functions from the raw protobuf types in
// `steam_vent_proto_csgo::cstrike15_gcmessages`. The field access style
// depends on whether steam-vent-proto-csgo uses prost or protobuf v2.
//
// PROST STYLE:
//   field: Option<T>  →  msg.field.unwrap_or_default()
//   repeated: Vec<T>  →  msg.field (direct)
//
// PROTOBUF V2 STYLE:
//   field: T           →  msg.field()  (returns value or default)
//   msg.has_field()    →  bool
//   repeated: Vec<T>   →  msg.get_field() or &msg.field
//
// The code below uses PROTOBUF V2 style based on docs.rs evidence for
// steam-vent-proto 0.1.x. If your version uses prost, swap the accessors.

use steam_vent_proto_csgo::cstrike15_gcmessages as pb;

impl OwnProfile {
    /// Convert from `CMsgGCCStrike15_v2_MatchmakingGC2ClientHello`.
    pub fn from_proto(hello: &pb::CMsgGCCStrike15_v2_MatchmakingGC2ClientHello) -> Self {
        let mut rankings = Vec::new();

        // The `ranking` field is the legacy single ranking (MessageField).
        // `rankings` is the repeated field with all modes.
        if hello.ranking.is_some() {
            let r = &*hello.ranking;
            rankings.push(Ranking {
                rank_type: RankType::from_id(r.rank_type_id()),
                rank_id: r.rank_id(),
                wins: r.wins(),
            });
        }

        for r in &hello.rankings {
            let rt = RankType::from_id(r.rank_type_id());
            // Skip if we already have this rank type from the legacy field
            if !rankings.iter().any(|existing| existing.rank_type == rt) {
                rankings.push(Ranking {
                    rank_type: rt,
                    rank_id: r.rank_id(),
                    wins: r.wins(),
                });
            }
        }

        Self {
            account_id: hello.account_id(),
            player_level: hello.player_level(),
            player_cur_xp: hello.player_cur_xp(),
            rankings,
        }
    }
}

impl MatchInfo {
    /// Convert from `CDataGCCStrike15_v2_MatchInfo`.
    pub fn from_proto(m: &pb::CDataGCCStrike15_v2_MatchInfo) -> Self {
        let match_time = {
            let ts = m.matchtime(); // or: m.matchtime.unwrap_or(0)
            if ts > 0 {
                DateTime::from_timestamp(ts as i64, 0)
            } else {
                None
            }
        };

        // The last entry in `roundstatsall` is the final scoreboard.
        // Earlier entries are per-round snapshots. Fall back to
        // `roundstats_legacy` if `roundstatsall` is empty.
        let final_stats = m.roundstatsall.last().or(m.roundstats_legacy.as_ref());

        let tv_port = m
            .watchablematchinfo
            .as_ref()
            .map(|w| w.tv_port())
            .unwrap_or(0);

        let (team_scores, match_result, match_duration, players, share_parts) = match final_stats {
            Some(stats) => extract_round_stats(m.matchid(), stats, tv_port),
            None => (vec![], 0, 0, vec![], None),
        };

        // Map name: prefer WatchableMatchInfo.game_map, fall back to the
        // round stats map field (which may contain a map name or a demo URL).
        let watchable_map = m
            .watchablematchinfo
            .as_ref()
            .map(|w| w.game_map().to_string())
            .unwrap_or_default();

        let stats_map = final_stats.map(|s| s.map().to_string()).unwrap_or_default();

        let map = if !watchable_map.is_empty() {
            watchable_map
        } else if !stats_map.is_empty() && !stats_map.starts_with("http") {
            stats_map.clone()
        } else {
            String::new()
        };

        // Demo info: prefer WatchableMatchInfo, fall back to the round stats
        // map field if it contains a URL.
        //
        // For MatchListRequestFullGameInfo responses, the GC often populates
        // WatchableMatchInfo with server_ip/tv_port but NOT reservation_id.
        // The round stats `reservationid` field (proto field 1) has the value
        // we need to construct the download URL.
        let stats_reservation_id = final_stats.and_then(|s| {
            let rid = s.reservationid();
            if rid != 0 {
                Some(rid)
            } else {
                None
            }
        });

        // Log raw proto values for debugging demo URL construction
        if let Some(w) = m.watchablematchinfo.as_ref() {
            tracing::debug!(
                watchable_server_ip = w.server_ip(),
                watchable_tv_port = w.tv_port(),
                watchable_has_reservation_id = w.has_reservation_id(),
                watchable_reservation_id = w.reservation_id(),
                stats_reservation_id = ?stats_reservation_id,
                stats_map = %stats_map,
                "Raw proto demo fields"
            );
        }

        // The round stats `map` field often contains the demo download URL
        // directly (e.g. http://replay271.valve.net/730/00...dem.bz2).
        // This is the most reliable source — use it as raw_url whenever available.
        let raw_url = if stats_map.starts_with("http") {
            Some(stats_map.clone())
        } else {
            None
        };

        let demo = if raw_url.is_some() {
            // We have a direct URL from round stats — use it
            Some(DemoInfo {
                server_ip: None,
                tv_port: 0,
                match_id: m.matchid(),
                reservation_id: stats_reservation_id,
                raw_url,
            })
        } else if let Some(w) = m.watchablematchinfo.as_ref() {
            let raw_ip = w.server_ip();
            let server_ip = if raw_ip != 0 {
                Some(std::net::Ipv4Addr::from(raw_ip))
            } else {
                None
            };
            let reservation_id = if w.has_reservation_id() {
                Some(w.reservation_id())
            } else {
                stats_reservation_id
            };
            Some(DemoInfo {
                server_ip,
                tv_port,
                match_id: m.matchid(),
                reservation_id,
                raw_url: None,
            })
        } else {
            None
        };

        Self {
            match_id: m.matchid(),
            match_time,
            map,
            team_scores,
            match_result,
            match_duration_secs: match_duration,
            players,
            share_code_parts: share_parts,
            demo,
        }
    }
}

/// Extract scoreboard data from the final round's stats.
fn extract_round_stats(
    match_id: u64,
    stats: &pb::CMsgGCCStrike15_v2_MatchmakingServerRoundStats,
    tv_port: u32,
) -> (Vec<i32>, i32, i32, Vec<PlayerStats>, Option<ShareCodeParts>) {
    // Account IDs come from the reservation sub-message.
    // The indices in kills/assists/deaths/scores correspond to these IDs.
    let account_ids: &[u32] = stats
        .reservation
        .as_ref()
        .map(|r| r.account_ids.as_slice())
        .unwrap_or(&[]);

    let players: Vec<PlayerStats> = account_ids
        .iter()
        .enumerate()
        .filter(|(_, &id)| id != 0) // skip empty/bot slots
        .map(|(i, &acct_id)| PlayerStats {
            account_id: acct_id,
            team: if i < 5 { 1 } else { 2 },
            kills: stats.kills.get(i).copied().unwrap_or(0),
            assists: stats.assists.get(i).copied().unwrap_or(0),
            deaths: stats.deaths.get(i).copied().unwrap_or(0),
            score: stats.scores.get(i).copied().unwrap_or(0),
            headshots: stats.enemy_headshots.get(i).copied().unwrap_or(0),
            mvps: stats.mvps.get(i).copied().unwrap_or(0),
            entry_3k: stats.enemy_3ks.get(i).copied().unwrap_or(0),
            entry_4k: stats.enemy_4ks.get(i).copied().unwrap_or(0),
            entry_5k: stats.enemy_5ks.get(i).copied().unwrap_or(0),
        })
        .collect();

    // Share code parts: matchid + reservation's match_id (= outcomeid) + token.
    // The token is tv_port from WatchableMatchInfo.
    let share_parts = stats.reservation.as_ref().map(|r| ShareCodeParts {
        match_id,
        outcome_id: r.match_id(),
        token: tv_port,
    });

    (
        stats.team_scores.clone(),
        stats.match_result(),
        stats.match_duration(),
        players,
        share_parts,
    )
}
