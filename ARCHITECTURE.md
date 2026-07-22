# CS2 Match Bot — Architecture

## Authentication

The Game Coordinator protocol requires a real Steam account logged in as a
client. This is intentional — the GC thinks it's talking to a running copy
of CS2. Services like Leetify, FACEIT, and CS Demo Manager all use dedicated
bot accounts.

**Use a dedicated bot account**, not your personal one:
- Steam doesn't allow two simultaneous client sessions on the same account
- If Valve rate-limits or restricts the account, it's not your main
- The bot needs CS2 in its library (free) and ideally Prime status

There is also a **Steam Web API** (`ICSGOPlayers_730/GetNextMatchSharingCode`)
that uses an API key + the player's Game Authentication Code to chain through
share codes. This is HTTP-only (no GC needed) but requires the player to
opt in by generating an auth code. Could be a lighter alternative for some
use cases, but doesn't give full scoreboard data.

## How steam-vent Actually Works

From studying the `steam-vent-chat` source:

```rust
// Request-response for Steam unified services:
let result = self.connection.service_method(req).await?;

// Subscribe to incoming notifications by protobuf type:
let stream = self.connection
    .on_notification::<CFriendMessages_IncomingMessage_Notification>();
```

These are **not** used for GC communication. GC messages are a different
protocol layer:

```
Steam Unified Services (chat, friends, inventory)
  → connection.service_method(CXxx_Method_Request) → CXxx_Method_Response
  → connection.on_notification::<CXxx_Notification>() → Stream

Game Coordinator (CS2, Dota 2, TF2)
  → Raw protobuf wrapped in CMsgGCClient
  → Sent via EMsg::ClientToGC (200)
  → Received via EMsg::ClientFromGC (201)
  → Own message type ID namespace (ECsgoGCMsg)
```

## Layered Architecture

Following the `steam-vent-chat` pattern of wrapping `Connection`:

```
┌───────────────────────────────────────────┐
│  cs2-matches (binary)                     │
│    Thin CLI that calls cs2-gc methods     │
│    and prints results                     │
├───────────────────────────────────────────┤
│  cs2-gc (library)                         │
│    Cs2GcClient                            │
│      .hello() → OwnProfile               │
│      .recent_matches(id) → Vec<MatchInfo> │
│      .match_info(id, out, tok) → Vec<..>  │
├───────────────────────────────────────────┤
│  transport.rs                             │
│    GcTransport                            │
│      wraps GameCoordinator (typed GC I/O) │
├───────────────────────────────────────────┤
│  steam-vent  (Connection + GameCoordinator)│
│    GameCoordinator: .send(), .one()       │
│    Uses csgo feature + GCHandshake from   │
│    steam-vent-proto-csgo                  │
└───────────────────────────────────────────┘
```

The transport layer wraps steam-vent's `GameCoordinator`, which handles GC
protocol details (set playing, CMsgClientHello/Welcome, message wrapping).
All GC I/O uses typed protobuf messages via `gc.send()` and `gc.one()`.

## GC Message Flow

### Handshake (your own data)

```
set_playing(730)
  ↓
ClientHello (9109, empty) ──────────────► CS2 Game Coordinator
                                                │
GC2ClientHello (9110) ◄────────────────────────┘
  ├── account_id, player_level, player_cur_xp
  ├── ranking: PlayerRankingInfo  (legacy single)
  ├── rankings[]: PlayerRankingInfo  (all modes)
  └── penalty_seconds, vac_banned, commendation, medals
```

### Recent Matches (your own or a friend's)

```
MatchListRequestRecentUserGames (9148)
  { accountid: 12345678 }
  ↓                                      ► CS2 GC
MatchList (9149) ◄──────────────────────┘
  ├── matches[]: MatchInfo
  │     ├── matchid, matchtime
  │     ├── watchablematchinfo { server_ip, tv_port, ... }
  │     └── roundstatsall[]: ServerRoundStats
  │           ├── reservation { account_ids[] }
  │           ├── map, team_scores[], match_result, match_duration
  │           ├── kills[], assists[], deaths[], scores[]  (per player)
  │           └── (indices correspond to reservation.account_ids)
```

**Friend requirement:** the target account_id must be on your Steam
friends list. This is why Leetify requires you to add their bot.

### Match by Share Code

```
MatchListRequestFullGameInfo (9146)
  { matchid, outcomeid, token }          ► CS2 GC
  ↓
MatchList (9149) ◄──────────────────────┘
  (same structure as above)
```

## Key Protobuf Messages

| Msg ID | Name | Direction | Trigger |
|--------|------|-----------|---------|
| 9109 | MatchmakingClient2GCHello | Client→GC | Handshake |
| 9110 | MatchmakingGC2ClientHello | GC→Client | Welcome + rank |
| 9146 | MatchListRequestFullGameInfo | Client→GC | Share code lookup |
| 9148 | MatchListRequestRecentUserGames | Client→GC | Match history |
| 9149 | MatchList | GC→Client | Response to 9146/9148 |
| 9150 | MatchListRequestCurrentLiveGames | Client→GC | Live games |
| 9151 | MatchListRequestLiveGameForUser | Client→GC | User's live game |

## Project Structure

```
steam_bot/
├── Cargo.toml                          workspace root (members, shared deps)
├── ARCHITECTURE.md                     this file
│
├── crates/
│   └── cs2-gc/                         library crate
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs                  Cs2GcClient (public API)
│           ├── transport.rs            GcTransport (wraps GameCoordinator)
│           └── types.rs               Friendly Rust types + proto conversion
│
└── bins/
    └── cs2-matches/                    binary crate
        ├── Cargo.toml
        └── src/
            └── main.rs                CLI tool
```

New crates go in `crates/` (libraries) or `bins/` (executables). The
workspace uses `members = ["crates/*", "bins/*"]` so new crates are
picked up automatically.

## Future Work

- **Share code encode/decode** → `crates/cs2-sharecode/`
- **Demo download URLs** from `WatchableMatchInfo`
- **Demo parsing** via `source2-demo` crate → `crates/cs2-demo/`
- **Polling service** for rank change detection → `bins/cs2-poller/`
- **Discord bot** frontend → `bins/cs2-bot/`
