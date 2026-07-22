//! GC transport layer — handles CS2 Game Coordinator communication.
//!
//! Bypasses steam-vent's `GameCoordinator` because its `wait_welcome()`
//! only listens for GC message kind 4004 (CMsgClientWelcome). CS2's GC
//! may instead respond with kind 9187 (ClientLogonFatalError), which
//! steam-vent silently discards — causing an infinite hang.
//!
//! This transport handles both cases and provides clear error reporting.

use std::pin::pin;
use std::time::Duration;

use protobuf::Message as _;
use steam_vent::proto::enums_clientserver::EMsg;
use steam_vent::proto::steammessages_base::CMsgProtoBufHeader;
use steam_vent::proto::steammessages_clientserver::cmsg_client_games_played::GamePlayed;
use steam_vent::proto::steammessages_clientserver::CMsgClientGamesPlayed;
use steam_vent::proto::steammessages_clientserver_2::CMsgGCClient;
use steam_vent::proto::{MsgKind, MsgKindEnum, RpcMessage, RpcMessageWithKind, PROTO_MASK};
use steam_vent::{Connection, ConnectionTrait, NetworkError, RawNetMessage};
use steam_vent_proto_csgo::cstrike15_gcmessages::ECsgoGCMsg;
use steam_vent_proto_csgo::gcsdk_gcmessages::CMsgClientHello;
use steam_vent_proto_csgo::gcsystemmsgs::EGCBaseClientMsg;
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt};
use tracing::{debug, info, warn};

const APP_ID: u32 = 730;
const GC_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(60);
const HELLO_INTERVAL: Duration = Duration::from_secs(5);

/// CS2 client version sent in CMsgClientHello.
/// Without this, the GC responds with ClientLogonFatalError (error code 4).
/// Source: https://github.com/SteamDatabase/GameTracking-CS2/blob/master/game/csgo/steam.inf
/// Update this when CS2 updates break the connection.
const CS2_CLIENT_VERSION: u32 = 2000738;

// ── Wrapper types (these are pub(crate) inside steam-vent) ───────────

/// Wraps a `CMsgGCClient` for sending to the GC (EMsg 5452).
#[derive(Debug)]
struct ClientToGcMessage {
    data: CMsgGCClient,
}

impl RpcMessageWithKind for ClientToGcMessage {
    type KindEnum = EMsg;
    const KIND: Self::KindEnum = EMsg::k_EMsgClientToGC;
}

impl RpcMessage for ClientToGcMessage {
    fn parse(reader: &mut dyn std::io::Read) -> protobuf::Result<Self> {
        Ok(Self {
            data: CMsgGCClient::parse_from_reader(reader)?,
        })
    }
    fn write(&self, writer: &mut dyn std::io::Write) -> protobuf::Result<()> {
        self.data.write_to_writer(writer)
    }
    fn encode_size(&self) -> usize {
        self.data.compute_size() as usize
    }
}

/// Wraps a `CMsgGCClient` received from the GC (EMsg 5453).
#[derive(Debug)]
struct ClientFromGcMessage {
    data: CMsgGCClient,
}

impl RpcMessageWithKind for ClientFromGcMessage {
    type KindEnum = EMsg;
    const KIND: Self::KindEnum = EMsg::k_EMsgClientFromGC;
}

impl RpcMessage for ClientFromGcMessage {
    fn parse(reader: &mut dyn std::io::Read) -> protobuf::Result<Self> {
        Ok(Self {
            data: CMsgGCClient::parse_from_reader(reader)?,
        })
    }
    fn write(&self, writer: &mut dyn std::io::Write) -> protobuf::Result<()> {
        self.data.write_to_writer(writer)
    }
    fn encode_size(&self) -> usize {
        self.data.compute_size() as usize
    }
}

// ── Transport ────────────────────────────────────────────────────────

/// Holds a `Connection` and routes GC messages through a channel.
pub struct GcTransport {
    connection: Connection,
    gc_rx: mpsc::Receiver<RawNetMessage>,
}

/// Errors from the GC transport layer.
#[derive(Debug, thiserror::Error)]
pub enum GcTransportError {
    #[error(transparent)]
    Network(Box<NetworkError>),

    #[error(
        "GC handshake timed out after {0}s — the CS2 Game Coordinator did not respond.\n\
         Common causes:\n\
         - The bot account does not have CS2 in its Steam library (add it — it's free)\n\
         - The CS2 GC is temporarily down (try again later)\n\
         - Steam is performing maintenance (check steamstat.us)"
    )]
    HandshakeTimeout(u64),

    #[error("GC logon fatal error (code {error_code}): {message}")]
    LogonFatalError {
        error_code: u32,
        message: String,
        country: String,
    },

    #[error("GC message stream closed unexpectedly")]
    StreamClosed,
}

impl From<NetworkError> for GcTransportError {
    fn from(e: NetworkError) -> Self {
        Self::Network(Box::new(e))
    }
}

impl GcTransport {
    /// Connect to the CS2 Game Coordinator.
    ///
    /// 1. Subscribes to incoming GC messages (EMsg::k_EMsgClientFromGC)
    /// 2. Tells Steam we're playing CS2 (app 730)
    /// 3. Sends CMsgClientHello, waits for CMsgClientWelcome or fatal error
    pub async fn connect(connection: Connection) -> Result<Self, GcTransportError> {
        info!("Starting GC handshake...");

        // Subscribe BEFORE sending anything so we don't miss the response
        let gc_stream = connection.on::<ClientFromGcMessage>();

        // Spawn background task that unwraps CMsgGCClient envelopes
        let (gc_tx, gc_rx) = mpsc::channel::<RawNetMessage>(32);
        tokio::spawn(gc_reader_task(gc_stream, gc_tx));

        // Tell Steam we're playing CS2
        let mut games = CMsgClientGamesPlayed::new();
        let mut game = GamePlayed::new();
        game.set_game_id(APP_ID as u64);
        games.games_played.push(game);

        connection
            .send_with_kind(games, EMsg::k_EMsgClientGamesPlayedWithDataBlob)
            .await
            .map_err(GcTransportError::from)?;
        info!("Set playing CS2 (app {})", APP_ID);

        let mut transport = Self { connection, gc_rx };
        transport.gc_handshake().await?;

        Ok(transport)
    }

    /// Send a typed GC message.
    pub async fn send<M>(&self, msg: M) -> Result<(), GcTransportError>
    where
        M: RpcMessageWithKind + Send,
        M::KindEnum: MsgKindEnum,
    {
        let gc_kind_encoded = M::KIND.encode_kind(true);
        let payload = serialize_gc_payload(gc_kind_encoded, &msg);

        let mut wrapper = CMsgGCClient::new();
        wrapper.set_appid(APP_ID);
        wrapper.set_msgtype(gc_kind_encoded);
        wrapper.set_payload(payload);

        self.connection
            .send(ClientToGcMessage { data: wrapper })
            .await
            .map_err(GcTransportError::from)
    }

    /// Wait for a specific typed GC message.
    ///
    /// Reads from the GC message channel until a message with matching kind
    /// arrives. Non-matching messages are logged and discarded.
    /// Fatal errors (kind 9187) are detected and surfaced immediately.
    pub async fn one<M>(&mut self) -> Result<M, GcTransportError>
    where
        M: steam_vent::NetMessage + 'static,
    {
        loop {
            let raw = self
                .gc_rx
                .recv()
                .await
                .ok_or(GcTransportError::StreamClosed)?;

            if raw.kind == ECsgoGCMsg::k_EMsgGCCStrike15_v2_ClientLogonFatalError {
                return Err(parse_fatal_error(raw));
            }

            if raw.kind == M::KIND {
                return raw.into_message::<M>().map_err(GcTransportError::from);
            }

            debug!(kind = ?raw.kind, "Skipping unhandled GC message");
        }
    }

    /// Generic GC handshake: send CMsgClientHello until we get CMsgClientWelcome.
    async fn gc_handshake(&mut self) -> Result<(), GcTransportError> {
        let deadline = tokio::time::Instant::now() + GC_HANDSHAKE_TIMEOUT;
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            info!(
                attempt,
                "Sending GC hello (kind 4006, version {})...", CS2_CLIENT_VERSION
            );
            let mut hello = CMsgClientHello::new();
            hello.set_version(CS2_CLIENT_VERSION);
            self.send(hello).await?;

            // Wait up to HELLO_INTERVAL for a response
            let wait_until = std::cmp::min(deadline, tokio::time::Instant::now() + HELLO_INTERVAL);

            loop {
                match tokio::time::timeout_at(wait_until, self.gc_rx.recv()).await {
                    Ok(Some(raw)) => {
                        if raw.kind == ECsgoGCMsg::k_EMsgGCCStrike15_v2_ClientLogonFatalError {
                            return Err(parse_fatal_error(raw));
                        }
                        if raw.kind == EGCBaseClientMsg::k_EMsgGCClientWelcome {
                            info!("GC handshake complete (CMsgClientWelcome received)");
                            return Ok(());
                        }
                        debug!(kind = ?raw.kind, "Ignoring GC message during handshake");
                    }
                    Ok(None) => return Err(GcTransportError::StreamClosed),
                    Err(_) => break, // timeout → send another hello
                }
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(GcTransportError::HandshakeTimeout(
                    GC_HANDSHAKE_TIMEOUT.as_secs(),
                ));
            }
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

/// Background task: reads `ClientFromGcMessage` from the connection,
/// unwraps the `CMsgGCClient` envelope, parses inner payload into
/// `RawNetMessage`, and forwards through the channel.
async fn gc_reader_task(
    gc_stream: impl Stream<Item = Result<ClientFromGcMessage, NetworkError>>,
    tx: mpsc::Sender<RawNetMessage>,
) {
    let mut gc_stream = pin!(gc_stream);
    while let Some(result) = gc_stream.next().await {
        match result {
            Ok(msg) => {
                let raw_kind = msg.data.msgtype();
                let kind = MsgKind((raw_kind & !PROTO_MASK) as i32);
                let payload = msg.data.payload.unwrap_or_default();

                debug!(?kind, payload_len = payload.len(), "Received GC message");

                match RawNetMessage::read(payload) {
                    Ok(raw) => {
                        if tx.send(raw).await.is_err() {
                            break; // receiver dropped
                        }
                    }
                    Err(e) => warn!(?kind, "Failed to parse GC payload: {e}"),
                }
            }
            Err(e) => {
                warn!("GC message stream error: {e}");
                break;
            }
        }
    }
    debug!("GC reader task exiting");
}

/// Serialize a protobuf GC message into the inner payload format.
///
/// Format: `[kind|PROTO_MASK (4 LE)] [header_len (4 LE)] [proto_header] [body]`
fn serialize_gc_payload(kind_with_mask: u32, msg: &impl RpcMessage) -> Vec<u8> {
    let proto_header = CMsgProtoBufHeader::new();
    let header_bytes = proto_header.write_to_bytes().unwrap_or_default();

    let mut body_buf = Vec::new();
    msg.write(&mut body_buf).unwrap_or_default();

    let mut payload = Vec::with_capacity(8 + header_bytes.len() + body_buf.len());
    payload.extend_from_slice(&kind_with_mask.to_le_bytes());
    payload.extend_from_slice(&(header_bytes.len() as u32).to_le_bytes());
    payload.extend_from_slice(&header_bytes);
    payload.extend_from_slice(&body_buf);
    payload
}

/// Parse a `CMsgGCCStrike15_v2_ClientLogonFatalError` from a raw GC message.
fn parse_fatal_error(raw: RawNetMessage) -> GcTransportError {
    use steam_vent_proto_csgo::cstrike15_gcmessages::CMsgGCCStrike15_v2_ClientLogonFatalError;

    // raw.data contains the protobuf body (header already stripped by RawNetMessage::read)
    match CMsgGCCStrike15_v2_ClientLogonFatalError::parse_from_bytes(&raw.data) {
        Ok(err_msg) => {
            let error_code = err_msg.errorcode();
            let message = err_msg.message().to_string();
            let country = err_msg.country().to_string();

            warn!(
                error_code,
                message = %message,
                country = %country,
                "GC logon fatal error"
            );

            GcTransportError::LogonFatalError {
                error_code,
                message,
                country,
            }
        }
        Err(e) => {
            warn!("Failed to parse ClientLogonFatalError body: {e}");
            GcTransportError::LogonFatalError {
                error_code: 0,
                message: format!("(unparseable: {e})"),
                country: String::new(),
            }
        }
    }
}
