//! One-shot Steam mobile authenticator linker.
//!
//! Links a mobile authenticator to the bot account
//! (`TwoFactor.AddAuthenticator` / `FinalizeAddAuthenticator`), driving
//! every interactive step — the login's email code and the SMS activation
//! code — through the portal-daemon Steam Guard page. The whole bootstrap
//! works from a browser on the tailnet: no SSH session, no desktop tools,
//! and the shared secret is born on the box.
//!
//! Unlike the bots, this tool does NOT log in through `steam-login-gate`'s
//! CM connection: Steam only accepts `AddAuthenticator` from a mobile
//! session, and steam-vent hard-codes the SteamClient platform type with no
//! override. The login and the two-factor calls therefore go over the
//! WebAPI with mobile device details — see [`mobile`].
//!
//! ## Usage
//!
//! ```bash
//! # On the portal box (stops cs2-enricher, frees the guard page port,
//! # restarts the enricher when done — see steam-guard-link.service):
//! systemctl start steam-guard-link
//! # then open https://<box>.ts.net:8443/ and follow the prompts.
//!
//! # Interactively (console prompts when GUARD_ADDR is unset):
//! STEAM_USERNAME=bot STEAM_PASSWORD=xxx steam-guard-link
//! ```
//!
//! Prerequisites: the account must have a phone number (Steam sends the
//! activation code by SMS) — add one at store.steampowered.com/phone/manage.
//! Secrets are written to `<MAFILE_DIR>/<account>.maFile` (mode 0600)
//! BEFORE finalization, so a crash mid-activation cannot lose them; the
//! revocation code is also printed to the journal and shown on the page.

use base64::Engine;
use clap::Parser;
use hmac::{Hmac, Mac};
use portal_daemon::GuardGate;
use sha1::Sha1;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use steam_vent::proto::protobuf::UnknownValueRef;
use steam_vent::proto::steammessages_twofactor_steamclient::{
    CTwoFactor_AddAuthenticator_Request, CTwoFactor_AddAuthenticator_Response,
    CTwoFactor_FinalizeAddAuthenticator_Request, CTwoFactor_FinalizeAddAuthenticator_Response,
    CTwoFactor_Status_Request, CTwoFactor_Status_Response,
};
use tracing::{error, info, warn};

mod mobile;
use mobile::{ApiRefusal, MobileSession};

type Error = Box<dyn std::error::Error>;

/// Per-prompt park time on the guard page; the prompt re-arms on expiry.
const CODE_WAIT: Duration = Duration::from_secs(300);
/// Give up after this many re-arms of one prompt (~2 h) so an abandoned
/// run cannot hold a Steam session open forever.
const MAX_PROMPT_ARMS: u32 = 24;
/// Steam accepts a code from consecutive 30 s windows during finalize;
/// how many windows to try before declaring the clocks hopeless.
const MAX_FINALIZE_WINDOWS: u32 = 30;
/// How many rejected SMS codes before giving up.
const MAX_SMS_ATTEMPTS: u32 = 3;

/// Link a Steam mobile authenticator to the bot account.
#[derive(Parser)]
#[command(name = "steam-guard-link")]
struct Args {
    /// Steam account username.
    #[arg(long, env = "STEAM_USERNAME")]
    username: String,

    /// Steam account password (prompted if not set).
    #[arg(long, env = "STEAM_PASSWORD", hide_env_values = true)]
    password: Option<String>,

    /// Directory the <account>.maFile is written to.
    #[arg(long, env = "MAFILE_DIR", default_value = ".")]
    mafile_dir: PathBuf,

    /// Keep the guard page (showing the revocation code and next steps)
    /// up this long after success before exiting.
    #[arg(long, env = "LINGER_SECS", default_value = "900")]
    linger_secs: u64,
}

#[tokio::main]
async fn main() {
    // Deliberately NOT RUST_LOG: the shared /etc/portal/cs2-enricher.env
    // pins RUST_LOG=cs2_enricher=info, which would silence this tool.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("GUARD_LINK_LOG").unwrap_or_else(|_| {
                "steam_guard_link=info,steam_login_gate=info,portal_daemon=info".into()
            }),
        )
        .init();

    let args = Args::parse();

    let gate = GuardGate::new("steam_guard_link");
    let page_enabled = portal_daemon::start_guard_from_env(Arc::clone(&gate));

    match run(&args, page_enabled, &gate).await {
        Ok(()) => {
            info!("authenticator linked successfully");
            linger(page_enabled, args.linger_secs).await;
        }
        Err(e) => {
            error!("linking failed: {e}");
            gate.set_notice(format!(
                "Linking FAILED: {e}\n\nSee journalctl -u steam-guard-link for details."
            ));
            linger(page_enabled, args.linger_secs.min(300)).await;
            std::process::exit(1);
        }
    }
}

/// Keep the page process alive so the operator can read the final notice.
async fn linger(page_enabled: bool, secs: u64) {
    if !page_enabled || secs == 0 {
        return;
    }
    info!(
        secs,
        "keeping the guard page up for the final notice — Ctrl-C / systemctl stop to exit sooner"
    );
    tokio::select! {
        () = portal_daemon::shutdown_signal() => {}
        () = tokio::time::sleep(Duration::from_secs(secs)) => {}
    }
}

async fn run(args: &Args, page_enabled: bool, gate: &Arc<GuardGate>) -> Result<(), Error> {
    let prompt_gate = page_enabled.then_some(gate);
    let mafile_path = args.mafile_dir.join(format!("{}.maFile", args.username));
    if mafile_path.exists() {
        return Err(format!(
            "{} already exists — refusing to overwrite an existing authenticator's secrets. \
             Move it away first if you really mean to re-link.",
            mafile_path.display()
        )
        .into());
    }
    std::fs::create_dir_all(&args.mafile_dir)?;

    let password = match args.password {
        Some(ref p) => p.clone(),
        None => rpassword::prompt_password("Steam password: ")?,
    };

    gate.set_notice(format!(
        "Linking a mobile authenticator to {} — logging in to Steam. \
         A login code prompt may appear below.",
        args.username
    ));

    // Deliberately NOT the shared steam_login_gate/CM path the bots use:
    // Steam only accepts AddAuthenticator from a mobile session, and
    // steam-vent hard-codes the SteamClient platform type. See mobile.rs.
    info!(username = %args.username, "Logging in to Steam as the mobile app...");
    let session = MobileSession::login(&args.username, &password, |prompt| {
        let account = args.username.clone();
        async move { prompt_code(prompt_gate, &account, &prompt).await }
    })
    .await?;
    let steam_id = session.steam_id;
    info!(steam_id, "logged in");

    // TOTP codes are time-based — use Steam's clock, not ours.
    let clock_offset = session.server_time().await? as i64 - unix_now() as i64;
    info!(clock_offset, "synced clock with TwoFactor.QueryTime");

    preflight(&session, steam_id).await?;

    gate.set_notice(format!(
        "Logged in as {}. Requesting an authenticator from Steam…",
        args.username
    ));

    let device_id = format!("android:{}", uuid_v4());
    let mut add = CTwoFactor_AddAuthenticator_Request::new();
    add.set_steamid(steam_id);
    add.set_authenticator_time(steam_now(clock_offset));
    add.set_authenticator_type(1);
    add.set_device_identifier(device_id.clone());
    add.set_version(2);
    let added: CTwoFactor_AddAuthenticator_Response = session
        .two_factor("AddAuthenticator", &add)
        .await
        .map_err(|e| -> Error { explain_refusal_error(e, explain_add_refusal) })?;

    if added.shared_secret().is_empty() {
        return Err(explain_add_refusal(added.status()).into());
    }

    let shared_secret = added.shared_secret().to_vec();
    let revocation_code = added.revocation_code().to_string();

    // Persist BEFORE finalize: if anything dies between Steam activating
    // the authenticator and us writing the secrets, the account would be
    // locked out of its own 2FA.
    write_mafile(
        &mafile_path,
        &args.username,
        steam_id,
        &device_id,
        &added,
        false,
    )?;
    info!(
        path = %mafile_path.display(),
        revocation_code = %revocation_code,
        "authenticator secrets saved (not yet activated)"
    );

    let sms_hint = if added.phone_number_hint().is_empty() {
        "the phone number on the account".to_string()
    } else {
        format!("the phone number ending in {}", added.phone_number_hint())
    };
    gate.set_notice(format!(
        "Steam issued the authenticator (revocation code {revocation_code} — write it down!). \
         It is NOT active yet: enter the SMS code below to finalize."
    ));

    finalize(
        &session,
        gate,
        prompt_gate,
        args,
        steam_id,
        &shared_secret,
        &sms_hint,
        clock_offset,
    )
    .await?;

    // Activation confirmed — mark the maFile fully enrolled.
    write_mafile(
        &mafile_path,
        &args.username,
        steam_id,
        &device_id,
        &added,
        true,
    )?;

    let secret_b64 = base64::engine::general_purpose::STANDARD.encode(&shared_secret);
    // Everything except the secret itself. The journal is shipped off-box
    // (alloy tails every unit into Loki), so the shared secret must never be
    // logged — it lives in the maFile, and on the tailnet-only guard page
    // for as long as this process lingers.
    let summary = format!(
        "Authenticator LINKED and ACTIVE for {account}.\n\
         \n\
         Revocation code: {revocation_code}\n\
         Write the revocation code down — it is the only way to remove the\n\
         authenticator if the maFile is lost.\n\
         \n\
         maFile: {path}\n\
         \n\
         cs2-enricher is deliberately still STOPPED: the account now requires\n\
         a TOTP code it cannot produce until the secret is deployed. Starting\n\
         it before then only throttles the account.\n\
         \n\
         Next:\n\
         \x20 1. Read the shared secret out of the maFile:\n\
         \x20    sudo jq -r .shared_secret {path}\n\
         \x20 2. Put it in the vault as vault_steam_bot_shared_secret\n\
         \x20    (just edit-vault) and redeploy.\n\
         \x20 3. systemctl start cs2-enricher — it logs in with TOTP from\n\
         \x20    now on.",
        account = args.username,
        path = mafile_path.display(),
    );
    info!("{summary}");
    // The page gets the one extra line the journal must not carry.
    gate.set_notice(format!(
        "{summary}\n\
         \n\
         Shared secret (base64) for vault_steam_bot_shared_secret:\n\
         {secret_b64}"
    ));
    Ok(())
}

/// The EResult behind a refusal, if the error is one.
fn refusal_code(e: &Error) -> Option<i32> {
    e.downcast_ref::<ApiRefusal>().map(|r| r.eresult)
}

/// Steam reports a refused two-factor call in one of two places: a
/// `status` field in the response body, or an `x-eresult` header on the
/// response. The numeric spaces are the same, so route both through one
/// explainer rather than letting a header-borne refusal surface as an
/// opaque transport error.
fn explain_refusal_error(e: Error, explain: fn(i32) -> String) -> Error {
    match refusal_code(&e) {
        Some(code) => explain(code).into(),
        None => e,
    }
}

fn explain_add_refusal(code: i32) -> String {
    match code {
        2 => "Steam refused with Fail (2). This login is already a mobile session (see \
              mobile.rs), so the usual platform-type cause is ruled out. What remains is the \
              account itself: confirm it has a *verified* phone number at \
              https://store.steampowered.com/phone/manage — Steam texts the activation code, \
              so an unverified or recently-changed number is refused here."
            .to_string(),
        29 => "Steam refused with DuplicateRequest (29): the account already has an \
               authenticator. Remove it first (revocation code via its maFile), or reuse \
               its existing shared secret instead of linking a new one."
            .to_string(),
        84 => "Steam refused with RateLimitExceeded (84): too many attempts — wait a \
               while (up to a day) and run this again."
            .to_string(),
        15 => "Steam refused with AccessDenied (15): the logged-in session is not allowed to \
               add an authenticator to this account."
            .to_string(),
        s => format!("Steam refused the AddAuthenticator request (code {s})."),
    }
}

/// Whether the account has a phone number attached.
///
/// This is the one prerequisite `TwoFactor.Status` cannot answer —
/// `authenticator_allowed` is account eligibility, not phone presence —
/// and a missing phone fails `AddAuthenticator` with the same bare `Fail`
/// as a platform-type refusal. `IPhoneService` has no proto in
/// steam-vent-proto-steam, so ask the WebAPI, which (unlike the CM path)
/// hands back a readable body. Purely diagnostic: it logs what it finds
/// and never fails the run, since the token we hold is a SteamClient one
/// and Steam may decline to answer it.
async fn phone_status(token: &str) {
    let url = "https://api.steampowered.com/IPhoneService/AccountPhoneStatus/v1/";
    let resp = reqwest::Client::new()
        .get(url)
        .query(&[("access_token", token), ("format", "json")])
        .timeout(Duration::from_secs(15))
        .send()
        .await;
    let body = match resp {
        Ok(r) if r.status().is_success() => r.text().await.unwrap_or_default(),
        Ok(r) => {
            warn!(status = %r.status(), "AccountPhoneStatus refused — phone presence unknown");
            return;
        }
        Err(e) => {
            warn!(error = %e, "AccountPhoneStatus unreachable — phone presence unknown");
            return;
        }
    };

    // Log the raw body too: the field set here is not covered by any proto
    // we ship, so if `has_phone` ever moves this is what identifies it.
    info!(body = %body.trim(), "IPhoneService.AccountPhoneStatus");
    match serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v["response"]["has_phone"].as_bool())
    {
        Some(true) => info!("account has a phone number — SMS activation can proceed"),
        Some(false) => warn!(
            "ACCOUNT HAS NO PHONE NUMBER — this alone makes Steam refuse AddAuthenticator, \
             and the SMS activation code later in this flow has nowhere to go. Add one at \
             https://store.steampowered.com/phone/manage, then re-run."
        ),
        None => warn!("could not read has_phone from the response — phone presence unknown"),
    }
}

/// Ask Steam what it thinks of the account's two-factor state before
/// requesting an authenticator. `AddAuthenticator` answers a refusal with
/// a bare EResult, which is far too coarse to act on; `TwoFactor.Status`
/// costs one round trip and names the actual blocker.
async fn preflight(session: &MobileSession, steam_id: u64) -> Result<(), Error> {
    phone_status(session.access_token()).await;

    let mut req = CTwoFactor_Status_Request::new();
    req.set_steamid(steam_id);
    let st: CTwoFactor_Status_Response = match session.two_factor("QueryStatus", &req).await {
        Ok(st) => st,
        // Never block the attempt on the diagnostic itself.
        Err(e) => {
            warn!(error = %e, "TwoFactor.Status failed — continuing without a preflight");
            return Ok(());
        }
    };

    info!(
        state = st.state(),
        authenticator_type = st.authenticator_type(),
        authenticator_allowed = st.authenticator_allowed(),
        email_validated = st.email_validated(),
        steamguard_scheme = st.steamguard_scheme(),
        token_gid = %st.token_gid(),
        "TwoFactor.Status"
    );

    if st.has_authenticator_allowed() && !st.authenticator_allowed() {
        return Err(
            "Steam reports authenticator_allowed=false for this account — it will \
                    refuse AddAuthenticator. This is normally a missing/unconfirmed phone \
                    number: add one at https://store.steampowered.com/phone/manage, then \
                    re-run."
                .into(),
        );
    }
    if st.authenticator_type() != 0 {
        return Err(format!(
            "Steam reports an existing authenticator (authenticator_type={}, token_gid={}). \
             Remove it with its revocation code before linking a new one.",
            st.authenticator_type(),
            st.token_gid()
        )
        .into());
    }
    Ok(())
}

/// The finalize loop. Steam validates our TOTP generation by accepting
/// codes across consecutive 30 s windows: `want_more` (a field newer than
/// this proto dump — read from unknown fields) and status 88
/// (TwoFactorCodeMismatch) both mean "next window, try again"; status 89
/// means the SMS activation code itself was wrong.
#[allow(clippy::too_many_arguments)]
async fn finalize(
    session: &MobileSession,
    gate: &Arc<GuardGate>,
    prompt_gate: Option<&Arc<GuardGate>>,
    args: &Args,
    steam_id: u64,
    shared_secret: &[u8],
    sms_hint: &str,
    clock_offset: i64,
) -> Result<(), Error> {
    let mut sms_attempts = 0;
    loop {
        sms_attempts += 1;
        if sms_attempts > MAX_SMS_ATTEMPTS {
            return Err(format!(
                "{MAX_SMS_ATTEMPTS} SMS codes rejected (status 89) — giving up. The saved \
                 maFile is NOT active; re-running starts a fresh attempt."
            )
            .into());
        }

        let activation_code = prompt_code(
            prompt_gate,
            &args.username,
            &format!("SMS activation code: enter the code Steam texted to {sms_hint}"),
        )
        .await?;

        let mut time = steam_now(clock_offset);
        let mut sms_rejected = false;
        for window in 0..MAX_FINALIZE_WINDOWS {
            let mut fin = CTwoFactor_FinalizeAddAuthenticator_Request::new();
            fin.set_steamid(steam_id);
            fin.set_activation_code(activation_code.clone());
            fin.set_authenticator_code(totp_code(shared_secret, time));
            fin.set_authenticator_time(time);
            fin.set_validate_sms_code(true);
            // 88/89 can arrive as an `x-eresult` header rather than a body
            // status — normalise both shapes to a status.
            let resp: CTwoFactor_FinalizeAddAuthenticator_Response =
                match session.two_factor("FinalizeAddAuthenticator", &fin).await {
                    Ok(resp) => resp,
                    Err(e) => match refusal_code(&e) {
                        Some(code @ (88 | 89)) => {
                            let mut synth = CTwoFactor_FinalizeAddAuthenticator_Response::new();
                            synth.set_status(code);
                            synth
                        }
                        _ => {
                            return Err(explain_refusal_error(e, |s| {
                                format!("FinalizeAddAuthenticator failed (code {s}).")
                            }))
                        }
                    },
                };

            if resp.status() == 89 {
                warn!("Steam rejected the SMS activation code (status 89)");
                gate.set_notice(
                    "Steam rejected that SMS code — enter it again, or wait for a fresh one.",
                );
                sms_rejected = true;
                break; // re-prompt via the outer loop
            }
            if wants_more(&resp) || resp.status() == 88 {
                info!(
                    window,
                    status = resp.status(),
                    "Steam wants the next code window"
                );
                time += 30;
                continue;
            }
            if resp.success() {
                return Ok(());
            }
            return Err(format!(
                "FinalizeAddAuthenticator failed (status {}). The saved maFile is NOT active.",
                resp.status()
            )
            .into());
        }
        if !sms_rejected {
            return Err(format!(
                "finalize never converged after {MAX_FINALIZE_WINDOWS} code windows — check \
                 the server clock (chrony/ntp), then re-run."
            )
            .into());
        }
    }
}

/// `want_more` (field 2) is newer than this proto dump, so it parses into
/// unknown fields; a set varint there means "submit the next code".
fn wants_more(resp: &CTwoFactor_FinalizeAddAuthenticator_Response) -> bool {
    match resp.special_fields.unknown_fields().get(2) {
        Some(UnknownValueRef::Varint(v)) => v != 0,
        _ => false,
    }
}

/// Ask the operator for a code: via the guard page when enabled (re-arming
/// until [`MAX_PROMPT_ARMS`]), else on the console.
async fn prompt_code(
    gate: Option<&Arc<GuardGate>>,
    account: &str,
    prompt: &str,
) -> Result<String, Error> {
    match gate {
        Some(gate) => {
            for _ in 0..MAX_PROMPT_ARMS {
                info!(prompt, "waiting for a code on the guard page (GUARD_ADDR)");
                if let Some(code) = gate.wait_for_code(account, prompt, CODE_WAIT).await {
                    return Ok(code);
                }
            }
            Err("no code arrived on the guard page after ~2 h — giving up".into())
        }
        None => {
            let prompt = prompt.to_string();
            let code = tokio::task::spawn_blocking(move || -> Result<String, std::io::Error> {
                use std::io::{BufRead, Write};
                print!("{prompt}: ");
                std::io::stdout().flush()?;
                let mut line = String::new();
                std::io::stdin().lock().read_line(&mut line)?;
                Ok(line.trim().to_ascii_uppercase())
            })
            .await??;
            if code.is_empty() {
                return Err("empty code entered — aborting".into());
            }
            Ok(code)
        }
    }
}

// ── maFile ───────────────────────────────────────────────────────────

/// Write the SDA/steamguard-cli-compatible maFile. The first write must
/// land atomically with no-clobber (`create_new`); the post-activation
/// update rewrites the file we just created.
fn write_mafile(
    path: &Path,
    account: &str,
    steam_id: u64,
    device_id: &str,
    added: &CTwoFactor_AddAuthenticator_Response,
    fully_enrolled: bool,
) -> Result<(), Error> {
    use std::io::Write;
    let b64 = base64::engine::general_purpose::STANDARD;
    let mafile = serde_json::json!({
        "account_name": added.account_name(),
        "device_id": device_id,
        "identity_secret": b64.encode(added.identity_secret()),
        "revocation_code": added.revocation_code(),
        "secret_1": b64.encode(added.secret_1()),
        "serial_number": added.serial_number().to_string(),
        "server_time": added.server_time(),
        "shared_secret": b64.encode(added.shared_secret()),
        "status": added.status(),
        "token_gid": added.token_gid(),
        "uri": added.uri(),
        "account": account,
        "steamid": steam_id,
        "fully_enrolled": fully_enrolled,
        "Session": serde_json::Value::Null,
    });

    let mut options = std::fs::OpenOptions::new();
    options.write(true);
    if fully_enrolled {
        options.truncate(true);
    } else {
        options.create_new(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(serde_json::to_string_pretty(&mafile)?.as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

// ── Helpers ──────────────────────────────────────────────────────────

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_secs()
}

/// Unix time corrected to Steam's clock.
fn steam_now(clock_offset: i64) -> u64 {
    unix_now().saturating_add_signed(clock_offset)
}

/// Random v4 UUID for the device identifier (`android:<uuid>`), matching
/// what the real mobile app and steamguard-cli register.
fn uuid_v4() -> String {
    let mut b: [u8; 16] = rand::random();
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13],
        b[14], b[15]
    )
}

/// Steam Guard TOTP (same algorithm as the standalone steam-totp bin):
/// HMAC-SHA1 over the 30 s time step, 5 chars from Steam's alphabet.
fn totp_code(secret: &[u8], time: u64) -> String {
    const STEAM_ALPHABET: &[u8] = b"23456789BCDFGHJKMNPQRTVWXY";
    let time_step = (time / 30) as i64;
    let mut mac = Hmac::<Sha1>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(&time_step.to_be_bytes());
    let hash = mac.finalize().into_bytes();

    let offset = (hash[19] & 0x0f) as usize;
    let mut code = u32::from_be_bytes([
        hash[offset],
        hash[offset + 1],
        hash[offset + 2],
        hash[offset + 3],
    ]);
    code &= 0x7fff_ffff;

    let mut out = String::with_capacity(5);
    for _ in 0..5 {
        out.push(STEAM_ALPHABET[(code as usize) % STEAM_ALPHABET.len()] as char);
        code /= STEAM_ALPHABET.len() as u32;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn totp_shape_and_window_boundaries() {
        let secret = b"0123456789abcdef0123";
        assert_eq!(totp_code(secret, 0), totp_code(secret, 29));
        assert_ne!(totp_code(secret, 0), totp_code(secret, 30));
        // Known vector cross-checked against an independent Python
        // implementation of the Steam TOTP algorithm.
        assert_eq!(totp_code(secret, 1_700_000_000), "JTQJ4");
    }

    #[test]
    fn uuid_v4_shape() {
        let id = uuid_v4();
        assert_eq!(id.len(), 36);
        assert_eq!(id.as_bytes()[14], b'4');
        let variant = id.as_bytes()[19];
        assert!(matches!(variant, b'8' | b'9' | b'a' | b'b'), "{id}");
    }

    #[test]
    fn mafile_write_refuses_to_clobber_then_updates_in_place() {
        let dir = std::env::temp_dir().join(format!("mafile-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bot.maFile");
        let _ = std::fs::remove_file(&path);

        let mut resp = CTwoFactor_AddAuthenticator_Response::new();
        resp.set_shared_secret(vec![1, 2, 3]);
        resp.set_revocation_code("R12345".into());
        resp.set_serial_number(42);
        resp.set_account_name("bot".into());

        write_mafile(
            &path,
            "bot",
            76_561_198_000_000_000,
            "android:x",
            &resp,
            false,
        )
        .unwrap();
        // A second initial write must not clobber.
        assert!(write_mafile(
            &path,
            "bot",
            76_561_198_000_000_000,
            "android:x",
            &resp,
            false
        )
        .is_err());
        let first: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(first["fully_enrolled"], false);
        assert_eq!(first["revocation_code"], "R12345");
        assert_eq!(first["serial_number"], "42");

        // The post-activation update rewrites in place.
        write_mafile(
            &path,
            "bot",
            76_561_198_000_000_000,
            "android:x",
            &resp,
            true,
        )
        .unwrap();
        let second: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(second["fully_enrolled"], true);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "maFile must not be world-readable");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A refusal delivered as an `x-eresult` header must produce the same
    /// actionable text as the equivalent body status, and anything that is
    /// not a refusal must pass through untouched rather than being
    /// misreported as one.
    #[test]
    fn header_eresult_and_body_status_explain_alike() {
        for code in [2, 15, 29, 84] {
            let refusal: Error = Box::new(ApiRefusal {
                eresult: code,
                message: None,
            });
            assert_eq!(
                explain_refusal_error(refusal, explain_add_refusal).to_string(),
                explain_add_refusal(code),
            );
        }
        assert!(explain_add_refusal(2).contains("phone"));
        assert!(explain_add_refusal(29).contains("already has an authenticator"));

        let transport: Error = "connection reset".into();
        assert_eq!(
            explain_refusal_error(transport, explain_add_refusal).to_string(),
            "connection reset",
        );
        assert_eq!(refusal_code(&"connection reset".into()), None);
    }

    #[test]
    fn wants_more_reads_unknown_field_2() {
        let mut resp = CTwoFactor_FinalizeAddAuthenticator_Response::new();
        assert!(!wants_more(&resp));
        resp.special_fields.mut_unknown_fields().add_varint(2, 1);
        assert!(wants_more(&resp));
    }
}
