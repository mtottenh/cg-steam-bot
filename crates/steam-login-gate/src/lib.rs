//! Steam login with Steam Guard challenges answered remotely.
//!
//! Bridges steam-vent's confirmation flow to the portal-daemon
//! [`GuardGate`] code-entry page, and provides the one [`login`] helper
//! shared by the bots (cs2-enricher) and the authenticator linker
//! (steam-guard-link). Handler precedence: TOTP shared secret if
//! configured, else the guard page if enabled, else the console prompt.

use portal_daemon::GuardGate;
use std::sync::Arc;
use std::time::Duration;
use steam_vent::auth::{
    ConsoleAuthConfirmationHandler, FileGuardDataStore, SharedSecretAuthConfirmationHandler,
    UserProvidedAuthConfirmationHandler,
};
use steam_vent::{Connection, ConnectionError, ServerList};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tracing::warn;

/// Default park time for a challenged login. Steam's auth session itself
/// expires after a few minutes, so waiting longer only hides the retry.
pub const DEFAULT_GUARD_CODE_WAIT: Duration = Duration::from_secs(300);

/// Bridge the guard-code page into steam-vent's confirmation flow.
///
/// `SteamGuardToken` cannot be constructed outside steam-vent, so instead
/// of implementing `AuthConfirmationHandler` we hand steam-vent a
/// `UserProvidedAuthConfirmationHandler` wired to in-memory pipes: the
/// prompt it writes arms the [`GuardGate`], and the code submitted through
/// the page is fed back as the "console" input line. The returned future
/// must be spawned alongside the login.
pub fn remote_guard_handler(
    gate: Arc<GuardGate>,
    account: String,
    code_wait: Duration,
) -> (
    UserProvidedAuthConfirmationHandler<DuplexStream, DuplexStream>,
    impl std::future::Future<Output = ()>,
) {
    let (handler_input, mut code_writer) = tokio::io::duplex(64);
    let (handler_output, mut prompt_reader) = tokio::io::duplex(1024);
    let handler = UserProvidedAuthConfirmationHandler::new(handler_input, handler_output);

    let feeder = async move {
        let mut prompt = Vec::new();
        let mut buf = [0u8; 256];
        // Blocks until steam-vent asks for a code; EOF means the login
        // completed without one (stored machine token still valid).
        match prompt_reader.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => prompt.extend_from_slice(&buf[..n]),
        }
        // The prompt is written in one flush, but drain any straggling
        // bytes before showing it on the page.
        while let Ok(Ok(n)) =
            tokio::time::timeout(Duration::from_millis(100), prompt_reader.read(&mut buf)).await
        {
            if n == 0 {
                break;
            }
            prompt.extend_from_slice(&buf[..n]);
        }
        let prompt = String::from_utf8_lossy(&prompt);
        let prompt = prompt.trim();
        warn!(
            account = %account,
            prompt,
            wait_secs = code_wait.as_secs(),
            "Steam Guard code required — enter it on the guard page (GUARD_ADDR)"
        );
        let line = match gate.wait_for_code(&account, prompt, code_wait).await {
            Some(code) => format!("{code}\n"),
            // An empty line makes the handler abort this attempt; the
            // caller's retry loop re-arms the page on the next pass.
            None => {
                warn!(account = %account, "No Steam Guard code arrived in time, aborting this login attempt");
                "\n".to_string()
            }
        };
        let _ = code_writer.write_all(line.as_bytes()).await;
    };

    (handler, feeder)
}

/// Log in to Steam, answering any Steam Guard challenge with (in order of
/// preference) the TOTP `shared_secret`, the guard page `gate`, or a
/// console prompt. Machine tokens persist via the user cache
/// (`FileGuardDataStore`), so a challenge answered once is skipped on
/// later logins.
pub async fn login(
    server_list: &ServerList,
    username: &str,
    password: &str,
    shared_secret: Option<&str>,
    gate: Option<&Arc<GuardGate>>,
    code_wait: Duration,
) -> Result<Connection, ConnectionError> {
    let guard_data = FileGuardDataStore::user_cache();

    if let Some(secret) = shared_secret {
        Connection::login(
            server_list,
            username,
            password,
            guard_data,
            SharedSecretAuthConfirmationHandler::new(secret),
        )
        .await
    } else if let Some(gate) = gate {
        let (handler, feeder) =
            remote_guard_handler(Arc::clone(gate), username.to_string(), code_wait);
        tokio::spawn(feeder);
        Connection::login(server_list, username, password, guard_data, handler).await
    } else {
        Connection::login(
            server_list,
            username,
            password,
            guard_data,
            ConsoleAuthConfirmationHandler::default(),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use portal_daemon::GuardSubmitOutcome;
    use steam_vent::auth::{AuthConfirmationHandler, ConfirmationAction};
    use steam_vent::proto::steammessages_auth_steamclient::{
        CAuthentication_AllowedConfirmation, EAuthSessionGuardType,
    };

    /// Drives the duplex bridge end to end: steam-vent's handler writes its
    /// prompt, the feeder arms the gate, a code submitted through the gate
    /// comes back as a `GuardToken` confirmation action.
    #[tokio::test]
    async fn remote_guard_handler_produces_a_guard_token() {
        let gate = GuardGate::new("test_login_gate_bridge");
        let (handler, feeder) =
            remote_guard_handler(Arc::clone(&gate), "bot".to_string(), Duration::from_secs(5));
        tokio::spawn(feeder);

        let submitter = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                while gate.status().is_none() {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                let status = gate.status().unwrap();
                assert_eq!(status.account, "bot");
                assert!(status.prompt.contains("device code"), "{}", status.prompt);
                gate.submit("abc12")
            })
        };

        let mut method = CAuthentication_AllowedConfirmation::new();
        method.set_confirmation_type(EAuthSessionGuardType::k_EAuthSessionGuardType_DeviceCode);
        method.set_associated_message("enter the code from your authenticator app".to_string());

        let action = handler.handle_confirmation(&[method.into()]).await;
        assert!(
            matches!(action, Some(ConfirmationAction::GuardToken(_, _))),
            "expected a guard token, got {action:?}"
        );
        assert_eq!(submitter.await.unwrap(), GuardSubmitOutcome::Accepted);
    }
}
