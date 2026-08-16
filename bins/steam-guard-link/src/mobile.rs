//! Steam WebAPI transport carrying a *mobile* auth session.
//!
//! Steam refuses `TwoFactor.AddAuthenticator` on a SteamClient-platform
//! session — it wants the request to come from the mobile app — and
//! steam-vent hard-codes that platform type with no override (the
//! `// todo: platform types` in its `auth/mod.rs`). So the linking flow
//! runs its own login here: the same protobuf messages steam-vent would
//! send, carried over the WebAPI, with device details that identify us as
//! the Android app.
//!
//! The WebAPI transport is also strictly better for diagnosis. Over the CM
//! connection a refusal arrives as a job-header EResult and steam-vent
//! drops the response body before parsing it, leaving nothing but
//! `ApiError(Fail)`. Here the refusal is an `x-eresult` header on an
//! otherwise readable response, so the body's own `status` survives.

use base64::Engine;
// `rsa` carries its own (older) rand_core than the workspace `rand`, so the
// RNG for the padding has to come from rsa's re-export, not rand::.
use rsa::rand_core::OsRng;
use rsa::{BigUint, Pkcs1v15Encrypt, RsaPublicKey};
use std::time::Duration;
use steam_vent::proto::protobuf::{EnumOrUnknown, Message, MessageField};
use steam_vent::proto::steammessages_auth_steamclient::{
    CAuthentication_BeginAuthSessionViaCredentials_Request,
    CAuthentication_BeginAuthSessionViaCredentials_Response, CAuthentication_DeviceDetails,
    CAuthentication_GetPasswordRSAPublicKey_Request,
    CAuthentication_GetPasswordRSAPublicKey_Response,
    CAuthentication_PollAuthSessionStatus_Request, CAuthentication_PollAuthSessionStatus_Response,
    CAuthentication_UpdateAuthSessionWithSteamGuardCode_Request,
    CAuthentication_UpdateAuthSessionWithSteamGuardCode_Response, EAuthSessionGuardType,
    EAuthTokenPlatformType,
};
use steam_vent::proto::steammessages_twofactor_steamclient::CTwoFactor_Time_Request;
use tracing::{debug, info, warn};

use crate::Error;

const API: &str = "https://api.steampowered.com";
/// Steam polls slowly by design; this bounds a login that never confirms.
const MAX_POLLS: u32 = 60;

/// What the mobile app tells Steam about itself. Steam does not verify any
/// of this, but the platform type is exactly what gates AddAuthenticator.
const DEVICE_NAME: &str = "Galaxy S22";
/// Steam's OS-type code for Android.
const OS_TYPE_ANDROID: i32 = -500;

/// An authenticated mobile session: the access token plus the client used
/// to obtain it.
pub struct MobileSession {
    client: reqwest::Client,
    access_token: String,
    pub steam_id: u64,
}

/// A refusal from a WebAPI service method.
#[derive(Debug)]
pub struct ApiRefusal {
    pub eresult: i32,
    pub message: Option<String>,
}

impl std::fmt::Display for ApiRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.message {
            Some(m) => write!(f, "EResult {} ({m})", self.eresult),
            None => write!(f, "EResult {}", self.eresult),
        }
    }
}

impl std::error::Error for ApiRefusal {}

/// One unified-service call. Requests and responses are protobuf; Steam
/// carries the request base64'd in `input_protobuf_encoded` and reports
/// failure in the `x-eresult` header rather than the HTTP status.
async fn call<Req: Message, Resp: Message>(
    client: &reqwest::Client,
    path: &str,
    req: &Req,
    access_token: Option<&str>,
    get: bool,
) -> Result<Resp, Error> {
    let encoded = base64::engine::general_purpose::STANDARD.encode(req.write_to_bytes()?);
    let url = format!("{API}/{path}");
    let mut params = vec![("input_protobuf_encoded", encoded)];
    if let Some(token) = access_token {
        params.push(("access_token", token.to_string()));
    }

    let request = if get {
        client.get(&url).query(&params)
    } else {
        client.post(&url).form(&params)
    };
    let resp = request.timeout(Duration::from_secs(30)).send().await?;

    // `x-eresult` is authoritative: Steam answers a refused call with 200
    // and an empty body just as often as with an HTTP error status.
    let eresult = resp
        .headers()
        .get("x-eresult")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<i32>().ok());
    let message = resp
        .headers()
        .get("x-error_message")
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let http_status = resp.status();
    let body = resp.bytes().await?;
    debug!(path, ?eresult, %http_status, body_len = body.len(), "WebAPI call");

    // EResult 1 is OK; a missing header on a 2xx also means success.
    match eresult {
        Some(1) | None if http_status.is_success() => {}
        Some(code) => return Err(Box::new(ApiRefusal { eresult: code, message })),
        None => return Err(format!("WebAPI {path} failed with HTTP {http_status}").into()),
    }
    Ok(Resp::parse_from_bytes(&body)?)
}

/// Steam's login RSA key, as a usable public key plus the timestamp that
/// must accompany the encrypted password.
async fn password_key(client: &reqwest::Client, account: &str) -> Result<(RsaPublicKey, u64), Error> {
    let mut req = CAuthentication_GetPasswordRSAPublicKey_Request::new();
    req.set_account_name(account.to_string());
    let resp: CAuthentication_GetPasswordRSAPublicKey_Response = call(
        client,
        "IAuthenticationService/GetPasswordRSAPublicKey/v1",
        &req,
        None,
        true,
    )
    .await?;

    let modulus = BigUint::parse_bytes(resp.publickey_mod().as_bytes(), 16)
        .ok_or("Steam returned an unparsable RSA modulus")?;
    let exponent = BigUint::parse_bytes(resp.publickey_exp().as_bytes(), 16)
        .ok_or("Steam returned an unparsable RSA exponent")?;
    Ok((
        RsaPublicKey::new(modulus, exponent)?,
        resp.timestamp(),
    ))
}

impl MobileSession {
    /// Log in as the mobile app, answering a Steam Guard challenge through
    /// `prompt` (which receives the human-readable description of what
    /// Steam is asking for).
    pub async fn login<F, Fut>(account: &str, password: &str, prompt: F) -> Result<Self, Error>
    where
        F: Fn(String) -> Fut,
        Fut: std::future::Future<Output = Result<String, Error>>,
    {
        let client = reqwest::Client::builder()
            .user_agent("Steam App / Android")
            .build()?;

        let (key, timestamp) = password_key(&client, account).await?;
        let encrypted = key.encrypt(&mut OsRng, Pkcs1v15Encrypt, password.as_bytes())?;

        let mut req = CAuthentication_BeginAuthSessionViaCredentials_Request::new();
        req.set_account_name(account.to_string());
        req.set_encrypted_password(base64::engine::general_purpose::STANDARD.encode(encrypted));
        req.set_encryption_timestamp(timestamp);
        req.set_website_id("Mobile".to_string());
        req.device_details = MessageField::some(CAuthentication_DeviceDetails {
            device_friendly_name: Some(DEVICE_NAME.to_string()),
            platform_type: Some(EnumOrUnknown::new(
                EAuthTokenPlatformType::k_EAuthTokenPlatformType_MobileApp,
            )),
            os_type: Some(OS_TYPE_ANDROID),
            ..CAuthentication_DeviceDetails::default()
        });

        let begun: CAuthentication_BeginAuthSessionViaCredentials_Response = call(
            &client,
            "IAuthenticationService/BeginAuthSessionViaCredentials/v1",
            &req,
            None,
            false,
        )
        .await?;
        if !begun.extended_error_message().is_empty() {
            warn!(message = %begun.extended_error_message(), "Steam returned an error message");
        }
        let steam_id = begun.steamid();
        info!(steam_id, "mobile auth session started");

        Self::confirm(&client, &begun, prompt).await?;

        let mut poll = CAuthentication_PollAuthSessionStatus_Request::new();
        poll.set_client_id(begun.client_id());
        poll.set_request_id(begun.request_id().to_vec());
        // Steam suggests its own poll interval; honour it, with a floor so
        // a bogus value cannot spin.
        let interval = Duration::from_secs_f32(begun.interval().max(1.0));

        for _ in 0..MAX_POLLS {
            let status: CAuthentication_PollAuthSessionStatus_Response = call(
                &client,
                "IAuthenticationService/PollAuthSessionStatus/v1",
                &poll,
                None,
                false,
            )
            .await?;
            if !status.access_token().is_empty() {
                return Ok(Self {
                    client,
                    access_token: status.access_token().to_string(),
                    steam_id,
                });
            }
            tokio::time::sleep(interval).await;
        }
        Err("Steam never confirmed the mobile login — giving up".into())
    }

    /// Answer whichever Steam Guard challenge the session demands. A
    /// confirmation done out-of-band (an approval tapped in the app, or a
    /// still-valid session) needs no code, so those are no-ops here and
    /// the poll loop simply succeeds.
    async fn confirm<F, Fut>(
        client: &reqwest::Client,
        begun: &CAuthentication_BeginAuthSessionViaCredentials_Response,
        prompt: F,
    ) -> Result<(), Error>
    where
        F: Fn(String) -> Fut,
        Fut: std::future::Future<Output = Result<String, Error>>,
    {
        for confirmation in &begun.allowed_confirmations {
            let guard_type = confirmation.confirmation_type();
            let description = match guard_type {
                EAuthSessionGuardType::k_EAuthSessionGuardType_EmailCode => {
                    "Steam Guard email code: enter the code Steam just emailed to the account"
                }
                EAuthSessionGuardType::k_EAuthSessionGuardType_DeviceCode => {
                    "Steam Guard app code: enter the code from the authenticator"
                }
                // None/MachineToken need no input; confirmation types the
                // operator resolves in the app resolve through the poll.
                _ => {
                    debug!(?guard_type, "no code needed for this confirmation type");
                    continue;
                }
            };

            let code = prompt(description.to_string()).await?;
            let mut req = CAuthentication_UpdateAuthSessionWithSteamGuardCode_Request::new();
            req.set_client_id(begun.client_id());
            req.set_steamid(begun.steamid());
            req.set_code(code);
            req.set_code_type(guard_type);
            let _: CAuthentication_UpdateAuthSessionWithSteamGuardCode_Response = call(
                client,
                "IAuthenticationService/UpdateAuthSessionWithSteamGuardCode/v1",
                &req,
                None,
                false,
            )
            .await?;
            return Ok(());
        }
        Ok(())
    }

    pub fn access_token(&self) -> &str {
        &self.access_token
    }

    /// Call a two-factor service method on this mobile session.
    pub async fn two_factor<Req: Message, Resp: Message>(
        &self,
        method: &str,
        req: &Req,
    ) -> Result<Resp, Error> {
        call(
            &self.client,
            &format!("ITwoFactorService/{method}/v1"),
            req,
            Some(&self.access_token),
            false,
        )
        .await
    }

    /// Steam's clock. TOTP codes are time-based, so the offset from our own
    /// clock matters more than the absolute value.
    pub async fn server_time(&self) -> Result<u64, Error> {
        let resp: steam_vent::proto::steammessages_twofactor_steamclient::CTwoFactor_Time_Response =
            self.two_factor("QueryTime", &CTwoFactor_Time_Request::new()).await?;
        Ok(resp.server_time())
    }
}
