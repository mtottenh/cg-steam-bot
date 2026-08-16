use base64::Engine;
use clap::Parser;
use hmac::{Hmac, Mac};
use sha1::Sha1;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const STEAM_ALPHABET: &[u8] = b"23456789BCDFGHJKMNPQRTVWXY";

#[derive(Parser)]
#[command(about = "Generate Steam Guard TOTP codes")]
struct Args {
    /// Steam shared secret (base64-encoded).
    #[arg(long, env = "STEAM_SHARED_SECRET", conflicts_with = "mafile")]
    secret: Option<String>,

    /// Read the shared secret from an maFile instead.
    ///
    /// Preferred on a server: passing --secret puts the secret in the
    /// process table and the shell history, where the whole point of the
    /// maFile's 0600 is to keep it out.
    #[arg(long)]
    mafile: Option<PathBuf>,

    /// Continuously display codes, refreshing every second
    #[arg(long)]
    watch: bool,
}

/// The base64 shared secret, from whichever source was given.
fn read_secret(args: &Args) -> Result<String, String> {
    match (&args.secret, &args.mafile) {
        (Some(secret), _) => Ok(secret.clone()),
        (None, Some(path)) => {
            let raw = std::fs::read_to_string(path)
                .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
            let json: serde_json::Value = serde_json::from_str(&raw)
                .map_err(|e| format!("{} is not valid JSON: {e}", path.display()))?;
            json["shared_secret"]
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| format!("{} has no shared_secret field", path.display()))
        }
        (None, None) => Err("need --secret (or STEAM_SHARED_SECRET) or --mafile".to_string()),
    }
}

fn generate_code(secret: &[u8], time: u64) -> String {
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

fn current_unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_secs()
}

fn main() {
    let args = Args::parse();

    let secret = read_secret(&args).unwrap_or_else(|e| {
        eprintln!("steam-totp: {e}");
        std::process::exit(2);
    });
    let secret = base64::engine::general_purpose::STANDARD
        .decode(secret.trim())
        .unwrap_or_else(|e| {
            eprintln!("steam-totp: invalid base64 in shared secret: {e}");
            std::process::exit(2);
        });
    // Steam issues 20-byte secrets; anything else means the wrong field or
    // the wrong encoding (the totp:// URI form is base32, not base64) and
    // would silently emit codes that never work.
    if secret.len() != 20 {
        eprintln!(
            "steam-totp: shared secret decoded to {} bytes, expected 20 — \
             wrong field, or base32 rather than base64?",
            secret.len()
        );
        std::process::exit(2);
    }

    if args.watch {
        let mut last_code = String::new();
        loop {
            let now = current_unix_time();
            let remaining = 30 - (now % 30);
            let code = generate_code(&secret, now);
            if code != last_code {
                println!("{code}  ({remaining}s remaining)");
                last_code = code;
            } else {
                // Overwrite the line with updated countdown
                print!("\r{last_code}  ({remaining}s remaining) ");
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    } else {
        let now = current_unix_time();
        let remaining = 30 - (now % 30);
        let code = generate_code(&secret, now);
        println!("{code}  ({remaining}s remaining)");
    }
}
