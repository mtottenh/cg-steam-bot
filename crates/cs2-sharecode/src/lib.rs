//! CS2 share code encoding and decoding.
//!
//! Share codes look like `CSGO-xxxxx-xxxxx-xxxxx-xxxxx-xxxxx` and encode
//! three values: `(match_id, outcome_id, token)`.
//!
//! Algorithm based on [akiver/csgo-sharecode](https://github.com/akiver/csgo-sharecode).

use std::fmt;
use std::str::FromStr;

const DICTIONARY: &[u8] = b"ABCDEFGHJKLMNOPQRSTUVWXYZabcdefhijkmnopqrstuvwxyz23456789";
const DICTIONARY_LEN: u8 = 57;
const SHARE_CODE_LEN: usize = 25;
const PREFIX: &str = "CSGO-";

#[derive(Debug, thiserror::Error)]
pub enum ShareCodeError {
    #[error("share code must start with 'CSGO-'")]
    InvalidPrefix,
    #[error("share code must have exactly 25 characters after removing prefix and dashes")]
    InvalidLength,
    #[error("invalid character '{0}' in share code")]
    InvalidChar(char),
}

/// A decoded CS2 share code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ShareCode {
    pub match_id: u64,
    pub outcome_id: u64,
    pub token: u16,
}

impl ShareCode {
    /// Decode a share code string like `CSGO-xxxxx-xxxxx-xxxxx-xxxxx-xxxxx`.
    pub fn decode(code: &str) -> Result<Self, ShareCodeError> {
        let stripped = code
            .strip_prefix(PREFIX)
            .ok_or(ShareCodeError::InvalidPrefix)?;

        let chars: Vec<u8> = stripped.bytes().filter(|&b| b != b'-').collect();

        if chars.len() != SHARE_CODE_LEN {
            return Err(ShareCodeError::InvalidLength);
        }

        // Map characters to dictionary indices (reversed — share codes are stored reversed)
        let mut indices = [0u8; SHARE_CODE_LEN];
        for (i, &ch) in chars.iter().enumerate() {
            let pos = DICTIONARY
                .iter()
                .position(|&d| d == ch)
                .ok_or(ShareCodeError::InvalidChar(ch as char))?;
            indices[i] = pos as u8;
        }
        indices.reverse();

        // Convert from base-57 to 18 bytes (big-endian big integer)
        let mut bytes = [0u8; 18];
        for &idx in &indices {
            // Multiply bytes by 57 and add idx
            let mut carry = idx as u16;
            for b in bytes.iter_mut().rev() {
                let val = (*b as u16) * (DICTIONARY_LEN as u16) + carry;
                *b = (val & 0xFF) as u8;
                carry = val >> 8;
            }
        }

        // Unpack: [match_id(8 LE) | outcome_id(8 LE) | token(2 LE)]
        let match_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let outcome_id = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let token = u16::from_le_bytes(bytes[16..18].try_into().unwrap());

        Ok(Self {
            match_id,
            outcome_id,
            token,
        })
    }

    /// Encode to a share code string like `CSGO-xxxxx-xxxxx-xxxxx-xxxxx-xxxxx`.
    pub fn encode(&self) -> String {
        // Pack into 18 bytes LE: [match_id(8) | outcome_id(8) | token(2)]
        let mut bytes = [0u8; 18];
        bytes[0..8].copy_from_slice(&self.match_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.outcome_id.to_le_bytes());
        bytes[16..18].copy_from_slice(&self.token.to_le_bytes());

        // Convert to base-57 by repeated division
        let mut chars = [0u8; SHARE_CODE_LEN];
        for ch in &mut chars {
            // Divide the big integer by 57, remainder is the next digit
            let mut remainder = 0u16;
            for b in bytes.iter_mut() {
                let val = (remainder << 8) | (*b as u16);
                *b = (val / DICTIONARY_LEN as u16) as u8;
                remainder = val % DICTIONARY_LEN as u16;
            }
            *ch = DICTIONARY[remainder as usize];
        }

        // The first remainder is the LSD, which is the first char in the share code
        // format (decode reverses input so MSD-first processing maps last char → power 0).
        // So we do NOT reverse here.
        let encoded: String = chars.iter().map(|&b| b as char).collect();

        format!(
            "{}{}-{}-{}-{}-{}",
            PREFIX,
            &encoded[0..5],
            &encoded[5..10],
            &encoded[10..15],
            &encoded[15..20],
            &encoded[20..25],
        )
    }
}

impl fmt::Display for ShareCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.encode())
    }
}

impl FromStr for ShareCode {
    type Err = ShareCodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::decode(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_VECTORS: &[(&str, u64, u64, u16)] = &[
        (
            "CSGO-L9spZ-ihuov-cyhtE-kxbqa-FkBAA",
            3400360672356205056,
            3400367402569957763,
            9725,
        ),
        (
            "CSGO-GADqf-jjyJ8-cSP2r-smZRo-TO2xK",
            3230642215713767580,
            3230647599455273103,
            55788,
        ),
        (
            "CSGO-bPQEz-PrYTq-u5w8E-ZbUy7-ZeQ3A",
            3325408798641750542,
            3325410334092558852,
            240,
        ),
        (
            "CSGO-wBrm6-7fkM6-AzBC5-u6GmR-iHLHA",
            3302232779302895618,
            3302241568953467250,
            3085,
        ),
        (
            "CSGO-TKDTJ-YrAXs-sDNfL-HOuKO-i84VH",
            3402250361329680757,
            3402250801563828781,
            61630,
        ),
        (
            "CSGO-p4X9o-3Mfut-tpe5y-J8K6f-mj5ZJ",
            3402249502336221574,
            3402252092201501292,
            14119,
        ),
    ];

    #[test]
    fn decode_all_vectors() {
        for &(code, match_id, outcome_id, token) in TEST_VECTORS {
            let sc =
                ShareCode::decode(code).unwrap_or_else(|e| panic!("failed to decode {code}: {e}"));
            assert_eq!(sc.match_id, match_id, "match_id mismatch for {code}");
            assert_eq!(sc.outcome_id, outcome_id, "outcome_id mismatch for {code}");
            assert_eq!(sc.token, token, "token mismatch for {code}");
        }
    }

    #[test]
    fn encode_all_vectors() {
        for &(code, match_id, outcome_id, token) in TEST_VECTORS {
            let sc = ShareCode {
                match_id,
                outcome_id,
                token,
            };
            assert_eq!(sc.encode(), code, "encode mismatch for match_id={match_id}");
        }
    }

    #[test]
    fn round_trip() {
        for &(code, _, _, _) in TEST_VECTORS {
            let decoded = ShareCode::decode(code).unwrap();
            let re_encoded = decoded.encode();
            assert_eq!(re_encoded, code);
            let re_decoded = ShareCode::decode(&re_encoded).unwrap();
            assert_eq!(re_decoded, decoded);
        }
    }

    #[test]
    fn display_and_from_str() {
        let sc = ShareCode {
            match_id: 3400360672356205056,
            outcome_id: 3400367402569957763,
            token: 9725,
        };
        let display = sc.to_string();
        assert_eq!(display, "CSGO-L9spZ-ihuov-cyhtE-kxbqa-FkBAA");

        let parsed: ShareCode = display.parse().unwrap();
        assert_eq!(parsed, sc);
    }

    #[test]
    fn invalid_prefix() {
        assert!(matches!(
            ShareCode::decode("DOTA-xxxxx-xxxxx-xxxxx-xxxxx-xxxxx"),
            Err(ShareCodeError::InvalidPrefix)
        ));
    }

    #[test]
    fn invalid_length() {
        assert!(matches!(
            ShareCode::decode("CSGO-xxxxx"),
            Err(ShareCodeError::InvalidLength)
        ));
    }

    #[test]
    fn invalid_char() {
        // '0', '1', 'l', 'g', 'O', 'I' are not in the dictionary
        assert!(matches!(
            ShareCode::decode("CSGO-00000-00000-00000-00000-00000"),
            Err(ShareCodeError::InvalidChar('0'))
        ));
    }
}
