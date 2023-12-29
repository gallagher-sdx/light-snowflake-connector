/// Re-export the `RS256KeyPair` type from `jwt_simple` to ease loading
pub use jwt_simple::algorithms::RS256KeyPair;
use jwt_simple::prelude::*;

use crate::errors::SnowflakeResult;

pub fn create_token(
    key_pair: &RS256KeyPair,
    mut account_identifier: &str,
    user: &str,
) -> SnowflakeResult<String> {
    let mut public_key_fingerprint = key_pair.public_key().sha256_thumbprint();
    // Undo the URL-safe base64 encoding
    public_key_fingerprint = public_key_fingerprint.replace('-', "+").replace('_', "/");
    let padding = public_key_fingerprint.len() % 3;
    for _ in 0..padding {
        public_key_fingerprint.push('=');
    }
    log::debug!("Public key fingerprint: {}", public_key_fingerprint);
    // If there is an account region included, remove it:
    // AAA00000.us-east-1 should become AAA00000
    if let Some(dot) = account_identifier.find('.') {
        account_identifier = &account_identifier[..dot];
    }
    let qualified_username = format!("{account_identifier}.{user}");
    let issuer = format!("{qualified_username}.SHA256:{public_key_fingerprint}");
    let claims = Claims::create(Duration::from_mins(59))
        .with_issuer(issuer)
        .with_subject(qualified_username);
    log::debug!("Claims: {:?}", claims);
    Ok(key_pair.sign(claims)?)
}

#[cfg(test)]
mod tests {
    use crate::errors::SnowflakeResult;

    use super::*;

    #[test]
    fn verify_jwt() -> SnowflakeResult<()> {
        let key = RS256KeyPair::generate(2048)?;
        let token = create_token(&key, "TEST_ACCOUNT", "TEST_USER")?;
        let verified = key
            .public_key()
            .verify_token::<JWTClaims<NoCustomClaims>>(&token, None);
        assert!(verified.is_ok());
        Ok(())
    }
}
