//! Authentication, in both directions.
//!
//! A proxy authenticates twice: it is a server to the client and a client to
//! the backend. Both halves live here.
//!
//! Luxbyte never needs, stores or forwards a plaintext password for the
//! client half. Verifying a SCRAM proof requires only the stored verifier —
//! the same `StoredKey`/`ServerKey` pair PostgreSQL itself keeps in
//! `pg_authid` — so a compromised proxy leaks no credential that a compromised
//! database would not have leaked already.

use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::error::ProtocolError;

/// The one SASL mechanism this crate implements.
pub const SCRAM_SHA_256: &str = "SCRAM-SHA-256";

/// Length of a SHA-256 digest, and so of every SCRAM key.
const KEY_LEN: usize = 32;

/// Upper bound on the iteration count accepted from a server.
///
/// `Hi` is a serial loop, so a server that answers with a huge count stalls
/// the proxy on a CPU-bound task. PostgreSQL defaults to 4096.
const MAX_ITERATIONS: u32 = 1_000_000;

fn hmac_sha256(key: &[u8], message: &[u8]) -> Result<[u8; KEY_LEN], ProtocolError> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key)
        .map_err(|_| ProtocolError::Auth("hmac rejected the key length".to_owned()))?;
    mac.update(message);
    let out = mac.finalize().into_bytes();
    <[u8; KEY_LEN]>::try_from(out.as_slice())
        .map_err(|_| ProtocolError::Auth("hmac produced an unexpected digest size".to_owned()))
}

fn sha256(data: &[u8]) -> [u8; KEY_LEN] {
    let digest = Sha256::digest(data);
    let mut out = [0_u8; KEY_LEN];
    out.copy_from_slice(&digest);
    out
}

/// PBKDF2-HMAC-SHA-256, which SCRAM calls `Hi`.
fn hi(password: &[u8], salt: &[u8], iterations: u32) -> Result<[u8; KEY_LEN], ProtocolError> {
    if iterations == 0 || iterations > MAX_ITERATIONS {
        return Err(ProtocolError::Auth(format!(
            "iteration count {iterations} is not in 1..={MAX_ITERATIONS}"
        )));
    }
    let mut block = Vec::with_capacity(salt.len().saturating_add(4));
    block.extend_from_slice(salt);
    block.extend_from_slice(&1_u32.to_be_bytes());

    let mut u = hmac_sha256(password, &block)?;
    let mut result = u;
    for _ in 1..iterations {
        u = hmac_sha256(password, &u)?;
        for (acc, next) in result.iter_mut().zip(u.iter()) {
            *acc ^= *next;
        }
    }
    Ok(result)
}

fn xor(a: &[u8; KEY_LEN], b: &[u8; KEY_LEN]) -> [u8; KEY_LEN] {
    let mut out = [0_u8; KEY_LEN];
    for (slot, (x, y)) in out.iter_mut().zip(a.iter().zip(b.iter())) {
        *slot = x ^ y;
    }
    out
}

fn b64_encode(data: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(data)
}

fn b64_decode(text: &str) -> Result<Vec<u8>, ProtocolError> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(text)
        .map_err(|_| ProtocolError::Auth("invalid base64 in a SCRAM message".to_owned()))
}

/// A fresh nonce, base64 of 24 random bytes.
///
/// SCRAM forbids a comma in a nonce because commas separate attributes;
/// base64's alphabet contains none, so the encoding is safe as it stands.
fn nonce() -> String {
    use rand::Rng as _;
    let mut raw = [0_u8; 24];
    rand::rng().fill_bytes(&mut raw);
    b64_encode(&raw)
}

/// Splits `k=v` attributes, returning the value for `key`.
fn attribute(message: &str, key: char) -> Option<&str> {
    message.split(',').find_map(|part| {
        let mut chars = part.chars();
        (chars.next() == Some(key) && chars.next() == Some('=')).then(|| {
            // Two characters consumed, so the value starts at byte 2; every
            // attribute name is ASCII, which makes that a char boundary.
            part.get(2..).unwrap_or("")
        })
    })
}

/// The shared secret PostgreSQL stores for a role, as parsed from `pg_authid`.
///
/// Holding this is enough to *verify* a password and not enough to *know* one,
/// which is the entire reason SCRAM is worth the complexity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramVerifier {
    /// Iteration count for `Hi`.
    pub iterations: u32,
    /// The salt, already decoded.
    pub salt: Vec<u8>,
    /// `SHA256(ClientKey)`.
    pub stored_key: [u8; KEY_LEN],
    /// `HMAC(SaltedPassword, "Server Key")`.
    pub server_key: [u8; KEY_LEN],
}

impl ScramVerifier {
    /// Derives a verifier from a password, as `CREATE ROLE ... PASSWORD` does.
    ///
    /// Useful for tests and for seeding a local user list; in production the
    /// verifier should be read from the database rather than recomputed, so
    /// that the proxy never sees the password at all.
    pub fn derive(password: &str, salt: &[u8], iterations: u32) -> Result<Self, ProtocolError> {
        let salted = hi(password.as_bytes(), salt, iterations)?;
        let client_key = hmac_sha256(&salted, b"Client Key")?;
        Ok(Self {
            iterations,
            salt: salt.to_vec(),
            stored_key: sha256(&client_key),
            server_key: hmac_sha256(&salted, b"Server Key")?,
        })
    }

    /// Parses PostgreSQL's stored form:
    /// `SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>`.
    pub fn parse(encoded: &str) -> Result<Self, ProtocolError> {
        let bad = || ProtocolError::Auth("malformed SCRAM verifier".to_owned());
        let rest = encoded.strip_prefix("SCRAM-SHA-256$").ok_or_else(bad)?;
        let (params, keys) = rest.split_once('$').ok_or_else(bad)?;
        let (iterations, salt) = params.split_once(':').ok_or_else(bad)?;
        let (stored, server) = keys.split_once(':').ok_or_else(bad)?;
        let to_key = |text: &str| -> Result<[u8; KEY_LEN], ProtocolError> {
            <[u8; KEY_LEN]>::try_from(b64_decode(text)?.as_slice()).map_err(|_| bad())
        };
        Ok(Self {
            iterations: iterations.parse().map_err(|_| bad())?,
            salt: b64_decode(salt)?,
            stored_key: to_key(stored)?,
            server_key: to_key(server)?,
        })
    }

    /// Renders the verifier in PostgreSQL's stored form.
    #[must_use]
    pub fn to_postgres_string(&self) -> String {
        format!(
            "SCRAM-SHA-256${}:{}${}:{}",
            self.iterations,
            b64_encode(&self.salt),
            b64_encode(&self.stored_key),
            b64_encode(&self.server_key)
        )
    }
}

/// The keys a successful SCRAM exchange proves a client holds.
///
/// `ClientKey` is what a password is *for*: with it, and the salt and
/// iteration count it was derived under, a SCRAM client can authenticate
/// without the password ever being known. A server recovers it from the
/// client's proof, which is what lets the proxy log in to a backend as the
/// client's own role rather than as one shared superuser.
#[derive(Clone, PartialEq, Eq)]
pub struct ScramKeys {
    /// `HMAC(SaltedPassword, "Client Key")`.
    pub client_key: [u8; KEY_LEN],
    /// `HMAC(SaltedPassword, "Server Key")`, to check the backend's answer.
    pub server_key: [u8; KEY_LEN],
    /// The salt both keys were derived under.
    pub salt: Vec<u8>,
    /// The iteration count both keys were derived under.
    pub iterations: u32,
}

// Written by hand so that a log line formatting credentials can never print
// a key that authenticates as somebody.
impl std::fmt::Debug for ScramKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScramKeys")
            .field("iterations", &self.iterations)
            .finish_non_exhaustive()
    }
}

/// What a [`ScramClient`] authenticates with.
#[derive(Clone)]
enum Secret {
    Password(String),
    Keys(ScramKeys),
}

/// The client half of SCRAM-SHA-256: what the proxy sends to a backend.
pub struct ScramClient {
    secret: Secret,
    client_nonce: String,
    client_first_bare: String,
    auth_message: String,
    server_signature: Option<[u8; KEY_LEN]>,
}

// Hand-written so the secret never reaches a log.
impl std::fmt::Debug for ScramClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScramClient").finish_non_exhaustive()
    }
}

impl ScramClient {
    /// Starts an exchange for the given password.
    #[must_use]
    pub fn new(password: &str) -> Self {
        Self {
            secret: Secret::Password(password.to_owned()),
            client_nonce: nonce(),
            client_first_bare: String::new(),
            auth_message: String::new(),
            server_signature: None,
        }
    }

    /// Starts an exchange with a caller-supplied nonce, for deterministic tests.
    #[must_use]
    pub fn with_nonce(password: &str, client_nonce: &str) -> Self {
        Self {
            secret: Secret::Password(password.to_owned()),
            client_nonce: client_nonce.to_owned(),
            client_first_bare: String::new(),
            auth_message: String::new(),
            server_signature: None,
        }
    }

    /// Starts an exchange that proves possession of keys recovered from
    /// another SCRAM exchange, without the password.
    ///
    /// Works only against a server holding the same verifier — the same salt
    /// and iteration count — because the keys were derived under those. A
    /// server with a different one is refused at its first message, with a
    /// reason that names the fix, rather than failing as a wrong password.
    #[must_use]
    pub fn with_keys(keys: ScramKeys) -> Self {
        Self {
            secret: Secret::Keys(keys),
            client_nonce: nonce(),
            client_first_bare: String::new(),
            auth_message: String::new(),
            server_signature: None,
        }
    }

    /// The client-first message, including the `n,,` channel-binding header.
    ///
    /// The user name is sent empty on purpose: PostgreSQL takes it from the
    /// startup packet, and SCRAM's own `n=` field would otherwise need
    /// `SASLprep` normalisation to agree with it.
    pub fn client_first(&mut self) -> String {
        self.client_first_bare = format!("n=,r={}", self.client_nonce);
        format!("n,,{}", self.client_first_bare)
    }

    /// Consumes the server-first message and produces the client-final one.
    pub fn client_final(&mut self, server_first: &str) -> Result<String, ProtocolError> {
        let missing = |what: &str| ProtocolError::Auth(format!("server-first has no {what}"));
        let server_nonce = attribute(server_first, 'r').ok_or_else(|| missing("nonce"))?;
        let salt = attribute(server_first, 's').ok_or_else(|| missing("salt"))?;
        let iterations = attribute(server_first, 'i').ok_or_else(|| missing("iteration count"))?;

        // The server must extend our nonce, not replace it. Skipping this
        // check is what turns SCRAM into an unauthenticated key exchange.
        if !server_nonce.starts_with(&self.client_nonce) || server_nonce == self.client_nonce {
            return Err(ProtocolError::Auth(
                "server nonce does not extend the client nonce".to_owned(),
            ));
        }

        let salt = b64_decode(salt)?;
        let iterations: u32 = iterations
            .parse()
            .map_err(|_| ProtocolError::Auth("iteration count is not a number".to_owned()))?;

        let (client_key, server_key) = match &self.secret {
            Secret::Password(password) => {
                let salted = hi(password.as_bytes(), &salt, iterations)?;
                (
                    hmac_sha256(&salted, b"Client Key")?,
                    hmac_sha256(&salted, b"Server Key")?,
                )
            }
            Secret::Keys(keys) => {
                if keys.salt != salt || keys.iterations != iterations {
                    return Err(ProtocolError::Auth(
                        "the server holds a different SCRAM verifier for this role than the one \
                         the client authenticated against; give the role the same verifier \
                         everywhere"
                            .to_owned(),
                    ));
                }
                (keys.client_key, keys.server_key)
            }
        };
        let stored_key = sha256(&client_key);

        // `biws` is base64 of the `n,,` header we sent.
        let without_proof = format!("c=biws,r={server_nonce}");
        self.auth_message = format!(
            "{},{},{}",
            self.client_first_bare, server_first, without_proof
        );

        let client_signature = hmac_sha256(&stored_key, self.auth_message.as_bytes())?;
        let proof = xor(&client_key, &client_signature);

        self.server_signature = Some(hmac_sha256(&server_key, self.auth_message.as_bytes())?);

        Ok(format!("{without_proof},p={}", b64_encode(&proof)))
    }

    /// Checks the server's final message, proving the server knew the verifier.
    ///
    /// Without this step a man in the middle who only needs the connection to
    /// *appear* authenticated gets exactly that.
    pub fn verify_server_final(&self, server_final: &str) -> Result<(), ProtocolError> {
        if let Some(error) = attribute(server_final, 'e') {
            return Err(ProtocolError::Auth(format!(
                "server rejected SCRAM: {error}"
            )));
        }
        let expected = self.server_signature.ok_or_else(|| {
            ProtocolError::Auth("server-final arrived before client-final".to_owned())
        })?;
        let actual = attribute(server_final, 'v')
            .ok_or_else(|| ProtocolError::Auth("server-final has no signature".to_owned()))?;
        let actual = b64_decode(actual)?;
        if actual.ct_eq(&expected).into() {
            Ok(())
        } else {
            Err(ProtocolError::Auth(
                "server signature did not match; the peer does not hold the verifier".to_owned(),
            ))
        }
    }
}

/// The server half of SCRAM-SHA-256: what the proxy presents to a client.
#[derive(Debug)]
pub struct ScramServer {
    verifier: ScramVerifier,
    server_nonce: String,
    client_nonce: String,
    client_first_bare: String,
    server_first: String,
    proven: Option<[u8; KEY_LEN]>,
}

impl ScramServer {
    /// Starts an exchange backed by a stored verifier.
    #[must_use]
    pub fn new(verifier: ScramVerifier) -> Self {
        Self {
            verifier,
            server_nonce: nonce(),
            client_nonce: String::new(),
            client_first_bare: String::new(),
            server_first: String::new(),
            proven: None,
        }
    }

    /// Starts an exchange with a caller-supplied nonce, for deterministic tests.
    #[must_use]
    pub fn with_nonce(verifier: ScramVerifier, server_nonce: &str) -> Self {
        Self {
            verifier,
            server_nonce: server_nonce.to_owned(),
            client_nonce: String::new(),
            client_first_bare: String::new(),
            server_first: String::new(),
            proven: None,
        }
    }

    /// Consumes the client-first message and produces the server-first one.
    pub fn server_first(&mut self, client_first: &str) -> Result<String, ProtocolError> {
        // Strip the GS2 header: `n,,` or `y,,` with no channel binding, or
        // `p=name,,` with it. Channel binding is not implemented, and a client
        // that insists on it must be told so rather than quietly downgraded.
        let bare = if let Some(rest) = client_first.strip_prefix("n,,") {
            rest
        } else if let Some(rest) = client_first.strip_prefix("y,,") {
            rest
        } else {
            return Err(ProtocolError::Auth(
                "channel binding is not supported by this proxy".to_owned(),
            ));
        };

        let client_nonce = attribute(bare, 'r')
            .ok_or_else(|| ProtocolError::Auth("client-first has no nonce".to_owned()))?;

        bare.clone_into(&mut self.client_first_bare);
        client_nonce.clone_into(&mut self.client_nonce);
        let combined = format!("{client_nonce}{}", self.server_nonce);
        self.server_first = format!(
            "r={combined},s={},i={}",
            b64_encode(&self.verifier.salt),
            self.verifier.iterations
        );
        Ok(self.server_first.clone())
    }

    /// Verifies the client-final message and produces the server-final one.
    pub fn server_final(&mut self, client_final: &str) -> Result<String, ProtocolError> {
        // The nonce has to be the one this exchange agreed on. The proof
        // would not verify otherwise either, but saying so here keeps the
        // reason honest and does not lean on that.
        let expected_nonce = format!("{}{}", self.client_nonce, self.server_nonce);
        if attribute(client_final, 'r') != Some(expected_nonce.as_str()) {
            return Err(ProtocolError::Auth(
                "client-final does not carry this exchange's nonce".to_owned(),
            ));
        }
        let proof = attribute(client_final, 'p')
            .ok_or_else(|| ProtocolError::Auth("client-final has no proof".to_owned()))?;
        let proof = <[u8; KEY_LEN]>::try_from(b64_decode(proof)?.as_slice())
            .map_err(|_| ProtocolError::Auth("client proof is the wrong length".to_owned()))?;

        // The proof covers everything up to but not including `,p=`.
        let without_proof = client_final
            .rsplit_once(",p=")
            .map(|(head, _)| head)
            .ok_or_else(|| ProtocolError::Auth("client-final is malformed".to_owned()))?;

        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, without_proof
        );

        // Recover ClientKey from the proof and check that it hashes to the
        // StoredKey we hold. This is the whole verification.
        let client_signature = hmac_sha256(&self.verifier.stored_key, auth_message.as_bytes())?;
        let client_key = xor(&proof, &client_signature);
        if !bool::from(sha256(&client_key).ct_eq(&self.verifier.stored_key)) {
            return Err(ProtocolError::Auth(
                "password authentication failed".to_owned(),
            ));
        }

        let server_signature = hmac_sha256(&self.verifier.server_key, auth_message.as_bytes())?;
        self.proven = Some(client_key);
        Ok(format!("v={}", b64_encode(&server_signature)))
    }

    /// The keys the client proved it holds, once [`ScramServer::server_final`]
    /// has succeeded.
    #[must_use]
    pub fn keys(&self) -> Option<ScramKeys> {
        self.proven.map(|client_key| ScramKeys {
            client_key,
            server_key: self.verifier.server_key,
            salt: self.verifier.salt.clone(),
            iterations: self.verifier.iterations,
        })
    }
}

/// A verifier for a role that does not exist, derived from its name.
///
/// Answering an unknown name with an immediate refusal would tell anyone who
/// asks which roles exist. Running a full exchange against a stand-in instead
/// makes the two cases take the same steps and fail with the same error, as
/// PostgreSQL does. The salt depends only on the name and `secret`, so asking
/// twice gets the same salt, as it would for a real role.
pub fn stand_in_verifier(user: &str, secret: &[u8]) -> Result<ScramVerifier, ProtocolError> {
    let salt = hmac_sha256(secret, user.as_bytes())?;
    let mut password = [0_u8; KEY_LEN];
    {
        use rand::Rng as _;
        rand::rng().fill_bytes(&mut password);
    }
    ScramVerifier::derive(
        &b64_encode(&password),
        salt.get(..16).unwrap_or(&salt),
        4_096,
    )
}

/// `md5(md5(password + user) + salt)`, prefixed with `md5` as the wire expects.
///
/// Supported because real deployments still have it, and refusing to connect
/// to a legacy backend is not a security improvement if the alternative is not
/// using the proxy at all. Prefer SCRAM wherever the server allows it.
#[must_use]
pub fn md5_password(user: &str, password: &str, salt: [u8; 4]) -> String {
    use md5::Digest as _;

    let mut inner = md5::Md5::new();
    inner.update(password.as_bytes());
    inner.update(user.as_bytes());
    let inner = hex(&inner.finalize());

    let mut outer = md5::Md5::new();
    outer.update(inner.as_bytes());
    outer.update(salt);
    format!("md5{}", hex(&outer.finalize()))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut out, byte| {
        // Writing to a String cannot fail, and the result is discarded rather
        // than unwrapped so that this stays panic-free by construction.
        let _ = write!(out, "{byte:02x}");
        out
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn verifier() -> ScramVerifier {
        ScramVerifier::derive("hunter2", b"saltysalt", 4096).unwrap()
    }

    #[test]
    fn a_full_exchange_authenticates_both_directions() {
        let mut client = ScramClient::new("hunter2");
        let mut server = ScramServer::new(verifier());

        let client_first = client.client_first();
        let server_first = server.server_first(&client_first).unwrap();
        let client_final = client.client_final(&server_first).unwrap();
        let server_final = server.server_final(&client_final).unwrap();

        assert!(client.verify_server_final(&server_final).is_ok());
    }

    #[test]
    fn keys_recovered_by_a_server_authenticate_to_another_holding_the_same_verifier() {
        // The proxy authenticates the client, then logs in to a backend as
        // that client using only what the exchange proved.
        let mut client = ScramClient::new("hunter2");
        let mut proxy = ScramServer::new(verifier());
        let client_first = client.client_first();
        let server_first = proxy.server_first(&client_first).unwrap();
        let client_final = client.client_final(&server_first).unwrap();
        proxy.server_final(&client_final).unwrap();
        let keys = proxy.keys().expect("keys after success");

        let mut onward = ScramClient::with_keys(keys);
        let mut backend = ScramServer::new(verifier());
        let first = onward.client_first();
        let server_first = backend.server_first(&first).unwrap();
        let client_final = onward.client_final(&server_first).unwrap();
        let server_final = backend.server_final(&client_final).unwrap();
        assert!(onward.verify_server_final(&server_final).is_ok());
    }

    #[test]
    fn recovered_keys_are_refused_by_a_server_with_another_salt() {
        let mut client = ScramClient::new("hunter2");
        let mut proxy = ScramServer::new(verifier());
        let client_first = client.client_first();
        let server_first = proxy.server_first(&client_first).unwrap();
        let client_final = client.client_final(&server_first).unwrap();
        proxy.server_final(&client_final).unwrap();

        let other = ScramVerifier::derive("hunter2", b"othersalt", 4096).unwrap();
        let mut onward = ScramClient::with_keys(proxy.keys().unwrap());
        let mut backend = ScramServer::new(other);
        let first = onward.client_first();
        let server_first = backend.server_first(&first).unwrap();
        let error = onward.client_final(&server_first).unwrap_err();
        assert!(error.to_string().contains("same verifier"));
    }

    #[test]
    fn no_keys_are_released_before_the_proof_checks_out() {
        let mut client = ScramClient::new("wrong");
        let mut server = ScramServer::new(verifier());
        let client_first = client.client_first();
        let server_first = server.server_first(&client_first).unwrap();
        let client_final = client.client_final(&server_first).unwrap();
        assert!(server.server_final(&client_final).is_err());
        assert!(server.keys().is_none());
    }

    #[test]
    fn a_stand_in_verifier_has_a_stable_salt_and_an_unguessable_password() {
        let first = stand_in_verifier("nobody", b"secret").unwrap();
        let second = stand_in_verifier("nobody", b"secret").unwrap();
        assert_eq!(first.salt, second.salt);
        assert_ne!(first.stored_key, second.stored_key);
    }

    #[test]
    fn keys_never_print() {
        let keys = ScramKeys {
            client_key: [7; KEY_LEN],
            server_key: [9; KEY_LEN],
            salt: vec![1],
            iterations: 4096,
        };
        assert!(!format!("{keys:?}").contains('7'));
    }

    #[test]
    fn a_wrong_password_fails_at_the_server() {
        let mut client = ScramClient::new("wrong");
        let mut server = ScramServer::new(verifier());

        let client_first = client.client_first();
        let server_first = server.server_first(&client_first).unwrap();
        let client_final = client.client_final(&server_first).unwrap();

        assert!(server.server_final(&client_final).is_err());
    }

    #[test]
    fn a_server_that_does_not_hold_the_verifier_is_caught_by_the_client() {
        let mut client = ScramClient::new("hunter2");
        let mut server = ScramServer::new(verifier());
        let client_first = client.client_first();
        let server_first = server.server_first(&client_first).unwrap();
        let _ = client.client_final(&server_first).unwrap();

        // A signature from the wrong key, which a client that skips the check
        // would happily accept.
        let forged = format!("v={}", b64_encode(&[0_u8; KEY_LEN]));
        assert!(client.verify_server_final(&forged).is_err());
    }

    #[test]
    fn a_server_nonce_that_does_not_extend_ours_is_rejected() {
        let mut client = ScramClient::with_nonce("hunter2", "clientnonce");
        let _ = client.client_first();
        let hostile = format!("r=totallydifferent,s={},i=4096", b64_encode(b"salt"));
        assert!(client.client_final(&hostile).is_err());
    }

    #[test]
    fn a_server_nonce_equal_to_ours_is_rejected() {
        let mut client = ScramClient::with_nonce("hunter2", "clientnonce");
        let _ = client.client_first();
        let hostile = format!("r=clientnonce,s={},i=4096", b64_encode(b"salt"));
        assert!(client.client_final(&hostile).is_err());
    }

    #[test]
    fn channel_binding_is_refused_rather_than_downgraded() {
        let mut server = ScramServer::new(verifier());
        assert!(
            server
                .server_first("p=tls-server-end-point,,n=,r=abc")
                .is_err()
        );
    }

    #[test]
    fn an_absurd_iteration_count_is_refused_before_it_burns_cpu() {
        let mut client = ScramClient::with_nonce("hunter2", "abc");
        let _ = client.client_first();
        let greedy = format!("r=abcdef,s={},i=4000000000", b64_encode(b"salt"));
        assert!(client.client_final(&greedy).is_err());
    }

    #[test]
    fn verifiers_round_trip_through_the_postgres_text_form() {
        let original = verifier();
        let text = original.to_postgres_string();
        assert!(text.starts_with("SCRAM-SHA-256$4096:"));
        assert_eq!(ScramVerifier::parse(&text).unwrap(), original);
    }

    #[test]
    fn a_truncated_verifier_is_an_error_not_a_panic() {
        assert!(ScramVerifier::parse("SCRAM-SHA-256$4096:abc").is_err());
        assert!(ScramVerifier::parse("").is_err());
        assert!(ScramVerifier::parse("md5deadbeef").is_err());
    }

    #[test]
    fn md5_matches_the_documented_construction() {
        // md5("hunter2" + "alice") then md5 of that hex with the salt.
        let got = md5_password("alice", "hunter2", [1, 2, 3, 4]);
        assert!(got.starts_with("md5"));
        assert_eq!(got.len(), 35);
        // Stable across runs: the construction has no randomness in it.
        assert_eq!(got, md5_password("alice", "hunter2", [1, 2, 3, 4]));
        assert_ne!(got, md5_password("alice", "hunter2", [1, 2, 3, 5]));
    }

    #[test]
    fn attributes_are_found_without_matching_a_value_that_looks_like_a_key() {
        let message = "r=nonce,s=c2FsdA==,i=4096";
        assert_eq!(attribute(message, 'r'), Some("nonce"));
        assert_eq!(attribute(message, 'i'), Some("4096"));
        assert_eq!(attribute(message, 'z'), None);
    }
}
