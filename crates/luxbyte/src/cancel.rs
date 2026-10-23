//! Routing a client's `CancelRequest` to the work it means.
//!
//! The key a client is given at startup is the proxy's own, because the
//! request arrives on a new connection with nothing else in it, and through
//! a proxy the work it means may be running on several backends at once.
//! The key maps to the session's [`CancelScope`], which knows every backend
//! connection the session is using.

use std::collections::HashMap;

use luxbyte_pool::CancelScope;
use luxbyte_proto::startup::CancelRequest;
use parking_lot::Mutex;

/// Every live session's key and scope.
#[derive(Debug, Default)]
pub struct CancelRegistry {
    sessions: Mutex<HashMap<(i32, i32), CancelScope>>,
}

impl CancelRegistry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Issues a key for a session. The secret is random, so a key cannot be
    /// guessed from the ones handed out before it; the process id only has
    /// to be unique.
    pub fn register(&self, scope: CancelScope) -> CancelRequest {
        use rand::RngExt as _;
        let mut rng = rand::rng();
        let mut sessions = self.sessions.lock();
        loop {
            // Positive, as PostgreSQL's are: some clients print or parse the
            // process id as if it were a real pid.
            let process_id = rng.random_range(1..i32::MAX);
            let secret_key: i32 = rng.random();
            let key = (process_id, secret_key);
            if let std::collections::hash_map::Entry::Vacant(slot) = sessions.entry(key) {
                slot.insert(scope);
                return CancelRequest {
                    process_id,
                    secret_key,
                };
            }
        }
    }

    /// Forgets a session's key, when it ends.
    pub fn forget(&self, key: CancelRequest) {
        self.sessions
            .lock()
            .remove(&(key.process_id, key.secret_key));
    }

    /// The scope a request names, if its key is one this proxy issued.
    #[must_use]
    pub fn lookup(&self, key: CancelRequest) -> Option<CancelScope> {
        self.sessions
            .lock()
            .get(&(key.process_id, key.secret_key))
            .cloned()
    }

    /// How many sessions hold a key.
    #[must_use]
    pub fn len(&self) -> usize {
        self.sessions.lock().len()
    }

    /// Whether no session holds a key.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_finds_its_session_until_it_is_forgotten() {
        let registry = CancelRegistry::new();
        let key = registry.register(CancelScope::new());
        assert!(key.process_id > 0);
        assert!(registry.lookup(key).is_some());
        registry.forget(key);
        assert!(registry.lookup(key).is_none());
    }

    #[test]
    fn the_secret_has_to_match_not_just_the_process() {
        let registry = CancelRegistry::new();
        let key = registry.register(CancelScope::new());
        let guessed = CancelRequest {
            process_id: key.process_id,
            secret_key: key.secret_key.wrapping_add(1),
        };
        assert!(registry.lookup(guessed).is_none());
    }
}
