//! Which region holds which key.
//!
//! The directory is the one piece of state that makes geo-partitioning
//! different from hash sharding: it is an explicit, mutable mapping, so a key
//! can *move* when the person behind it moves.
//!
//! Relocation is modelled as two phases rather than one atomic switch,
//! because copying a key's rows between continents takes real time and the
//! application is not allowed to stop:
//!
//! - **Copying** — rows are being streamed to the destination. The source is
//!   still authoritative, so reads and writes both go there. Writes during
//!   this phase are fine; the copy picks them up.
//! - **Cutover** — the brief window where the destination is caught up and
//!   the mapping is about to flip. Reads still go to the source. Writes are
//!   refused with SQLSTATE `40001`, which every PostgreSQL client library
//!   already treats as "retry this transaction".
//!
//! Choosing an existing retryable SQLSTATE rather than inventing an error is
//! deliberate: it means a correctly written application survives a
//! relocation without being changed or even recompiled.

use std::collections::BTreeMap;

use parking_lot::RwLock;

/// Where a key currently lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyState {
    /// Settled in one region.
    Resident {
        /// The region holding it.
        region: String,
    },
    /// Being moved.
    Relocating {
        /// The region still authoritative for reads.
        from: String,
        /// The region it is moving to.
        to: String,
        /// How far the move has got.
        phase: RelocationPhase,
    },
}

/// How far a relocation has progressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelocationPhase {
    /// Rows are streaming; the source still serves everything.
    Copying,
    /// The destination is caught up; writes pause for the flip.
    Cutover,
}

impl KeyState {
    /// The region a read should go to.
    #[must_use]
    pub fn read_region(&self) -> &str {
        match self {
            Self::Resident { region } => region,
            // The source stays authoritative until the mapping flips, so a
            // read never sees the half-copied destination.
            Self::Relocating { from, .. } => from,
        }
    }

    /// The region a write should go to, or `None` while writes are paused.
    #[must_use]
    pub fn write_region(&self) -> Option<&str> {
        match self {
            Self::Resident { region } => Some(region),
            Self::Relocating {
                from,
                phase: RelocationPhase::Copying,
                ..
            } => Some(from),
            Self::Relocating {
                phase: RelocationPhase::Cutover,
                ..
            } => None,
        }
    }
}

/// Where a key lives, and how confidently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Location {
    /// The directory holds an entry.
    Known(KeyState),
    /// No entry: the key has never been seen.
    ///
    /// Not an error. A key with no entry is placed in the proxy's home
    /// region on first write, which is the whole point — a user's rows are
    /// created where the user is.
    Unseen,
}

/// A lookup table from key to region.
pub trait Directory: Send + Sync {
    /// Finds where a key lives.
    fn locate(&self, table: &str, key: &str) -> Location;

    /// Records a key as resident in a region.
    fn place(&self, table: &str, key: &str, region: &str);

    /// Begins a relocation, leaving the source authoritative.
    fn begin_relocation(&self, table: &str, key: &str, to: &str) -> Result<(), RelocationError>;

    /// Moves a relocation into its cutover window, pausing writes.
    fn begin_cutover(&self, table: &str, key: &str) -> Result<(), RelocationError>;

    /// Completes a relocation, making the destination authoritative.
    fn finish_relocation(&self, table: &str, key: &str) -> Result<(), RelocationError>;

    /// Abandons a relocation, leaving the key where it was.
    fn abort_relocation(&self, table: &str, key: &str) -> Result<(), RelocationError>;

    /// How many keys the directory knows about.
    fn len(&self) -> usize;

    /// Whether the directory is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Things that can go wrong when moving a key.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum RelocationError {
    /// The key has no directory entry, so there is nothing to move.
    #[error("key {key} of {table} has no directory entry")]
    NotFound {
        /// The table.
        table: String,
        /// The key.
        key: String,
    },
    /// A relocation is already running for this key.
    #[error("key {key} of {table} is already relocating to {to}")]
    AlreadyRelocating {
        /// The table.
        table: String,
        /// The key.
        key: String,
        /// Where the running relocation is headed.
        to: String,
    },
    /// The key is not relocating, so there is nothing to advance or finish.
    #[error("key {key} of {table} is not relocating")]
    NotRelocating {
        /// The table.
        table: String,
        /// The key.
        key: String,
    },
    /// The destination is where the key already is.
    #[error("key {key} of {table} is already in {region}")]
    AlreadyThere {
        /// The table.
        table: String,
        /// The key.
        key: String,
        /// The region.
        region: String,
    },
}

/// An in-memory directory.
///
/// Durable enough for a single proxy and for tests; a deployment with
/// several proxies replicates this mapping between them, which is the one
/// place a consensus protocol genuinely earns its keep.
#[derive(Debug, Default)]
pub struct MemoryDirectory {
    entries: RwLock<BTreeMap<(String, String), KeyState>>,
}

impl MemoryDirectory {
    /// An empty directory.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds a directory pre-populated with `(table, key, region)` triples.
    #[must_use]
    pub fn with_entries<'a>(
        entries: impl IntoIterator<Item = (&'a str, &'a str, &'a str)>,
    ) -> Self {
        let directory = Self::new();
        for (table, key, region) in entries {
            directory.place(table, key, region);
        }
        directory
    }

    /// Every entry, for the operator console and for tests.
    #[must_use]
    pub fn snapshot(&self) -> Vec<(String, String, KeyState)> {
        self.entries
            .read()
            .iter()
            .map(|((table, key), state)| (table.clone(), key.clone(), state.clone()))
            .collect()
    }

    fn mutate<F>(&self, table: &str, key: &str, change: F) -> Result<(), RelocationError>
    where
        F: FnOnce(&KeyState) -> Result<KeyState, RelocationError>,
    {
        let mut entries = self.entries.write();
        let id = (table.to_owned(), key.to_owned());
        let current = entries.get(&id).ok_or_else(|| RelocationError::NotFound {
            table: table.to_owned(),
            key: key.to_owned(),
        })?;
        let next = change(current)?;
        entries.insert(id, next);
        Ok(())
    }
}

impl Directory for MemoryDirectory {
    fn locate(&self, table: &str, key: &str) -> Location {
        self.entries
            .read()
            .get(&(table.to_owned(), key.to_owned()))
            .map_or(Location::Unseen, |state| Location::Known(state.clone()))
    }

    fn place(&self, table: &str, key: &str, region: &str) {
        self.entries.write().insert(
            (table.to_owned(), key.to_owned()),
            KeyState::Resident {
                region: region.to_owned(),
            },
        );
    }

    fn begin_relocation(&self, table: &str, key: &str, to: &str) -> Result<(), RelocationError> {
        self.mutate(table, key, |current| match current {
            KeyState::Resident { region } if region == to => Err(RelocationError::AlreadyThere {
                table: table.to_owned(),
                key: key.to_owned(),
                region: region.clone(),
            }),
            KeyState::Resident { region } => Ok(KeyState::Relocating {
                from: region.clone(),
                to: to.to_owned(),
                phase: RelocationPhase::Copying,
            }),
            KeyState::Relocating { to: running, .. } => Err(RelocationError::AlreadyRelocating {
                table: table.to_owned(),
                key: key.to_owned(),
                to: running.clone(),
            }),
        })
    }

    fn begin_cutover(&self, table: &str, key: &str) -> Result<(), RelocationError> {
        self.mutate(table, key, |current| match current {
            KeyState::Relocating { from, to, .. } => Ok(KeyState::Relocating {
                from: from.clone(),
                to: to.clone(),
                phase: RelocationPhase::Cutover,
            }),
            KeyState::Resident { .. } => Err(RelocationError::NotRelocating {
                table: table.to_owned(),
                key: key.to_owned(),
            }),
        })
    }

    fn finish_relocation(&self, table: &str, key: &str) -> Result<(), RelocationError> {
        self.mutate(table, key, |current| match current {
            KeyState::Relocating { to, .. } => Ok(KeyState::Resident { region: to.clone() }),
            KeyState::Resident { .. } => Err(RelocationError::NotRelocating {
                table: table.to_owned(),
                key: key.to_owned(),
            }),
        })
    }

    fn abort_relocation(&self, table: &str, key: &str) -> Result<(), RelocationError> {
        self.mutate(table, key, |current| match current {
            KeyState::Relocating { from, .. } => Ok(KeyState::Resident {
                region: from.clone(),
            }),
            KeyState::Resident { .. } => Err(RelocationError::NotRelocating {
                table: table.to_owned(),
                key: key.to_owned(),
            }),
        })
    }

    fn len(&self) -> usize {
        self.entries.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn directory() -> MemoryDirectory {
        MemoryDirectory::with_entries([("users", "alice", "eu-west")])
    }

    #[test]
    fn an_unseen_key_is_reported_as_unseen_rather_than_guessed() {
        assert_eq!(directory().locate("users", "nobody"), Location::Unseen);
    }

    #[test]
    fn a_resident_key_serves_reads_and_writes_from_its_region() {
        let Location::Known(state) = directory().locate("users", "alice") else {
            panic!("alice should be known");
        };
        assert_eq!(state.read_region(), "eu-west");
        assert_eq!(state.write_region(), Some("eu-west"));
    }

    #[test]
    fn during_copying_the_source_still_serves_everything() {
        let directory = directory();
        directory
            .begin_relocation("users", "alice", "us-east")
            .expect("relocation should start");
        let Location::Known(state) = directory.locate("users", "alice") else {
            panic!("alice should be known");
        };
        assert_eq!(state.read_region(), "eu-west");
        assert_eq!(state.write_region(), Some("eu-west"));
    }

    #[test]
    fn during_cutover_reads_continue_but_writes_pause() {
        let directory = directory();
        directory
            .begin_relocation("users", "alice", "us-east")
            .expect("relocation should start");
        directory
            .begin_cutover("users", "alice")
            .expect("cutover should start");
        let Location::Known(state) = directory.locate("users", "alice") else {
            panic!("alice should be known");
        };
        assert_eq!(state.read_region(), "eu-west");
        assert_eq!(state.write_region(), None);
    }

    #[test]
    fn finishing_a_relocation_moves_the_key() {
        let directory = directory();
        directory
            .begin_relocation("users", "alice", "us-east")
            .expect("relocation should start");
        directory
            .finish_relocation("users", "alice")
            .expect("relocation should finish");
        assert_eq!(
            directory.locate("users", "alice"),
            Location::Known(KeyState::Resident {
                region: "us-east".to_owned()
            })
        );
    }

    #[test]
    fn aborting_a_relocation_leaves_the_key_where_it_was() {
        let directory = directory();
        directory
            .begin_relocation("users", "alice", "us-east")
            .expect("relocation should start");
        directory
            .abort_relocation("users", "alice")
            .expect("relocation should abort");
        assert_eq!(
            directory.locate("users", "alice"),
            Location::Known(KeyState::Resident {
                region: "eu-west".to_owned()
            })
        );
    }

    #[test]
    fn a_second_relocation_for_the_same_key_is_refused() {
        let directory = directory();
        directory
            .begin_relocation("users", "alice", "us-east")
            .expect("relocation should start");
        assert_eq!(
            directory.begin_relocation("users", "alice", "ap-south"),
            Err(RelocationError::AlreadyRelocating {
                table: "users".to_owned(),
                key: "alice".to_owned(),
                to: "us-east".to_owned(),
            })
        );
    }

    #[test]
    fn relocating_a_key_to_where_it_already_is_is_refused() {
        assert_eq!(
            directory().begin_relocation("users", "alice", "eu-west"),
            Err(RelocationError::AlreadyThere {
                table: "users".to_owned(),
                key: "alice".to_owned(),
                region: "eu-west".to_owned(),
            })
        );
    }

    #[test]
    fn moving_a_key_that_does_not_exist_is_refused() {
        assert_eq!(
            directory().begin_relocation("users", "ghost", "us-east"),
            Err(RelocationError::NotFound {
                table: "users".to_owned(),
                key: "ghost".to_owned(),
            })
        );
    }

    #[test]
    fn finishing_a_relocation_that_never_started_is_refused() {
        assert_eq!(
            directory().finish_relocation("users", "alice"),
            Err(RelocationError::NotRelocating {
                table: "users".to_owned(),
                key: "alice".to_owned(),
            })
        );
    }

    #[test]
    fn keys_in_different_tables_do_not_collide() {
        let directory = MemoryDirectory::with_entries([
            ("users", "alice", "eu-west"),
            ("orders", "alice", "us-east"),
        ]);
        let Location::Known(users) = directory.locate("users", "alice") else {
            panic!("users.alice should be known");
        };
        let Location::Known(orders) = directory.locate("orders", "alice") else {
            panic!("orders.alice should be known");
        };
        assert_eq!(users.read_region(), "eu-west");
        assert_eq!(orders.read_region(), "us-east");
        assert_eq!(directory.len(), 2);
    }
}
