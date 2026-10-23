//! Which region holds which key.
//!
//! The directory is the one piece of state that makes geo-partitioning
//! different from hash sharding: it is an explicit, mutable mapping, so a key
//! can *move* when the person behind it moves.
//!
//! A move copies the key's rows to the destination, deletes them from the
//! source and changes the mapping in one distributed transaction, so the rows
//! are never in both regions or neither once it commits. Around that
//! transaction the key goes through two phases:
//!
//! - **Cutover** — writes to the key wait. Reads still go to the source,
//!   which holds every row and, with writes waiting, is not changing.
//! - **Committing** — reads wait too. The regions commit one after another,
//!   so for a moment the rows may be visible in both or in neither, and a
//!   read in that moment would see one of those.
//!
//! Waiting rather than failing is deliberate: a statement held for the
//! second a move takes costs the application nothing, where an error would
//! cost it a retry path. Only a statement that has waited too long, or one
//! inside a transaction that may be holding what the move needs, is refused,
//! and then with SQLSTATE `40001`, which every client library already treats
//! as "retry this transaction".

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
        /// Where the rows are until the move commits. `None` for a key that
        /// had no directory entry, whose rows may be in any region.
        from: Option<String>,
        /// The region it is moving to.
        to: String,
        /// How far the move has got.
        phase: RelocationPhase,
    },
}

/// How far a relocation has progressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RelocationPhase {
    /// Writes wait; the source still serves reads.
    Cutover,
    /// The move is committing; reads wait as well.
    Committing,
}

/// Where a read of a key goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reads<'a> {
    /// One region holds every row.
    Region(&'a str),
    /// The rows may be in any region.
    Everywhere,
    /// Nowhere yet: the key is between regions.
    Wait,
}

impl KeyState {
    /// Where a read should go.
    #[must_use]
    pub fn reads(&self) -> Reads<'_> {
        match self {
            Self::Resident { region } => Reads::Region(region),
            Self::Relocating {
                from,
                phase: RelocationPhase::Cutover,
                ..
            } => from.as_deref().map_or(Reads::Everywhere, Reads::Region),
            Self::Relocating {
                phase: RelocationPhase::Committing,
                ..
            } => Reads::Wait,
        }
    }

    /// The region a write should go to, or `None` while writes wait.
    #[must_use]
    pub fn write_region(&self) -> Option<&str> {
        match self {
            Self::Resident { region } => Some(region),
            Self::Relocating { .. } => None,
        }
    }

    /// Where the key's rows are right now, if one region is known to hold
    /// them all.
    #[must_use]
    pub fn source(&self) -> Option<&str> {
        match self {
            Self::Resident { region } => Some(region),
            Self::Relocating { from, .. } => from.as_deref(),
        }
    }

    const fn phase(&self) -> Option<RelocationPhase> {
        match self {
            Self::Resident { .. } => None,
            Self::Relocating { phase, .. } => Some(*phase),
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

    /// The furthest phase any key of `table` has reached in a move, or
    /// `None` if none of its keys is moving.
    ///
    /// A statement that touches every key of a table — an `UPDATE` with no
    /// key in its `WHERE` — touches the moving ones too.
    fn moving_in(&self, table: &str) -> Option<RelocationPhase>;

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
}

#[derive(Debug, Default)]
struct Entries {
    keys: BTreeMap<(String, String), KeyState>,
    /// For each table, how many of its keys are in each phase of a move, so
    /// that asking whether a table has a moving key does not scan it.
    moving: BTreeMap<String, BTreeMap<RelocationPhase, usize>>,
}

impl Entries {
    fn count(&mut self, table: &str, phase: Option<RelocationPhase>, up: bool) {
        let Some(phase) = phase else { return };
        let phases = self.moving.entry(table.to_owned()).or_default();
        let count = phases.entry(phase).or_insert(0);
        *count = if up {
            count.saturating_add(1)
        } else {
            count.saturating_sub(1)
        };
        if *count == 0 {
            phases.remove(&phase);
        }
        if phases.is_empty() {
            self.moving.remove(table);
        }
    }

    fn set(&mut self, table: &str, key: &str, state: Option<KeyState>) {
        let id = (table.to_owned(), key.to_owned());
        let old = match state {
            Some(state) => {
                self.count(table, state.phase(), true);
                self.keys.insert(id, state)
            }
            None => self.keys.remove(&id),
        };
        if let Some(old) = old {
            self.count(table, old.phase(), false);
        }
    }
}

/// An in-memory directory.
///
/// The proxy keeps its working copy here, loaded from a durable store when
/// one is configured; every lookup is a map read with no I/O.
#[derive(Debug, Default)]
pub struct MemoryDirectory {
    entries: RwLock<Entries>,
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
            .keys
            .iter()
            .map(|((table, key), state)| (table.clone(), key.clone(), state.clone()))
            .collect()
    }

    /// Sets a key's state outright.
    pub fn set(&self, table: &str, key: &str, state: KeyState) {
        self.entries.write().set(table, key, Some(state));
    }

    /// Removes a key's entry, so that it is unseen again.
    pub fn forget(&self, table: &str, key: &str) {
        self.entries.write().set(table, key, None);
    }

    /// Starts moving a key to `to`: writes to it wait from here on.
    ///
    /// A key with no entry can be moved too; its source is then unknown
    /// until the mover finds its rows.
    pub fn begin_relocation(
        &self,
        table: &str,
        key: &str,
        to: &str,
    ) -> Result<(), RelocationError> {
        let mut entries = self.entries.write();
        let from = match entries.keys.get(&(table.to_owned(), key.to_owned())) {
            Some(KeyState::Relocating { to: running, .. }) => {
                return Err(RelocationError::AlreadyRelocating {
                    table: table.to_owned(),
                    key: key.to_owned(),
                    to: running.clone(),
                });
            }
            Some(KeyState::Resident { region }) => Some(region.clone()),
            None => None,
        };
        entries.set(
            table,
            key,
            Some(KeyState::Relocating {
                from,
                to: to.to_owned(),
                phase: RelocationPhase::Cutover,
            }),
        );
        Ok(())
    }

    /// Records where a moving key's rows were found.
    pub fn found_source(
        &self,
        table: &str,
        key: &str,
        region: Option<&str>,
    ) -> Result<(), RelocationError> {
        self.advance(table, key, |from, to, phase| KeyState::Relocating {
            from: region.map(ToOwned::to_owned).or_else(|| from.clone()),
            to: to.to_owned(),
            phase,
        })
    }

    /// Moves a relocation into its commit: reads of the key wait as well.
    pub fn begin_committing(&self, table: &str, key: &str) -> Result<(), RelocationError> {
        self.advance(table, key, |from, to, _| KeyState::Relocating {
            from: from.clone(),
            to: to.to_owned(),
            phase: RelocationPhase::Committing,
        })
    }

    /// Completes a relocation, making the destination authoritative.
    pub fn finish_relocation(&self, table: &str, key: &str) -> Result<(), RelocationError> {
        self.advance(table, key, |_, to, _| KeyState::Resident {
            region: to.to_owned(),
        })
    }

    /// Abandons a relocation, leaving the key where it was — or unseen, if
    /// it was unseen before.
    pub fn abort_relocation(&self, table: &str, key: &str) -> Result<(), RelocationError> {
        let mut entries = self.entries.write();
        let restored = match entries.keys.get(&(table.to_owned(), key.to_owned())) {
            Some(KeyState::Relocating { from, .. }) => {
                from.clone().map(|region| KeyState::Resident { region })
            }
            _ => {
                return Err(RelocationError::NotRelocating {
                    table: table.to_owned(),
                    key: key.to_owned(),
                });
            }
        };
        entries.set(table, key, restored);
        Ok(())
    }

    fn advance<F>(&self, table: &str, key: &str, next: F) -> Result<(), RelocationError>
    where
        F: FnOnce(&Option<String>, &str, RelocationPhase) -> KeyState,
    {
        let mut entries = self.entries.write();
        let state = match entries.keys.get(&(table.to_owned(), key.to_owned())) {
            Some(KeyState::Relocating { from, to, phase }) => next(from, to, *phase),
            _ => {
                return Err(RelocationError::NotRelocating {
                    table: table.to_owned(),
                    key: key.to_owned(),
                });
            }
        };
        entries.set(table, key, Some(state));
        Ok(())
    }
}

impl Directory for MemoryDirectory {
    fn locate(&self, table: &str, key: &str) -> Location {
        self.entries
            .read()
            .keys
            .get(&(table.to_owned(), key.to_owned()))
            .map_or(Location::Unseen, |state| Location::Known(state.clone()))
    }

    fn place(&self, table: &str, key: &str, region: &str) {
        self.set(
            table,
            key,
            KeyState::Resident {
                region: region.to_owned(),
            },
        );
    }

    fn moving_in(&self, table: &str) -> Option<RelocationPhase> {
        self.entries
            .read()
            .moving
            .get(table)
            .and_then(|phases| phases.keys().next_back().copied())
    }

    fn len(&self) -> usize {
        self.entries.read().keys.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn directory() -> MemoryDirectory {
        MemoryDirectory::with_entries([("users", "alice", "eu-west")])
    }

    fn state(directory: &MemoryDirectory, key: &str) -> KeyState {
        let Location::Known(state) = directory.locate("users", key) else {
            panic!("{key} should be known");
        };
        state
    }

    #[test]
    fn an_unseen_key_is_reported_as_unseen_rather_than_guessed() {
        assert_eq!(directory().locate("users", "nobody"), Location::Unseen);
    }

    #[test]
    fn a_resident_key_serves_reads_and_writes_from_its_region() {
        let state = state(&directory(), "alice");
        assert_eq!(state.reads(), Reads::Region("eu-west"));
        assert_eq!(state.write_region(), Some("eu-west"));
    }

    #[test]
    fn during_cutover_reads_continue_from_the_source_but_writes_wait() {
        let directory = directory();
        directory
            .begin_relocation("users", "alice", "us-east")
            .expect("relocation should start");
        let state = state(&directory, "alice");
        assert_eq!(state.reads(), Reads::Region("eu-west"));
        assert_eq!(state.write_region(), None);
        assert_eq!(directory.moving_in("users"), Some(RelocationPhase::Cutover));
    }

    #[test]
    fn while_committing_reads_wait_too() {
        let directory = directory();
        directory
            .begin_relocation("users", "alice", "us-east")
            .expect("relocation should start");
        directory
            .begin_committing("users", "alice")
            .expect("commit should start");
        let state = state(&directory, "alice");
        assert_eq!(state.reads(), Reads::Wait);
        assert_eq!(state.write_region(), None);
        assert_eq!(
            directory.moving_in("users"),
            Some(RelocationPhase::Committing)
        );
    }

    #[test]
    fn an_unseen_key_being_moved_is_read_everywhere_until_its_rows_are_found() {
        let directory = directory();
        directory
            .begin_relocation("users", "ghost", "us-east")
            .expect("an unseen key can move");
        assert_eq!(state(&directory, "ghost").reads(), Reads::Everywhere);
        directory
            .found_source("users", "ghost", Some("ap-south"))
            .expect("still relocating");
        assert_eq!(
            state(&directory, "ghost").reads(),
            Reads::Region("ap-south")
        );
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
            state(&directory, "alice"),
            KeyState::Resident {
                region: "us-east".to_owned()
            }
        );
        assert_eq!(directory.moving_in("users"), None);
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
            state(&directory, "alice"),
            KeyState::Resident {
                region: "eu-west".to_owned()
            }
        );
        assert_eq!(directory.moving_in("users"), None);
    }

    #[test]
    fn aborting_the_move_of_an_unseen_key_leaves_it_unseen() {
        let directory = directory();
        directory
            .begin_relocation("users", "ghost", "us-east")
            .expect("an unseen key can move");
        directory
            .abort_relocation("users", "ghost")
            .expect("relocation should abort");
        assert_eq!(directory.locate("users", "ghost"), Location::Unseen);
        assert_eq!(directory.len(), 1);
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
    fn a_table_counts_as_moving_until_its_last_moving_key_settles() {
        let directory = MemoryDirectory::with_entries([
            ("users", "alice", "eu-west"),
            ("users", "bob", "eu-west"),
        ]);
        directory
            .begin_relocation("users", "alice", "us-east")
            .unwrap();
        directory
            .begin_relocation("users", "bob", "us-east")
            .unwrap();
        directory.begin_committing("users", "bob").unwrap();
        assert_eq!(
            directory.moving_in("users"),
            Some(RelocationPhase::Committing)
        );
        directory.finish_relocation("users", "bob").unwrap();
        assert_eq!(directory.moving_in("users"), Some(RelocationPhase::Cutover));
        directory.finish_relocation("users", "alice").unwrap();
        assert_eq!(directory.moving_in("users"), None);
        assert_eq!(directory.moving_in("orders"), None);
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
        assert_eq!(users.reads(), Reads::Region("eu-west"));
        assert_eq!(orders.reads(), Reads::Region("us-east"));
        assert_eq!(directory.len(), 2);
    }
}
