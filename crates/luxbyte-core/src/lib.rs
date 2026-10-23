//! Topology, the key directory, and the routing planner.
//!
//! This crate holds everything that decides *where* a statement runs, and
//! nothing that performs I/O. Keeping the decision separate from the
//! execution is what lets the awkward cases — a write during a cutover, a
//! join whose two tables are on different continents, an aggregate over
//! three regions — be tested exhaustively and in microseconds.

pub mod directory;
pub mod router;
pub mod topology;

pub use directory::{
    Directory, KeyState, Location, MemoryDirectory, RelocationError, RelocationPhase,
};
pub use router::{LocalIntent, ParameterValues, Plan, Refusal, Router};
pub use topology::{
    Endpoint, Placement, Region, TablePolicy, Topology, TopologyError, UnconfiguredTables,
};
