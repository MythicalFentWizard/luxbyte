//! The extended query protocol: `Parse`, `Bind`, `Describe`, `Execute`,
//! `Close`, `Sync` and `Flush`.
//!
//! # Why the proxy keeps the statements
//!
//! A client prepares a statement once and binds it many times, but each
//! `Bind` may route to a different region, and outside a transaction each
//! batch borrows whatever pooled connection is free. Forwarding the client's
//! `Parse` to one backend would leave the statement on a connection the next
//! `Bind` may never see. So the proxy holds every statement and portal
//! itself, and prepares it on whichever connection a `Bind` is routed to.
//!
//! A statement the client named is prepared there under a name the proxy
//! derives from its text, and the connection remembers it, so binding it
//! again on that connection costs no parse: once each pooled connection has
//! seen it, a named statement runs as cheaply as it would against
//! PostgreSQL directly. A connection's copy is only used if it was parsed
//! after the client's own `Parse`, so a schema change behaves as it would on
//! one server: before the client prepared, it is simply part of the
//! statement; after, PostgreSQL replans, or refuses as it would directly
//! when the change alters the result type. An unnamed statement stays
//! unnamed on the backend, because a client that did not name it asked for
//! a one-off plan, and a named one would let PostgreSQL switch to a generic
//! plan behind its back.
//!
//! # Batches
//!
//! Nothing is sent until the client sends `Sync` or `Flush`, because the
//! region is only known at `Bind`, and because everything up to a `Sync` is
//! one implicit transaction. Messages are planned as they arrive and their
//! backend messages queued on a *lane*: a borrowed connection for the
//! implicit transaction, or a participant of the open explicit one. At the
//! `Sync` the queue goes out in one write, so a `Parse`/`Bind`/`Execute`
//! batch costs one round trip, as it would against PostgreSQL directly.
//!
//! # Errors
//!
//! PostgreSQL answers an error by discarding everything up to the next
//! `Sync`, and rolling back the implicit transaction. The proxy keeps that
//! promise across connections:
//!
//! - Effects the proxy applies itself — a new statement, a new portal, a
//!   `SET` — are journaled and undone when an earlier message fails.
//! - Effects it cannot undo, such as `COMMIT`, wait until everything queued
//!   before them has been answered.
//! - A lane whose backend did not see the error is never sent the `Sync`
//!   that would commit it. Its connection is dropped instead, which
//!   PostgreSQL treats as a rollback.
//!
//! # Statements across regions
//!
//! A statement whose plan spans regions — a read to combine, a write to
//! commit in two phases — is run by the same code as a simple query, with
//! its parameters written into the text as literals of their own types.
//! PostgreSQL renders each value and names its type (`$1::text,
//! pg_typeof($1)`), so the literal means exactly what the parameter did,
//! whatever its type or format. The answer is sent in the formats the
//! client asked for; binary ones are encoded by the home region, which
//! knows every type's binary form.
//!
//! A write across regions outside a transaction opens one for the rest of
//! the batch, committed in two phases at the `Sync`, so the batch stays all
//! or nothing as PostgreSQL promises.
//!
//! # What is refused
//!
//! A batch outside a transaction that has already written to one region
//! cannot then write to another, since what it wrote is not part of any
//! transaction the proxy can commit atomically with the rest; nor can it
//! read across regions, which would miss what it wrote. Both refusals say
//! what to do.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use bytes::Bytes;
use luxbyte_core::{LocalIntent, ParameterValues, Plan, Refusal};
use luxbyte_pool::{PoolError, PooledConnection, QueryResult, Step, StepKind, StepReply};
use luxbyte_proto::backend::{Backend, TransactionStatus, build};
use luxbyte_proto::frontend::{self, Bind, Format, Parse, Target};
use luxbyte_proto::{RawMessage, tag};
use luxbyte_sql::{Access, Prepared, TransactionOp};

use super::{Session, SessionError, to_refusal, txn_refusal};

/// Identifies a statement's text and declared types, which together decide
/// what the backend makes of it.
type StatementKey = (String, Vec<i32>);

/// The name the proxy gives its own portals on a backend.
///
/// Replaying a `SET` needs a portal. Using the unnamed one would destroy a
/// client's unnamed portal that is still open inside a transaction.
const SETTING_PORTAL: &str = "luxbyte.setting";

/// How many parameter type lists to remember from `Describe` answers.
///
/// Bounded because a client that builds SQL by concatenation produces a new
/// statement for every value. Forgetting costs a key that routes as unknown,
/// never a wrong one.
const LEARNED_TYPES_LIMIT: usize = 1_024;

/// What a prepared statement is.
#[derive(Debug)]
enum Body {
    /// Empty text, answered with `EmptyQueryResponse`.
    Empty,
    /// A console command, which the proxy answers itself.
    Console,
    /// SQL for a backend.
    Sql(Prepared),
}

/// A statement the client prepared.
#[derive(Debug)]
struct Statement {
    key: StatementKey,
    body: Body,
    /// The name it is prepared under on backend connections, for a
    /// statement the client named; `None` keeps it unnamed there.
    backend_name: Option<String>,
    /// When the client parsed it: a backend copy parsed earlier is not
    /// used.
    parsed_at: std::time::Instant,
}

impl Statement {
    fn sql(&self) -> &str {
        &self.key.0
    }

    fn declared(&self) -> &[i32] {
        &self.key.1
    }
}

/// Where a portal runs.
#[derive(Debug, Clone)]
enum Destination {
    /// On a backend, through a lane.
    Backend(LaneId),
    /// In the proxy: `BEGIN`, `SET` and friends.
    Local(LocalIntent),
    /// A console command's result, computed at `Bind`.
    Console(QueryResult),
    /// Across regions, through the simple protocol's machinery.
    Spread(Arc<Spread>),
    /// Nothing to run.
    Empty,
}

/// A statement spanning regions, with its parameters written in.
#[derive(Debug)]
struct Spread {
    /// The statement, every parameter a literal of its own type.
    sql: String,
    regions: Vec<String>,
    write: bool,
    /// The format the client asked for each result column in.
    formats: Vec<Format>,
}

/// What a spread statement produced and has not yet sent, for an
/// `Execute` that asks for some rows at a time.
#[derive(Debug, Default)]
struct Answer {
    rows: VecDeque<Bytes>,
    tag: String,
    /// Whether an earlier `Execute` stopped at its row limit.
    suspended: bool,
}

/// A portal the client bound.
#[derive(Debug, Clone)]
struct Portal {
    statement: Arc<Statement>,
    destination: Destination,
}

/// Which backend connection a lane is.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LaneId {
    /// A participant of the open explicit transaction, by region.
    Participant(String),
    /// A connection borrowed for the current implicit transaction.
    Implicit {
        /// The region.
        region: String,
        /// Whether it is the region's primary.
        primary: bool,
    },
}

/// Where a lane stands relative to its last `Sync`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Backlog {
    /// Nothing sent since; the connection is between batches.
    Clean,
    /// Messages sent and answered, but not yet committed by a `Sync`.
    Unsynced,
    /// The backend reported an error, and is discarding everything up to
    /// the next `Sync`, at which it will roll back.
    Failed,
}

/// One backend connection the current batch uses.
#[derive(Debug)]
struct Lane {
    id: LaneId,
    /// The borrowed connection, for an implicit lane. A participant's
    /// connection belongs to the transaction.
    connection: Option<PooledConnection>,
    /// Whether anything queued on it writes.
    writes: bool,
    /// Where it stands relative to its last `Sync`.
    backlog: Backlog,
    /// The session's settings have been replayed onto it in this batch.
    prepared_settings: bool,
    /// What the backend's unnamed statement holds, so a statement bound
    /// many times in one batch is parsed once.
    unnamed: Option<StatementKey>,
    /// The named statements queued steps use since the last send, with
    /// what each holds: one prepared earlier in the same batch is not
    /// prepared twice, and if the server says it lost one they are
    /// forgotten.
    named: Vec<(String, StatementKey)>,
}

impl Lane {
    const fn new(id: LaneId, connection: Option<PooledConnection>) -> Self {
        Self {
            id,
            connection,
            writes: false,
            backlog: Backlog::Clean,
            prepared_settings: false,
            unnamed: None,
            named: Vec::new(),
        }
    }
}

/// Whether a lane's connection holds a named statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Held {
    /// It does; bind it as it is.
    Already,
    /// It does not, and will once the queued `Parse` runs.
    Now,
    /// It holds as many as it should; parse this one unnamed.
    Unnamed,
}

/// What to pass on from a step's answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Relay {
    /// Everything: it answers a message the client sent.
    Answer,
    /// Only errors and notices: the step is the proxy's own, such as the
    /// `Parse` that precedes a `Bind`, and its success is not news.
    Quiet,
}

/// One thing owed to the client, in the order its messages arrived.
#[derive(Debug)]
enum Output {
    /// Messages the proxy decided on itself.
    Ready(Vec<Bytes>),
    /// An error; nothing after it is sent.
    Fail(Vec<Bytes>),
    /// The answer to a queued step.
    Step {
        index: usize,
        relay: Relay,
        /// Remember the parameter types this `Describe` reports.
        learn: Option<StatementKey>,
        /// For an `Execute`, when it was asked for, to time it.
        started: Option<std::time::Instant>,
    },
}

/// An effect the proxy applied that a later failure must undo.
#[derive(Debug)]
enum Undo {
    Statement(String, Option<Arc<Statement>>),
    Portal(String, Option<Portal>),
    Settings(Vec<String>),
}

/// The extended-protocol state of one session.
#[derive(Debug, Default)]
pub(super) struct State {
    statements: HashMap<String, Arc<Statement>>,
    portals: HashMap<String, Portal>,
    lanes: Vec<Lane>,
    /// The lane the queued steps go to.
    pending_lane: Option<LaneId>,
    steps: Vec<Step>,
    /// What the client is owed, tagged with the message that owes it.
    outputs: Vec<(u64, Output)>,
    journal: Vec<(u64, Undo)>,
    /// Numbers messages, so a failure knows which effects came after it.
    sequence: u64,
    /// Set after an error, until the next `Sync`.
    skipping: bool,
    /// Whether anything in this batch failed.
    batch_failed: bool,
    /// Parameter types reported by `Describe`, for decoding binary keys.
    learned: HashMap<StatementKey, Vec<i32>>,
    /// Writes bound and not yet answered, which a move has to wait for.
    /// A portal can run in a later batch of the same transaction, so inside
    /// one they last until it ends.
    writes: Vec<crate::relocate::Ticket>,
    /// Answers to spread statements still being fetched, by portal.
    answers: HashMap<String, Answer>,
    /// The open transaction was begun by the proxy for this batch, to
    /// commit a write across regions atomically with the rest of it, and
    /// ends at the `Sync`.
    implicit: bool,
}

impl State {
    /// Whether a batch is under way, so that a simple `Query` has to end it.
    pub(super) fn active(&self) -> bool {
        !self.outputs.is_empty()
            || self.skipping
            || self.lanes.iter().any(|lane| {
                lane.backlog != Backlog::Clean || matches!(lane.id, LaneId::Implicit { .. })
            })
    }

    fn lane_index(&self, id: &LaneId) -> Option<usize> {
        self.lanes.iter().position(|lane| &lane.id == id)
    }

    fn lane_mut(&mut self, id: &LaneId) -> Option<&mut Lane> {
        self.lanes.iter_mut().find(|lane| &lane.id == id)
    }

    /// Clears what only lasts until a transaction ends.
    pub(super) fn end_transaction(&mut self) {
        self.portals.clear();
        self.writes.clear();
        self.answers.clear();
        self.implicit = false;
        self.lanes
            .retain(|lane| !matches!(lane.id, LaneId::Participant(_)));
    }
}

/// The message a step's relay keeps.
fn passes(relay: Relay, message: &RawMessage) -> bool {
    relay == Relay::Answer || message.is(tag::EXECUTE_OR_ERROR) || message.is(tag::NOTICE_RESPONSE)
}

impl Session {
    // ------------------------------------------------------------ dispatch

    fn next_sequence(&mut self) -> u64 {
        self.extended.sequence = self.extended.sequence.wrapping_add(1);
        self.extended.sequence
    }

    /// Owes the client a message decided on now.
    fn owe(&mut self, sequence: u64, messages: Vec<Bytes>) {
        self.extended
            .outputs
            .push((sequence, Output::Ready(messages)));
    }

    /// Refuses the current message and discards the rest of the batch.
    fn refuse(&mut self, sequence: u64, refusal: &Refusal) -> Result<(), SessionError> {
        self.shared.metrics.error(&refusal.sqlstate);
        let mut builder = build::ErrorBuilder::error(&refusal.sqlstate, refusal.message.clone());
        if let Some(hint) = &refusal.hint {
            builder = builder.hint(hint.clone());
        }
        self.extended
            .outputs
            .push((sequence, Output::Fail(vec![builder.build()?])));
        self.extended.skipping = true;
        Ok(())
    }

    fn journal(&mut self, sequence: u64, undo: Undo) {
        self.extended.journal.push((sequence, undo));
    }

    /// Handles `Parse`.
    pub(super) fn on_parse(&mut self, parse: &Parse<'_>) -> Result<(), SessionError> {
        let sequence = self.next_sequence();
        if self.extended.skipping {
            return Ok(());
        }
        if !parse.name.is_empty() && self.extended.statements.contains_key(parse.name) {
            return self.refuse(
                sequence,
                &plain(
                    "42P05",
                    format!("prepared statement \"{}\" already exists", parse.name),
                ),
            );
        }

        let body = if parse.sql.trim().is_empty() {
            Body::Empty
        } else if crate::console::handle(&self.shared, parse.sql).is_some() {
            Body::Console
        } else if crate::relocate::parse(parse.sql).is_some()
            || crate::relocate::parse_rebalance(parse.sql).is_some()
        {
            return self.refuse(
                sequence,
                &plain(
                    "0A000",
                    "RELOCATE and REBALANCE run only as simple queries, as psql sends them",
                ),
            );
        } else if is_copy(parse.sql) {
            // A COPY switches the connection into a sub-protocol of its own,
            // which this relay does not speak; refusing here is better than
            // a backend left waiting for data that never comes.
            return self.refuse(
                sequence,
                &plain(
                    "0A000",
                    "COPY is not supported over the extended query protocol",
                ),
            );
        } else {
            match self.shared.cache.analyze(parse.sql) {
                Ok(prepared) if prepared.analyses.len() > 1 => {
                    return self.refuse(
                        sequence,
                        &plain(
                            "42601",
                            "cannot insert multiple commands into a prepared statement",
                        ),
                    );
                }
                Ok(prepared)
                    if prepared
                        .analyses
                        .first()
                        .is_some_and(|analysis| self.blocked_by_failed_transaction(analysis)) =>
                {
                    return self.refuse(sequence, &aborted());
                }
                Ok(prepared) => Body::Sql(prepared),
                Err(error) => return self.refuse(sequence, &plain("42601", error.to_string())),
            }
        };

        let backend_name = (!parse.name.is_empty() && matches!(body, Body::Sql(_)))
            .then(|| backend_name(parse.sql, &parse.parameter_types));
        let statement = Arc::new(Statement {
            key: (parse.sql.to_owned(), parse.parameter_types.clone()),
            body,
            backend_name,
            parsed_at: std::time::Instant::now(),
        });
        let previous = self
            .extended
            .statements
            .insert(parse.name.to_owned(), statement);
        self.journal(sequence, Undo::Statement(parse.name.to_owned(), previous));
        self.owe(sequence, vec![build::parse_complete()?]);
        Ok(())
    }

    /// Handles `Bind`: the moment the parameters, and so the region, are
    /// known.
    pub(super) async fn on_bind(&mut self, bind: &Bind<'_>) -> Result<(), SessionError> {
        let sequence = self.next_sequence();
        if self.extended.skipping {
            return Ok(());
        }
        let Some(statement) = self.extended.statements.get(bind.statement).cloned() else {
            return self.refuse(
                sequence,
                &plain(
                    "26000",
                    format!("prepared statement \"{}\" does not exist", bind.statement),
                ),
            );
        };

        let destination = match self.destination(&statement, bind).await? {
            Ok(destination) => destination,
            Err(refusal) => return self.refuse(sequence, &refusal),
        };

        if let Destination::Backend(lane) = &destination {
            let name = self.queue_parse(sequence, lane, &statement).await?;
            self.queue(
                sequence,
                lane,
                Step::new(StepKind::Bind, bind.encode_from(&name)?),
                Relay::Answer,
                None,
            )
            .await?;
            // Switching lanes sent earlier work, and it failed: this Bind is
            // part of the discarded rest of the batch.
            if self.extended.skipping {
                return Ok(());
            }
        } else {
            self.owe(sequence, vec![build::bind_complete()?]);
        }

        let previous = self.extended.portals.insert(
            bind.portal.to_owned(),
            Portal {
                statement,
                destination,
            },
        );
        self.journal(sequence, Undo::Portal(bind.portal.to_owned(), previous));
        Ok(())
    }

    /// Plans a bound statement.
    async fn destination(
        &mut self,
        statement: &Statement,
        bind: &Bind<'_>,
    ) -> Result<Result<Destination, Refusal>, SessionError> {
        let prepared = match &statement.body {
            Body::Empty => return Ok(Ok(Destination::Empty)),
            Body::Console => {
                return Ok(Ok(crate::console::handle(&self.shared, statement.sql())
                    .map_or(Destination::Empty, Destination::Console)));
            }
            Body::Sql(prepared) => prepared,
        };
        let Some(analysis) = prepared.analyses.first() else {
            return Ok(Ok(Destination::Empty));
        };
        if self.blocked_by_failed_transaction(analysis) {
            return Ok(Err(aborted()));
        }

        let values = self.bound_values(statement, bind);
        let parameters = ParameterValues::from_prepared(prepared, &values);
        let (plan, write) = self.route(analysis, &parameters).await;
        self.extended.writes.extend(write);
        if let (Plan::Single { region, access }, true) = (&plan, self.transaction.is_some()) {
            if access.needs_primary() {
                self.txn_wrote.lock().insert(region.clone());
            }
        }
        let (region, access) = match plan {
            Plan::Single { region, access } => (region, access),
            // SHOW is answered by a backend like any read.
            Plan::Local(LocalIntent::Show) => (
                self.shared.router.topology().home_region.clone(),
                Access::ReadOnly,
            ),
            Plan::Local(intent) => return Ok(Ok(Destination::Local(intent))),
            Plan::Refuse(refusal) => return Ok(Err(refusal)),
            Plan::ScatterGather { regions, .. } => {
                return self.spread(statement, bind, regions, false).await;
            }
            Plan::Distributed { regions } => {
                return self.spread(statement, bind, regions, true).await;
            }
            // Parameters were supplied, so the planner has what it asked for.
            Plan::NeedsParameters { .. } => {
                return Ok(Err(plain(
                    "XX000",
                    "the statement could not be routed with the bound parameters",
                )));
            }
        };
        Ok(self
            .lane_for(&region, access)
            .await?
            .map(Destination::Backend))
    }

    /// The bound parameters as text, where they can be read as text.
    fn bound_values(&self, statement: &Statement, bind: &Bind<'_>) -> Vec<Option<String>> {
        let learned = self.extended.learned.get(&statement.key);
        (0..bind.parameters.len())
            .map(|index| match bind.parameter_format(index) {
                Format::Text => bind.parameter_as_text(index).map(ToOwned::to_owned),
                Format::Binary => {
                    // A declared type wins; `0` means the client left it to
                    // the server, whose answer a `Describe` may have taught us.
                    let declared = statement.declared().get(index).copied().unwrap_or(0);
                    let oid = if declared == 0 {
                        learned
                            .and_then(|types| types.get(index))
                            .copied()
                            .unwrap_or(0)
                    } else {
                        declared
                    };
                    let bytes = bind.parameters.get(index).copied().flatten()?;
                    luxbyte_proto::types::text_from_binary(oid, bytes)
                }
            })
            .collect()
    }

    /// Handles `Describe`.
    pub(super) async fn on_describe(
        &mut self,
        target: Target,
        name: &str,
    ) -> Result<(), SessionError> {
        let sequence = self.next_sequence();
        if self.extended.skipping {
            return Ok(());
        }
        match target {
            Target::Statement => self.describe_statement(sequence, name).await,
            Target::Portal => {
                let Some(portal) = self.extended.portals.get(name).cloned() else {
                    return self.refuse(
                        sequence,
                        &plain("34000", format!("portal \"{name}\" does not exist")),
                    );
                };
                match &portal.destination {
                    Destination::Backend(lane) => {
                        let step = Step::new(
                            StepKind::DescribePortal,
                            frontend::describe(Target::Portal, name)?,
                        );
                        self.queue(sequence, lane, step, Relay::Answer, None).await
                    }
                    Destination::Console(result) => {
                        let reply = describe_result(result)?;
                        self.owe(sequence, vec![reply]);
                        Ok(())
                    }
                    Destination::Spread(spread) => self.describe_spread(sequence, spread).await,
                    Destination::Local(_) | Destination::Empty => {
                        self.owe(sequence, vec![build::no_data()?]);
                        Ok(())
                    }
                }
            }
        }
    }

    async fn describe_statement(&mut self, sequence: u64, name: &str) -> Result<(), SessionError> {
        let Some(statement) = self.extended.statements.get(name).cloned() else {
            return self.refuse(
                sequence,
                &plain(
                    "26000",
                    format!("prepared statement \"{name}\" does not exist"),
                ),
            );
        };
        let prepared = match &statement.body {
            Body::Empty => {
                let messages = vec![
                    build::parameter_description(statement.declared())?,
                    build::no_data()?,
                ];
                self.owe(sequence, messages);
                return Ok(());
            }
            Body::Console => {
                let result =
                    crate::console::handle(&self.shared, statement.sql()).unwrap_or_default();
                let messages = vec![
                    build::parameter_description(&[])?,
                    describe_result(&result)?,
                ];
                self.owe(sequence, messages);
                return Ok(());
            }
            Body::Sql(prepared) => prepared,
        };

        // The answer is the same in every region, since schema changes reach
        // them all, so the cheapest connection to ask is the right one.
        let lane = if let Some(lane) = self.extended.pending_lane.clone() {
            lane
        } else {
            let region = match prepared
                .analyses
                .first()
                .map(|analysis| self.shared.router.plan(analysis, None))
            {
                Some(Plan::Single { region, .. }) => region,
                _ => self.shared.router.topology().home_region.clone(),
            };
            let participant = LaneId::Participant(region.clone());
            if self.extended.lane_index(&participant).is_none() {
                return self.describe_aside(sequence, &statement, &region).await;
            }
            participant
        };
        let name = self.queue_parse(sequence, &lane, &statement).await?;
        let step = Step::new(
            StepKind::DescribeStatement,
            frontend::describe(Target::Statement, &name)?,
        );
        self.queue(
            sequence,
            &lane,
            step,
            Relay::Answer,
            Some(statement.key.clone()),
        )
        .await
    }

    /// Describes a statement on a connection borrowed just for that, so a
    /// `Describe` on its own does not tie the batch to a region.
    async fn describe_aside(
        &mut self,
        sequence: u64,
        statement: &Statement,
        region: &str,
    ) -> Result<(), SessionError> {
        let steps = [
            Step::new(
                StepKind::Parse,
                frontend::parse("", statement.sql(), statement.declared())?,
            ),
            Step::new(
                StepKind::DescribeStatement,
                frontend::describe(Target::Statement, "")?,
            ),
            Step::new(StepKind::Sync, frontend::sync()?),
        ];
        let replies = match self.borrow(region, Access::ReadOnly).await {
            Ok(mut connection) => connection.pipeline(&steps).await,
            Err(error) => Err(error),
        };
        let replies = match replies {
            Ok(replies) => replies,
            Err(error) => return self.refuse(sequence, &to_refusal(&error)),
        };

        let mut messages = Vec::new();
        for (reply, relay) in replies.iter().zip([Relay::Quiet, Relay::Answer]) {
            for message in reply.messages.iter().filter(|m| passes(relay, m)) {
                messages.push(message.to_bytes()?);
            }
            if reply.failed {
                self.extended
                    .outputs
                    .push((sequence, Output::Fail(messages)));
                self.extended.skipping = true;
                return Ok(());
            }
        }
        if let Some(reply) = replies.get(1) {
            self.learn(&statement.key, reply);
        }
        self.owe(sequence, messages);
        Ok(())
    }

    /// Remembers the parameter types a `Describe` reported.
    fn learn(&mut self, key: &StatementKey, reply: &StepReply) {
        for message in &reply.messages {
            if let Ok(Backend::ParameterDescription { types }) = Backend::decode(message) {
                if self.extended.learned.len() >= LEARNED_TYPES_LIMIT {
                    self.extended.learned.clear();
                }
                self.extended.learned.insert(key.clone(), types);
            }
        }
    }

    /// Handles `Execute`.
    pub(super) async fn on_execute(
        &mut self,
        portal: &str,
        max_rows: i32,
    ) -> Result<(), SessionError> {
        let sequence = self.next_sequence();
        if self.extended.skipping {
            return Ok(());
        }
        let Some(bound) = self.extended.portals.get(portal).cloned() else {
            return self.refuse(
                sequence,
                &plain("34000", format!("portal \"{portal}\" does not exist")),
            );
        };
        match bound.destination {
            Destination::Backend(lane) => {
                let region = match &lane {
                    LaneId::Participant(region) | LaneId::Implicit { region, .. } => region,
                };
                let remote = *region != self.shared.router.topology().home_region;
                self.shared.metrics.sent(region, remote);
                let step = Step::new(StepKind::Execute, frontend::execute(portal, max_rows)?);
                self.queue(sequence, &lane, step, Relay::Answer, None)
                    .await?;
                // Timed from now until its answer is passed on.
                if let Some((_, Output::Step { started, .. })) = self.extended.outputs.last_mut() {
                    *started = Some(std::time::Instant::now());
                }
                Ok(())
            }
            Destination::Empty => {
                self.owe(sequence, vec![build::empty_query_response()?]);
                Ok(())
            }
            Destination::Spread(spread) => {
                self.execute_spread(sequence, portal, &spread, max_rows)
                    .await
            }
            Destination::Console(result) => {
                let mut messages = Vec::with_capacity(result.rows.len().saturating_add(1));
                for row in &result.rows {
                    let values: Vec<Option<&str>> = row.iter().map(Option::as_deref).collect();
                    messages.push(build::data_row(&values)?);
                }
                messages.push(build::command_complete(&format!(
                    "SELECT {}",
                    result.rows.len()
                ))?);
                self.owe(sequence, messages);
                Ok(())
            }
            Destination::Local(LocalIntent::SessionState) => {
                let before = self.settings.clone();
                self.record_setting(bound.statement.sql());
                self.journal(sequence, Undo::Settings(before));
                self.owe(sequence, vec![build::command_complete("SET")?]);
                Ok(())
            }
            Destination::Local(LocalIntent::TransactionControl(op)) => {
                self.execute_transaction_control(sequence, op).await
            }
            // Planned onto a backend at `Bind`.
            Destination::Local(LocalIntent::Show) => {
                self.owe(sequence, vec![build::no_data()?]);
                Ok(())
            }
        }
    }

    /// Runs `BEGIN`, `COMMIT` or `ROLLBACK` sent as a prepared statement,
    /// which is how JDBC sends them.
    async fn execute_transaction_control(
        &mut self,
        sequence: u64,
        op: TransactionOp,
    ) -> Result<(), SessionError> {
        // A transaction boundary cannot be undone, so it waits until every
        // message before it has been answered.
        self.flush_pending().await?;
        if self.extended.skipping {
            return Ok(());
        }

        if op == TransactionOp::Begin && self.extended.implicit {
            return self.refuse(
                sequence,
                &Refusal {
                    hint: Some("send a Sync before BEGIN".to_owned()),
                    ..plain(
                        "0A000",
                        "BEGIN after a write across regions in the same extended-query batch \
                         is not supported",
                    )
                },
            );
        }
        if op == TransactionOp::Begin
            && self.transaction.is_none()
            && self
                .extended
                .lanes
                .iter()
                .any(|lane| matches!(lane.id, LaneId::Implicit { .. }))
        {
            // PostgreSQL would fold the statements already run in this
            // batch into the new transaction. They ran on connections the
            // coordinator does not own, so it cannot.
            return self.refuse(
                sequence,
                &Refusal {
                    sqlstate: "0A000".to_owned(),
                    message: "BEGIN after other statements in the same extended-query batch \
                              is not supported"
                        .to_owned(),
                    hint: Some("send a Sync before BEGIN".to_owned()),
                    retryable: false,
                },
            );
        }

        let ending = matches!(op, TransactionOp::Commit | TransactionOp::Rollback)
            && self.transaction.is_some();
        if ending {
            // The participants have to be out of their extended-query batch
            // before the coordinator speaks the simple protocol to them.
            self.sync_participants().await?;
        }
        match self.transaction_control(op).await? {
            Ok(()) => Ok(()),
            Err(refusal) => {
                self.refuse(sequence, &refusal)?;
                self.flush_pending().await
            }
        }
    }

    /// Handles `Close`.
    pub(super) async fn on_close(
        &mut self,
        target: Target,
        name: &str,
    ) -> Result<(), SessionError> {
        let sequence = self.next_sequence();
        if self.extended.skipping {
            return Ok(());
        }
        match target {
            Target::Statement => {
                let previous = self.extended.statements.remove(name);
                self.journal(sequence, Undo::Statement(name.to_owned(), previous));
            }
            Target::Portal => {
                self.extended.answers.remove(name);
                let previous = self.extended.portals.remove(name);
                let lane = previous
                    .as_ref()
                    .and_then(|portal| match &portal.destination {
                        Destination::Backend(lane) => Some(lane.clone()),
                        _ => None,
                    });
                self.journal(sequence, Undo::Portal(name.to_owned(), previous));
                // The backend's portal has to go too, or binding the name
                // again fails with "portal already exists".
                if let Some(lane) = lane {
                    let step = Step::new(StepKind::Close, frontend::close(Target::Portal, name)?);
                    return self.queue(sequence, &lane, step, Relay::Answer, None).await;
                }
            }
        }
        // Closing something that does not exist is not an error.
        self.owe(sequence, vec![build::close_complete()?]);
        Ok(())
    }

    /// Handles `Flush`: answer everything so far, without ending the batch.
    pub(super) async fn on_flush(&mut self) -> Result<(), SessionError> {
        self.flush_pending().await?;
        self.client.flush().await?;
        Ok(())
    }

    /// Handles `Sync`: the end of the batch, and of its implicit transaction.
    pub(super) async fn on_sync(&mut self) -> Result<(), SessionError> {
        self.end_batch().await?;
        self.ready().await
    }

    /// Ends the batch: runs what is queued, then commits or abandons each
    /// lane, as the batch's outcome requires.
    pub(super) async fn end_batch(&mut self) -> Result<(), SessionError> {
        let refusal_queued = self
            .extended
            .outputs
            .iter()
            .any(|(_, output)| matches!(output, Output::Fail(_)));
        // The common case — nothing refused — sends the `Sync` with the
        // queued steps, so the batch costs one round trip. A refusal waiting
        // in the queue means the lane must not get a `Sync` at all.
        if !refusal_queued && !self.extended.skipping {
            if let Some(lane) = self.extended.pending_lane.clone() {
                self.extended
                    .steps
                    .push(Step::new(StepKind::Sync, frontend::sync()?));
                let index = self.extended.steps.len().saturating_sub(1);
                let sequence = self.next_sequence();
                self.extended.outputs.push((
                    sequence,
                    Output::Step {
                        index,
                        relay: Relay::Quiet,
                        learn: None,
                        started: None,
                    },
                ));
                self.flush_to(&lane, true).await?;
            }
        }
        self.flush_pending().await?;

        let failed = self.extended.batch_failed;
        let lanes = std::mem::take(&mut self.extended.lanes);
        // Outside a transaction a lane's writes commit at its `Sync`.
        let committing: Vec<String> = lanes
            .iter()
            .filter_map(|lane| match &lane.id {
                LaneId::Implicit { region, .. } if lane.writes => Some(region.clone()),
                _ => None,
            })
            .collect();
        for mut lane in lanes {
            let participant = matches!(lane.id, LaneId::Participant(_));
            if lane.backlog != Backlog::Clean {
                // A backend that saw the error rolls back at its `Sync`. One
                // that did not would commit there, so outside a transaction it
                // is hung up on instead.
                if failed && lane.backlog != Backlog::Failed && !participant {
                    if let Some(connection) = lane.connection.as_mut() {
                        connection.poison();
                    }
                } else {
                    let replies = match self.lane_connection(&mut lane) {
                        Some(connection) => connection
                            .pipeline(&[Step::new(StepKind::Sync, frontend::sync()?)])
                            .await
                            .map_err(|error| to_refusal(&error)),
                        None => Err(plain("08000", "the connection was lost")),
                    };
                    let errors = match replies {
                        Ok(replies) => replies
                            .into_iter()
                            .flat_map(|reply| reply.messages)
                            .filter(|message| message.is(tag::EXECUTE_OR_ERROR))
                            .map(|message| message.to_bytes())
                            .collect::<Result<Vec<_>, _>>()?,
                        Err(refusal) => vec![error_message(&refusal)?],
                    };
                    // Only the first error of a batch reaches the client, as
                    // it would from PostgreSQL.
                    if !failed && !errors.is_empty() {
                        for error in errors {
                            self.client.queue_raw(&error);
                        }
                        self.extended.batch_failed = true;
                        if self.transaction.is_some() {
                            self.status = TransactionStatus::Failed;
                        }
                    }
                }
            }
            if participant && self.transaction.is_some() {
                lane.backlog = Backlog::Clean;
                lane.prepared_settings = false;
                lane.unnamed = None;
                self.extended.lanes.push(lane);
            }
            // An implicit lane is dropped here, which returns its connection
            // to the pool if it is clean and retires it if not.
        }

        if !self.extended.batch_failed {
            self.wrote_in(committing.iter().map(String::as_str));
        }
        if self.extended.implicit {
            self.end_implicit().await?;
        }
        if self.transaction.is_none() {
            self.extended.portals.clear();
            self.extended.writes.clear();
            self.extended.answers.clear();
            self.status = TransactionStatus::Idle;
        }
        self.extended.skipping = false;
        self.extended.batch_failed = false;
        self.extended.journal.clear();
        Ok(())
    }

    /// Sends `Sync` to every participant with messages outstanding.
    async fn sync_participants(&mut self) -> Result<(), SessionError> {
        let mut lanes = std::mem::take(&mut self.extended.lanes);
        for lane in &mut lanes {
            if !matches!(lane.id, LaneId::Participant(_)) || lane.backlog == Backlog::Clean {
                continue;
            }
            if let Some(connection) = self.lane_connection(lane) {
                // An error here surfaces again at the commit, which is where
                // the client expects to hear about it.
                let _ = connection
                    .pipeline(&[Step::new(StepKind::Sync, frontend::sync()?)])
                    .await;
            }
            lane.backlog = Backlog::Clean;
            lane.unnamed = None;
        }
        self.extended.lanes = lanes;
        Ok(())
    }

    // ------------------------------------------------------ across regions

    /// Plans a statement spanning regions: writes its parameters in, and
    /// opens a transaction for the batch if a write needs one.
    async fn spread(
        &mut self,
        statement: &Statement,
        bind: &Bind<'_>,
        regions: Vec<String>,
        write: bool,
    ) -> Result<Result<Destination, Refusal>, SessionError> {
        if self.transaction.is_none() {
            if self.extended.lanes.iter().any(|lane| lane.writes) {
                let what = if write {
                    "write to more regions"
                } else {
                    "read across regions, which would not see what it wrote"
                };
                return Ok(Err(Refusal {
                    hint: Some(
                        "send a Sync first, or wrap the batch in BEGIN and COMMIT".to_owned(),
                    ),
                    ..plain(
                        "0A000",
                        format!(
                            "this extended-query batch has written outside a transaction, and \
                             cannot then {what}"
                        ),
                    )
                }));
            }
            if write {
                // Everything the batch does from here commits together, in
                // two phases, at the `Sync`.
                let mut transaction = self.shared.coordinator.begin();
                transaction.cancel_with(&self.cancel);
                self.transaction = Some(transaction);
                self.extended.implicit = true;
            }
        }
        let sql = match self.literal_sql(statement, bind).await? {
            Ok(sql) => sql,
            Err(refusal) => return Ok(Err(refusal)),
        };
        Ok(Ok(Destination::Spread(Arc::new(Spread {
            sql,
            regions,
            write,
            formats: bind.result_formats.clone(),
        }))))
    }

    /// The statement's text with each parameter replaced by a literal of
    /// its own type, rendered by PostgreSQL in the home region.
    async fn literal_sql(
        &mut self,
        statement: &Statement,
        bind: &Bind<'_>,
    ) -> Result<Result<String, Refusal>, SessionError> {
        let count = bind.parameters.len();
        if count == 0 {
            return Ok(Ok(statement.sql().to_owned()));
        }
        let home = self.shared.router.topology().home_region.clone();
        let mut connection = match self.borrow(&home, Access::ReadOnly).await {
            Ok(connection) => connection,
            Err(error) => return Ok(Err(to_refusal(&error))),
        };

        let types = match self
            .parameter_types(&mut connection, statement, count)
            .await?
        {
            Ok(types) => types,
            Err(refusal) => return Ok(Err(refusal)),
        };

        let columns: Vec<String> = (1..=count)
            .map(|n| format!("${n}::text, pg_catalog.pg_typeof(${n})::text"))
            .collect();
        let steps = [
            Step::new(
                StepKind::Parse,
                frontend::parse("", &format!("SELECT {}", columns.join(", ")), &types)?,
            ),
            Step::new(
                StepKind::Bind,
                frontend::bind("", "", &bind.parameter_formats, &bind.parameters, &[])?,
            ),
            Step::new(StepKind::Execute, frontend::execute("", 0)?),
            Step::new(StepKind::Sync, frontend::sync()?),
        ];
        let replies = match connection.pipeline(&steps).await {
            Ok(replies) => replies,
            Err(error) => return Ok(Err(to_refusal(&error))),
        };
        if let Some(refusal) = failure(&replies) {
            return Ok(Err(refusal));
        }
        let rendered: Vec<Option<String>> = replies
            .get(2)
            .into_iter()
            .flat_map(|reply| &reply.messages)
            .find_map(|message| match Backend::decode(message) {
                Ok(Backend::DataRow { columns }) => Some(
                    columns
                        .into_iter()
                        .map(|value| value.map(|bytes| String::from_utf8_lossy(bytes).into_owned()))
                        .collect(),
                ),
                _ => None,
            })
            .unwrap_or_default();
        let literals: Vec<String> = rendered
            .chunks(2)
            .map(|pair| match pair {
                [value, Some(type_name)] => match value {
                    Some(value) => format!("({}::{type_name})", escape_literal(value)),
                    None => format!("(NULL::{type_name})"),
                },
                _ => "NULL".to_owned(),
            })
            .collect();
        if literals.len() != count {
            return Ok(Err(plain(
                "XX000",
                "the parameters could not be rendered as literals",
            )));
        }
        Ok(luxbyte_sql::statements::substitute(statement.sql(), &literals).map_err(|number| {
            plain(
                "08P01",
                format!("the statement refers to ${number}, but the Bind supplied {count} parameters"),
            )
        }))
    }

    /// The types a statement's parameters have: declared, learned from a
    /// `Describe`, or else asked of `connection` now.
    async fn parameter_types(
        &mut self,
        connection: &mut PooledConnection,
        statement: &Statement,
        count: usize,
    ) -> Result<Result<Vec<i32>, Refusal>, SessionError> {
        let mut types: Vec<i32> = (0..count)
            .map(|index| statement.declared().get(index).copied().unwrap_or(0))
            .collect();
        if types.contains(&0) {
            if !self.extended.learned.contains_key(&statement.key) {
                let steps = [
                    Step::new(
                        StepKind::Parse,
                        frontend::parse("", statement.sql(), statement.declared())?,
                    ),
                    Step::new(
                        StepKind::DescribeStatement,
                        frontend::describe(Target::Statement, "")?,
                    ),
                    Step::new(StepKind::Sync, frontend::sync()?),
                ];
                let replies = match connection.pipeline(&steps).await {
                    Ok(replies) => replies,
                    Err(error) => return Ok(Err(to_refusal(&error))),
                };
                if let Some(refusal) = failure(&replies) {
                    return Ok(Err(refusal));
                }
                if let Some(reply) = replies.get(1) {
                    self.learn(&statement.key, reply);
                }
            }
            let known = self
                .extended
                .learned
                .get(&statement.key)
                .cloned()
                .unwrap_or_default();
            for (slot, learned) in types.iter_mut().zip(known) {
                if *slot == 0 {
                    *slot = learned;
                }
            }
        }
        // Rendered as text, a parameter of unknown type could be read back
        // as something else entirely; PostgreSQL knows every parameter a
        // statement has, so one it does not know is one too many.
        if types.contains(&0) {
            return Ok(Err(plain(
                "08P01",
                format!("the Bind supplied {count} parameters, more than the statement has"),
            )));
        }
        Ok(Ok(types))
    }

    /// Describes a spread portal by binding its statement in the home
    /// region, which knows the columns and their formats as well as any.
    async fn describe_spread(
        &mut self,
        sequence: u64,
        spread: &Spread,
    ) -> Result<(), SessionError> {
        let home = self.shared.router.topology().home_region.clone();
        let steps = [
            Step::new(StepKind::Parse, frontend::parse("", &spread.sql, &[])?),
            Step::new(
                StepKind::Bind,
                frontend::bind("", "", &[], &[], &spread.formats)?,
            ),
            Step::new(
                StepKind::DescribePortal,
                frontend::describe(Target::Portal, "")?,
            ),
            Step::new(StepKind::Sync, frontend::sync()?),
        ];
        let replies = match self.borrow(&home, Access::ReadOnly).await {
            Ok(mut connection) => connection.pipeline(&steps).await,
            Err(error) => Err(error),
        };
        let replies = match replies {
            Ok(replies) => replies,
            Err(error) => return self.refuse(sequence, &to_refusal(&error)),
        };
        if let Some(refusal) = failure(&replies) {
            return self.refuse(sequence, &refusal);
        }
        let messages = replies
            .get(2)
            .map(|reply| {
                reply
                    .messages
                    .iter()
                    .map(RawMessage::to_bytes)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .unwrap_or_default();
        self.owe(sequence, messages);
        Ok(())
    }

    /// Runs a spread portal, or sends more of what it already produced.
    async fn execute_spread(
        &mut self,
        sequence: u64,
        portal: &str,
        spread: &Spread,
        max_rows: i32,
    ) -> Result<(), SessionError> {
        if !self.extended.answers.contains_key(portal) {
            // Everything before it has to have run, and been answered, first.
            self.flush_pending().await?;
            if self.extended.skipping {
                return Ok(());
            }
            if self.transaction.is_some() {
                // The participants speak the simple protocol from here.
                self.sync_participants().await?;
            }
            let analysis =
                self.extended
                    .portals
                    .get(portal)
                    .and_then(|bound| match &bound.statement.body {
                        Body::Sql(prepared) => prepared.analyses.first().cloned(),
                        _ => None,
                    });
            let Some(analysis) = analysis else {
                return self.refuse(sequence, &plain("XX000", "the portal lost its statement"));
            };
            if spread.write {
                self.txn_wrote.lock().extend(spread.regions.iter().cloned());
            }
            let started = std::time::Instant::now();
            let outcome = if spread.write {
                self.distributed_result(&analysis, &spread.sql, &spread.regions)
                    .await?
            } else {
                self.scatter_result(&spread.regions, &spread.sql).await?
            };
            self.shared.metrics.statement(
                if spread.write {
                    "distributed"
                } else {
                    "scatter"
                },
                started.elapsed(),
            );
            let result = match outcome {
                Ok(result) => result,
                Err(refusal) => {
                    self.refuse(sequence, &refusal)?;
                    return self.flush_pending().await;
                }
            };
            let rows = match self.encode_rows(&result, &spread.formats).await? {
                Ok(rows) => rows,
                Err(refusal) => {
                    self.refuse(sequence, &refusal)?;
                    return self.flush_pending().await;
                }
            };
            let tag = result
                .tags
                .last()
                .cloned()
                .unwrap_or_else(|| format!("SELECT {}", result.rows.len()));
            self.extended.answers.insert(
                portal.to_owned(),
                Answer {
                    rows: rows.into(),
                    tag,
                    suspended: false,
                },
            );
        }
        self.send_answer(sequence, portal, max_rows)
    }

    /// Sends up to `max_rows` rows of a spread portal's answer, as
    /// PostgreSQL would from a portal: `PortalSuspended` when the limit is
    /// reached, `CommandComplete` when the rows run out.
    fn send_answer(
        &mut self,
        sequence: u64,
        portal: &str,
        max_rows: i32,
    ) -> Result<(), SessionError> {
        let Some(answer) = self.extended.answers.get_mut(portal) else {
            return Ok(());
        };
        let limit = usize::try_from(max_rows).ok().filter(|limit| *limit > 0);
        let take = limit.unwrap_or(answer.rows.len()).min(answer.rows.len());
        let mut messages: Vec<Bytes> = answer.rows.drain(..take).collect();
        if limit.is_some_and(|limit| take == limit) {
            answer.suspended = true;
            messages.push(build::portal_suspended()?);
        } else {
            // A portal fetched in parts reports the rows of its last part.
            let tag = if answer.suspended && answer.tag.starts_with("SELECT ") {
                format!("SELECT {take}")
            } else {
                answer.tag.clone()
            };
            messages.push(build::command_complete(&tag)?);
            self.extended.answers.remove(portal);
        }
        self.owe(sequence, messages);
        Ok(())
    }

    /// The rows as `DataRow` messages in the formats the client asked for.
    /// Text comes straight from the result; binary is encoded by the home
    /// region, from the same values carried there as typed arrays.
    async fn encode_rows(
        &mut self,
        result: &QueryResult,
        formats: &[Format],
    ) -> Result<Result<Vec<Bytes>, Refusal>, SessionError> {
        if result.columns.is_empty() || result.rows.is_empty() {
            return Ok(Ok(Vec::new()));
        }
        if formats.iter().all(|format| *format == Format::Text) {
            let mut rows = Vec::with_capacity(result.rows.len());
            for row in &result.rows {
                let values: Vec<Option<&str>> = row.iter().map(Option::as_deref).collect();
                rows.push(build::data_row(&values)?);
            }
            return Ok(Ok(rows));
        }

        let types = match super::type_names(&result.type_oids) {
            Ok(types) => types,
            Err(refusal) => return Ok(Err(refusal)),
        };
        let columns: Vec<(String, Vec<Option<String>>)> = types
            .into_iter()
            .enumerate()
            .map(|(index, type_name)| {
                let values = result
                    .rows
                    .iter()
                    .map(|row| row.get(index).cloned().flatten())
                    .collect();
                (type_name, values)
            })
            .collect();
        let selected: Vec<String> = (1..=columns.len()).map(|n| format!("p.c{n}")).collect();
        let sql = format!(
            "SELECT {} FROM {} ORDER BY p.n",
            selected.join(", "),
            luxbyte_sql::combine::ordered_source(&columns)
        );
        let home = self.shared.router.topology().home_region.clone();
        let steps = [
            Step::new(StepKind::Parse, frontend::parse("", &sql, &[])?),
            Step::new(StepKind::Bind, frontend::bind("", "", &[], &[], formats)?),
            Step::new(StepKind::Execute, frontend::execute("", 0)?),
            Step::new(StepKind::Sync, frontend::sync()?),
        ];
        let replies = match self.borrow(&home, Access::ReadOnly).await {
            Ok(mut connection) => connection.pipeline(&steps).await,
            Err(error) => Err(error),
        };
        let replies = match replies {
            Ok(replies) => replies,
            Err(error) => return Ok(Err(to_refusal(&error))),
        };
        if let Some(refusal) = failure(&replies) {
            return Ok(Err(refusal));
        }
        let rows = replies
            .get(2)
            .into_iter()
            .flat_map(|reply| &reply.messages)
            .filter(|message| message.is(tag::DESCRIBE_OR_DATA_ROW))
            .map(RawMessage::to_bytes)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Ok(rows))
    }

    /// Ends the transaction the proxy opened for a batch: committed if the
    /// batch succeeded, rolled back if anything in it failed.
    async fn end_implicit(&mut self) -> Result<(), SessionError> {
        let Some(transaction) = self.transaction.take() else {
            self.extended.implicit = false;
            return Ok(());
        };
        self.extended.end_transaction();
        if self.extended.batch_failed {
            self.rollback(transaction).await;
        } else if let Err(error) = self.commit(transaction).await {
            let refusal = txn_refusal(&error);
            self.shared.metrics.error(&refusal.sqlstate);
            self.client.queue_raw(&error_message(&refusal)?);
        }
        self.status = TransactionStatus::Idle;
        Ok(())
    }

    // --------------------------------------------------------------- lanes

    /// The lane a statement for `region` runs on, opening one if needed.
    async fn lane_for(
        &mut self,
        region: &str,
        access: Access,
    ) -> Result<Result<LaneId, Refusal>, SessionError> {
        if self.transaction.is_some() {
            // Inside a transaction every statement goes to the primary: a
            // participant opened on a replica would refuse the first write.
            let Some(pool) = self.pool_for(region, Access::ReadWrite, 0) else {
                return Ok(Err(unconfigured(region)));
            };
            if let Some(transaction) = self.transaction.as_mut() {
                if let Err(error) = transaction.enlist(region, &pool).await {
                    return Ok(Err(txn_refusal(&error)));
                }
            }
            let id = LaneId::Participant(region.to_owned());
            if self.extended.lane_index(&id).is_none() {
                self.extended.lanes.push(Lane::new(id.clone(), None));
            }
            return Ok(Ok(id));
        }

        let primary = access.needs_primary();
        let writer = LaneId::Implicit {
            region: region.to_owned(),
            primary: true,
        };
        // A read after a write in the same region goes where the write went,
        // or it would not see a row the batch has just written.
        if self.extended.lane_index(&writer).is_some() {
            if let Some(lane) = self.extended.lane_mut(&writer) {
                lane.writes |= primary;
            }
            return Ok(Ok(writer));
        }
        let id = LaneId::Implicit {
            region: region.to_owned(),
            primary,
        };
        if self.extended.lane_index(&id).is_some() {
            return Ok(Ok(id));
        }
        if primary && self.extended.lanes.iter().any(|lane| lane.writes) {
            return Ok(Err(Refusal {
                sqlstate: "0A000".to_owned(),
                message: "this extended-query batch writes to more than one region outside a \
                          transaction, and could not commit atomically"
                    .to_owned(),
                hint: Some(
                    "wrap the batch in BEGIN and COMMIT, so it commits across regions in two \
                     phases"
                        .to_owned(),
                ),
                retryable: false,
            }));
        }

        let connection = match self.borrow(region, access).await {
            Ok(connection) => connection,
            Err(error) => return Ok(Err(to_refusal(&error))),
        };
        let mut lane = Lane::new(id.clone(), Some(connection));
        lane.writes = primary;
        self.extended.lanes.push(lane);
        Ok(Ok(id))
    }

    /// The connection behind a lane.
    fn lane_connection<'a>(&'a mut self, lane: &'a mut Lane) -> Option<&'a mut PooledConnection> {
        match &lane.id {
            LaneId::Participant(region) => self.transaction.as_mut()?.connection(region).ok(),
            LaneId::Implicit { .. } => lane.connection.as_mut(),
        }
    }

    /// Queues the `Parse` a statement needs on a lane, unless the lane's
    /// connection already holds it, and returns the name the backend knows
    /// it by.
    async fn queue_parse(
        &mut self,
        sequence: u64,
        lane: &LaneId,
        statement: &Statement,
    ) -> Result<String, SessionError> {
        self.switch_to(lane).await?;
        if self.extended.skipping {
            return Ok(String::new());
        }
        self.queue_settings(sequence, lane)?;
        if let Some(name) = &statement.backend_name {
            match self.prepared_on(lane, name, statement) {
                Held::Already => return Ok(name.clone()),
                Held::Now => {
                    // The server may hold a statement by this name: one
                    // parsed before the client's, or one the connection
                    // forgot; closing a statement that does not exist is not
                    // an error, while preparing one that does is.
                    self.push_step(
                        sequence,
                        Step::new(StepKind::Close, frontend::close(Target::Statement, name)?),
                        Relay::Quiet,
                        None,
                    );
                    let step = Step::prepare(name, statement.sql(), statement.declared())?;
                    self.push_step(sequence, step, Relay::Quiet, None);
                    return Ok(name.clone());
                }
                Held::Unnamed => {}
            }
        }
        let parsed = self
            .extended
            .lane_mut(lane)
            .is_some_and(|state| state.unnamed.as_ref() == Some(&statement.key));
        if !parsed {
            let step = Step::new(
                StepKind::Parse,
                frontend::parse("", statement.sql(), statement.declared())?,
            );
            self.push_step(sequence, step, Relay::Quiet, None);
            if let Some(state) = self.extended.lane_mut(lane) {
                state.unnamed = Some(statement.key.clone());
            }
        }
        Ok(String::new())
    }

    /// Whether a lane's connection holds `statement` as `name`, or will once
    /// what is queued for it runs.
    fn prepared_on(&mut self, lane: &LaneId, name: &str, statement: &Statement) -> Held {
        let mut lanes = std::mem::take(&mut self.extended.lanes);
        let held =
            lanes
                .iter_mut()
                .find(|state| &state.id == lane)
                .map_or(Held::Unnamed, |state| {
                    if let Some((_, key)) = state.named.iter().find(|(queued, _)| queued == name) {
                        // Another statement hashing to the same name is not
                        // this one; it goes unnamed rather than wrong.
                        return if *key == statement.key {
                            Held::Already
                        } else {
                            Held::Unnamed
                        };
                    }
                    let held = match self.lane_connection(state) {
                        Some(connection)
                            if connection.holds_statement(
                                name,
                                statement.sql(),
                                statement.declared(),
                                statement.parsed_at,
                            ) =>
                        {
                            Held::Already
                        }
                        Some(connection) if connection.can_prepare(name) => Held::Now,
                        _ => Held::Unnamed,
                    };
                    if held != Held::Unnamed {
                        state.named.push((name.to_owned(), statement.key.clone()));
                    }
                    held
                });
        self.extended.lanes = lanes;
        held
    }

    /// Makes a lane's connection forget the named statements its last
    /// batch used.
    ///
    /// Called when the server said one of them does not exist, which it can
    /// only say if something dropped it unseen; preparing them again costs
    /// a parse each. Other errors leave them alone: a statement whose plan
    /// a schema change broke keeps failing until the client prepares it
    /// again, as it would on one server.
    fn forget_statements(&mut self, lane: &LaneId, names: &[(String, StatementKey)]) {
        if names.is_empty() {
            return;
        }
        let mut lanes = std::mem::take(&mut self.extended.lanes);
        if let Some(connection) = lanes
            .iter_mut()
            .find(|state| &state.id == lane)
            .and_then(|state| self.lane_connection(state))
        {
            for (name, _) in names {
                connection.forget_statement(name);
            }
        }
        self.extended.lanes = lanes;
    }

    /// Queues a step on a lane.
    async fn queue(
        &mut self,
        sequence: u64,
        lane: &LaneId,
        step: Step,
        relay: Relay,
        learn: Option<StatementKey>,
    ) -> Result<(), SessionError> {
        self.switch_to(lane).await?;
        if self.extended.skipping {
            return Ok(());
        }
        self.queue_settings(sequence, lane)?;
        self.push_step(sequence, step, relay, learn);
        Ok(())
    }

    fn push_step(&mut self, sequence: u64, step: Step, relay: Relay, learn: Option<StatementKey>) {
        self.extended.steps.push(step);
        let index = self.extended.steps.len().saturating_sub(1);
        self.extended.outputs.push((
            sequence,
            Output::Step {
                index,
                relay,
                learn,
                started: None,
            },
        ));
    }

    /// Makes `lane` the one queued steps go to, sending whatever was queued
    /// for another lane first.
    ///
    /// Steps for two connections cannot share a queue: the answers have to
    /// reach the client in the order it sent the messages, and an error on
    /// the first connection has to stop the second from running anything.
    async fn switch_to(&mut self, lane: &LaneId) -> Result<(), SessionError> {
        if self.extended.pending_lane.as_ref() == Some(lane) {
            return Ok(());
        }
        self.flush_pending().await?;
        self.extended.pending_lane = Some(lane.clone());
        Ok(())
    }

    /// Replays the session's settings onto a lane before its first statement
    /// in this batch.
    ///
    /// Sent as steps of the same batch rather than as a simple query: a
    /// failed `SET` then stops the statements after it, as it would on one
    /// server, and the replay costs no round trip of its own.
    fn queue_settings(&mut self, sequence: u64, lane: &LaneId) -> Result<(), SessionError> {
        let Some(state) = self.extended.lane_mut(lane) else {
            return Ok(());
        };
        if state.prepared_settings {
            return Ok(());
        }
        state.prepared_settings = true;
        if !self.settings.is_empty() {
            state.unnamed = None;
        }
        for setting in self.settings.clone() {
            for step in [
                Step::new(StepKind::Parse, frontend::parse("", &setting, &[])?),
                Step::new(
                    StepKind::Bind,
                    frontend::bind(SETTING_PORTAL, "", &[], &[], &[])?,
                ),
                Step::new(StepKind::Execute, frontend::execute(SETTING_PORTAL, 0)?),
                Step::new(
                    StepKind::Close,
                    frontend::close(Target::Portal, SETTING_PORTAL)?,
                ),
            ] {
                self.push_step(sequence, step, Relay::Quiet, None);
            }
        }
        Ok(())
    }

    /// Sends what is queued and passes the answers on.
    async fn flush_pending(&mut self) -> Result<(), SessionError> {
        match self.extended.pending_lane.clone() {
            Some(lane) => self.flush_to(&lane, false).await,
            // Nothing queued for a backend; only the proxy's own answers.
            None => self.deliver(&[], None).map(|_| ()),
        }
    }

    async fn flush_to(&mut self, id: &LaneId, synced: bool) -> Result<(), SessionError> {
        let steps = std::mem::take(&mut self.extended.steps);
        self.extended.pending_lane = None;
        let mut named = Vec::new();
        let replies = if steps.is_empty() {
            Ok(Vec::new())
        } else {
            let mut lanes = std::mem::take(&mut self.extended.lanes);
            let outcome = match lanes.iter_mut().find(|lane| &lane.id == id) {
                Some(lane) => {
                    named = std::mem::take(&mut lane.named);
                    lane.backlog = if synced {
                        Backlog::Clean
                    } else {
                        Backlog::Unsynced
                    };
                    match self.lane_connection(lane) {
                        Some(connection) => connection.pipeline(&steps).await,
                        None => Err(PoolError::Closed),
                    }
                }
                None => Err(PoolError::Closed),
            };
            self.extended.lanes = lanes;
            outcome
        };

        let replies = match replies {
            Ok(replies) => replies,
            Err(error) => {
                // The connection is gone, along with whatever it was doing,
                // so the answers owed for it become one error.
                if let Some(lane) = self.extended.lane_mut(id) {
                    lane.backlog = Backlog::Clean;
                }
                let refusal = to_refusal(&error);
                return self.deliver(&[], Some(&refusal)).map(|_| ());
            }
        };
        if let Some(index) = self.deliver(&replies, None)? {
            if replies
                .get(index)
                .and_then(sqlstate)
                .is_some_and(|code| code == "26000")
            {
                self.forget_statements(id, &named);
            }
            if let Some(lane) = self.extended.lane_mut(id) {
                // Once its `Sync` is answered the backend has rolled back and
                // is ready again; before that it is discarding until one.
                if !synced {
                    lane.backlog = Backlog::Failed;
                }
            }
        }
        Ok(())
    }

    /// Passes queued answers to the client, in order, stopping at the first
    /// failure. Returns the index of the step that failed, if one did.
    ///
    /// `lost` stands in for every step's answer when the connection died.
    fn deliver(
        &mut self,
        replies: &[StepReply],
        lost: Option<&Refusal>,
    ) -> Result<Option<usize>, SessionError> {
        let outputs = std::mem::take(&mut self.extended.outputs);
        let mut failed_step = None;
        for (sequence, output) in outputs {
            match output {
                Output::Ready(messages) => {
                    for message in messages {
                        self.client.queue_raw(&message);
                    }
                }
                Output::Fail(messages) => {
                    for message in messages {
                        self.client.queue_raw(&message);
                    }
                    self.fail_from(sequence);
                    break;
                }
                Output::Step {
                    index,
                    relay,
                    learn,
                    started,
                } => {
                    if let Some(refusal) = lost {
                        self.client.queue_raw(&error_message(refusal)?);
                        self.fail_from(sequence);
                        break;
                    }
                    let Some(reply) = replies.get(index) else {
                        continue;
                    };
                    for message in reply.messages.iter().filter(|m| passes(relay, m)) {
                        self.client.queue(message)?;
                    }
                    if let Some(key) = learn {
                        self.learn(&key, reply);
                    }
                    if reply.failed {
                        if let Some(code) = sqlstate(reply) {
                            self.shared.metrics.error(&code);
                        }
                        self.fail_from(sequence);
                        failed_step = Some(index);
                        break;
                    }
                    if let Some(started) = started {
                        self.shared.metrics.statement("extended", started.elapsed());
                    }
                }
            }
        }
        if !self.extended.skipping {
            self.extended.journal.clear();
        }
        Ok(failed_step)
    }

    /// Undoes the effects of the failed message and everything after it,
    /// then discards the rest of the batch.
    fn fail_from(&mut self, sequence: u64) {
        while self
            .extended
            .journal
            .last()
            .is_some_and(|(at, _)| *at >= sequence)
        {
            let Some((_, undo)) = self.extended.journal.pop() else {
                break;
            };
            match undo {
                Undo::Statement(name, previous) => {
                    restore(&mut self.extended.statements, name, previous);
                }
                Undo::Portal(name, previous) => restore(&mut self.extended.portals, name, previous),
                Undo::Settings(previous) => self.settings = previous,
            }
        }
        self.extended.journal.clear();
        self.extended.outputs.clear();
        self.extended.steps.clear();
        // The statements those steps would have prepared never were.
        for lane in &mut self.extended.lanes {
            lane.named.clear();
        }
        self.extended.pending_lane = None;
        self.extended.skipping = true;
        self.extended.batch_failed = true;
        if self.transaction.is_some() {
            self.status = TransactionStatus::Failed;
        }
    }
}

/// The SQLSTATE of the error in a step's answer, if it has one.
fn sqlstate(reply: &StepReply) -> Option<String> {
    reply
        .messages
        .iter()
        .find_map(|message| match Backend::decode(message) {
            Ok(Backend::Error(notice)) => Some(notice.code().to_owned()),
            _ => None,
        })
}

fn restore<T>(map: &mut HashMap<String, T>, name: String, previous: Option<T>) {
    match previous {
        Some(value) => {
            map.insert(name, value);
        }
        None => {
            map.remove(&name);
        }
    }
}

/// The first error in a pipeline's answers, as a refusal.
fn failure(replies: &[StepReply]) -> Option<Refusal> {
    replies
        .iter()
        .flat_map(|reply| &reply.messages)
        .find_map(|message| match Backend::decode(message) {
            Ok(Backend::Error(notice)) => Some(plain(notice.code(), notice.message())),
            _ => None,
        })
}

/// The name a client's named statement is prepared under on a backend.
///
/// Derived from the text and declared types rather than the client's name,
/// so sessions preparing the same statement share it on a connection, and a
/// client reusing a name for different text gets a different one. Two
/// statements hashing alike cost a re-parse, never a wrong statement: the
/// connection checks the text before skipping the parse.
fn backend_name(sql: &str, types: &[i32]) -> String {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    types.hash(&mut hasher);
    format!("luxbyte_{:016x}", hasher.finish())
}

/// A string literal that means the same whatever
/// `standard_conforming_strings` is set to.
fn escape_literal(text: &str) -> String {
    let mut out = String::with_capacity(text.len().saturating_add(3));
    out.push_str("E'");
    for character in text.chars() {
        match character {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("''"),
            other => out.push(other),
        }
    }
    out.push('\'');
    out
}

fn plain(sqlstate: &str, message: impl Into<String>) -> Refusal {
    Refusal {
        sqlstate: sqlstate.to_owned(),
        message: message.into(),
        hint: None,
        retryable: false,
    }
}

fn aborted() -> Refusal {
    Refusal {
        sqlstate: "25P02".to_owned(),
        message: "current transaction is aborted, commands ignored until end of transaction \
                  block"
            .to_owned(),
        hint: Some("send ROLLBACK".to_owned()),
        retryable: false,
    }
}

fn unconfigured(region: &str) -> Refusal {
    plain("08000", format!("region {region} is not configured"))
}

fn error_message(refusal: &Refusal) -> Result<Bytes, SessionError> {
    let mut builder = build::ErrorBuilder::error(&refusal.sqlstate, refusal.message.clone());
    if let Some(hint) = &refusal.hint {
        builder = builder.hint(hint.clone());
    }
    Ok(builder.build()?)
}

/// A `RowDescription` for a result the proxy computed.
fn describe_result(result: &QueryResult) -> Result<Bytes, SessionError> {
    if result.columns.is_empty() {
        return Ok(build::no_data()?);
    }
    let names: Vec<&str> = result.columns.iter().map(String::as_str).collect();
    Ok(build::row_description_typed(&names, &result.type_oids)?)
}

fn is_copy(sql: &str) -> bool {
    sql.trim_start()
        .get(..4)
        .is_some_and(|word| word.eq_ignore_ascii_case("COPY"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_is_recognised_however_it_is_written() {
        assert!(is_copy("COPY users FROM STDIN"));
        assert!(is_copy("  copy users TO STDOUT"));
        assert!(!is_copy("SELECT 'COPY'"));
        assert!(!is_copy("CO"));
    }

    #[test]
    fn a_quiet_relay_keeps_only_errors_and_notices() {
        let complete = RawMessage::tagged(tag::PARSE_COMPLETE, Bytes::new());
        let error = RawMessage::tagged(tag::EXECUTE_OR_ERROR, Bytes::new());
        let notice = RawMessage::tagged(tag::NOTICE_RESPONSE, Bytes::new());
        assert!(!passes(Relay::Quiet, &complete));
        assert!(passes(Relay::Quiet, &error));
        assert!(passes(Relay::Quiet, &notice));
        assert!(passes(Relay::Answer, &complete));
    }
}
