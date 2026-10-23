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
//! itself, and re-parses the statement as the backend's unnamed statement on
//! whichever connection a `Bind` is routed to. It costs the backend a parse;
//! it saves the client from a "prepared statement does not exist" that
//! depends on which connection it happened to get.
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
//! # What is refused
//!
//! A batch outside a transaction may read from any number of regions but
//! write to only one: atomicity across two would need two-phase commit,
//! which only an explicit transaction gets. A statement whose plan spans
//! regions is refused too, because scatter-gather and split writes are built
//! on the simple protocol's text results. Both refusals say what to do.

use std::collections::HashMap;
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
    /// Nothing to run.
    Empty,
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
        }
    }
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

        let statement = Arc::new(Statement {
            key: (parse.sql.to_owned(), parse.parameter_types.clone()),
            body,
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
            self.queue_parse(sequence, lane, &statement).await?;
            self.queue(
                sequence,
                lane,
                Step::new(StepKind::Bind, bind.encode_from("")?),
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
        let (region, access) = match self.shared.router.plan(analysis, Some(&parameters)) {
            Plan::Single { region, access } => (region, access),
            // SHOW is answered by a backend like any read.
            Plan::Local(LocalIntent::Show) => (
                self.shared.router.topology().home_region.clone(),
                Access::ReadOnly,
            ),
            Plan::Local(intent) => return Ok(Ok(Destination::Local(intent))),
            Plan::Refuse(refusal) => return Ok(Err(refusal)),
            Plan::ScatterGather { regions, .. } | Plan::Distributed { regions } => {
                return Ok(Err(Refusal {
                    sqlstate: "0A000".to_owned(),
                    message: format!(
                        "the statement spans regions {}, which the extended query protocol \
                         does not support yet",
                        regions.join(", ")
                    ),
                    hint: Some(
                        "bind a shard key that names one region, or send this statement as a \
                         simple query"
                            .to_owned(),
                    ),
                    retryable: false,
                }));
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
        self.queue_parse(sequence, &lane, &statement).await?;
        let step = Step::new(
            StepKind::DescribeStatement,
            frontend::describe(Target::Statement, "")?,
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
        self.reads = self.reads.wrapping_add(1);
        let Some(pool) = self.shared.pool_for(region, Access::ReadOnly, self.reads) else {
            return self.refuse(
                sequence,
                &plain("08000", format!("region {region} is not configured")),
            );
        };
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
        let replies = match pool.acquire().await {
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
                let step = Step::new(StepKind::Execute, frontend::execute(portal, max_rows)?);
                self.queue(sequence, &lane, step, Relay::Answer, None).await
            }
            Destination::Empty => {
                self.owe(sequence, vec![build::empty_query_response()?]);
                Ok(())
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
                    },
                ));
                self.flush_to(&lane, true).await?;
            }
        }
        self.flush_pending().await?;

        let failed = self.extended.batch_failed;
        let lanes = std::mem::take(&mut self.extended.lanes);
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

        if self.transaction.is_none() {
            self.extended.portals.clear();
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
            let Some(pool) = self.shared.pool_for(region, Access::ReadWrite, 0) else {
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

        self.reads = self.reads.wrapping_add(1);
        let Some(pool) = self.shared.pool_for(region, access, self.reads) else {
            return Ok(Err(unconfigured(region)));
        };
        let connection = match pool.acquire().await {
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
    /// unnamed statement already holds it.
    async fn queue_parse(
        &mut self,
        sequence: u64,
        lane: &LaneId,
        statement: &Statement,
    ) -> Result<(), SessionError> {
        self.switch_to(lane).await?;
        if self.extended.skipping {
            return Ok(());
        }
        self.queue_settings(sequence, lane)?;
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
        Ok(())
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
        let replies = if steps.is_empty() {
            Ok(Vec::new())
        } else {
            let mut lanes = std::mem::take(&mut self.extended.lanes);
            let outcome = match lanes.iter_mut().find(|lane| &lane.id == id) {
                Some(lane) => {
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
        if self.deliver(&replies, None)?.is_some() {
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
                        self.fail_from(sequence);
                        failed_step = Some(index);
                        break;
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
        self.extended.pending_lane = None;
        self.extended.skipping = true;
        self.extended.batch_failed = true;
        if self.transaction.is_some() {
            self.status = TransactionStatus::Failed;
        }
    }
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
