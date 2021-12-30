use futures::TryStreamExt;

use async_trait::async_trait;
use cqrs_es::Aggregate;
use persist_es::{PersistedEventRepository, PersistenceError, SerializedEvent, SerializedSnapshot};
use serde_json::Value;
use sqlx::mysql::MySqlRow;
use sqlx::{MySql, Pool, Row, Transaction};

use crate::error::MysqlAggregateError;

static INSERT_EVENT: &str =
    "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata)
                               VALUES (?, ?, ?, ?, ?, ?, ?)";

static SELECT_EVENTS: &str =
    "SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
                                FROM events
                                WHERE aggregate_type = ? 
                                  AND aggregate_id = ? ORDER BY sequence";

static INSERT_SNAPSHOT: &str =
    "INSERT INTO snapshots (aggregate_type, aggregate_id, last_sequence, current_snapshot, payload)
                               VALUES (?, ?, ?, ?, ?)";
static UPDATE_SNAPSHOT: &str = "UPDATE snapshots
                               SET last_sequence= ? , payload= ?, current_snapshot= ?
                               WHERE aggregate_type= ? AND aggregate_id= ? AND current_snapshot= ?";
static SELECT_SNAPSHOT: &str =
    "SELECT aggregate_type, aggregate_id, last_sequence, current_snapshot, payload
                                FROM snapshots
                                WHERE aggregate_type = ? AND aggregate_id = ?";

/// A snapshot backed event repository for use in backing a `PersistedSnapshotStore`.
pub struct MysqlEventRepository {
    pool: Pool<MySql>,
}

#[async_trait]
impl PersistedEventRepository for MysqlEventRepository {
    async fn get_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let query = SELECT_EVENTS;
        self.select_events::<A>(aggregate_id, query).await
    }

    async fn get_last_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        number_events: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let query = format!(
            "SELECT aggregate_type, aggregate_id, sequence, payload, metadata
                                FROM events
                                WHERE aggregate_type = $1 AND aggregate_id = $2
                                  AND sequence > (SELECT max(sequence)
                                                  FROM events
                                                  WHERE aggregate_type = $1
                                                    AND aggregate_id = $2) - {}
                                ORDER BY sequence",
            number_events
        );
        self.select_events::<A>(aggregate_id, &query).await
    }

    async fn get_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        let row: MySqlRow = match sqlx::query(SELECT_SNAPSHOT)
            .bind(A::aggregate_type())
            .bind(&aggregate_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(MysqlAggregateError::from)?
        {
            Some(row) => row,
            None => {
                return Ok(None);
            }
        };
        Ok(Some(self.deser_snapshot(row)?))
    }

    async fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        match snapshot_update {
            None => {
                self.insert_events::<A>(events).await?;
            }
            Some((aggregate_id, aggregate, current_snapshot)) => {
                if current_snapshot == 1 {
                    self.insert::<A>(aggregate, aggregate_id, current_snapshot, events)
                        .await?;
                } else {
                    self.update::<A>(aggregate, aggregate_id, current_snapshot, events)
                        .await?;
                }
            }
        };
        Ok(())
    }
}

impl MysqlEventRepository {
    async fn select_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        query: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let mut rows = sqlx::query(query)
            .bind(A::aggregate_type())
            .bind(aggregate_id)
            .fetch(&self.pool);
        let mut result: Vec<SerializedEvent> = Default::default();
        while let Some(row) = rows.try_next().await.map_err(MysqlAggregateError::from)? {
            result.push(self.deser_event(row)?);
        }
        Ok(result)
    }
}

impl MysqlEventRepository {
    /// Creates a new `MysqlEventRepository` from the provided database connection
    /// used for backing a `MysqlSnapshotStore`.
    ///
    /// ```ignore
    /// let store = MysqlEventRepository::<MyAggregate>::new(pool);
    /// ```
    pub fn new(pool: Pool<MySql>) -> Self {
        Self { pool }
    }

    pub(crate) async fn insert_events<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
    ) -> Result<(), MysqlAggregateError> {
        let mut tx: Transaction<MySql> = sqlx::Acquire::begin(&self.pool).await?;
        self.persist_events::<A>(&mut tx, events).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn insert<A: Aggregate>(
        &self,
        aggregate_payload: Value,
        aggregate_id: String,
        current_snapshot: usize,
        events: &[SerializedEvent],
    ) -> Result<(), MysqlAggregateError> {
        let mut tx: Transaction<MySql> = sqlx::Acquire::begin(&self.pool).await?;
        let current_sequence = self.persist_events::<A>(&mut tx, events).await?;
        sqlx::query(INSERT_SNAPSHOT)
            .bind(A::aggregate_type())
            .bind(aggregate_id.as_str())
            .bind(current_sequence as u32)
            .bind(current_snapshot as u32)
            .bind(&aggregate_payload)
            .execute(&mut tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn update<A: Aggregate>(
        &self,
        aggregate: Value,
        aggregate_id: String,
        current_snapshot: usize,
        events: &[SerializedEvent],
    ) -> Result<(), MysqlAggregateError> {
        let mut tx: Transaction<MySql> = sqlx::Acquire::begin(&self.pool).await?;
        let current_sequence = self.persist_events::<A>(&mut tx, events).await?;

        let aggregate_payload = serde_json::to_value(&aggregate)?;
        sqlx::query(UPDATE_SNAPSHOT)
            .bind(current_sequence as u32)
            .bind(&aggregate_payload)
            .bind(current_snapshot as u32)
            .bind(A::aggregate_type())
            .bind(aggregate_id.as_str())
            .bind((current_snapshot - 1) as u32)
            .execute(&mut tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    fn deser_event(&self, row: MySqlRow) -> Result<SerializedEvent, MysqlAggregateError> {
        let aggregate_type: String = row.get("aggregate_type");
        let aggregate_id: String = row.get("aggregate_id");
        let sequence = {
            let s: i64 = row.get("sequence");
            s as usize
        };
        let event_type: String = row.get("event_type");
        let event_version: String = row.get("event_version");
        let payload: Value = row.get("payload");
        let metadata: Value = row.get("metadata");
        Ok(SerializedEvent::new(
            aggregate_id,
            sequence,
            aggregate_type,
            event_type,
            event_version,
            payload,
            metadata,
        ))
    }

    fn deser_snapshot(&self, row: MySqlRow) -> Result<SerializedSnapshot, MysqlAggregateError> {
        let aggregate_id = row.get("aggregate_id");
        let s: i64 = row.get("last_sequence");
        let current_sequence = s as usize;
        let s: i64 = row.get("current_snapshot");
        let current_snapshot = s as usize;
        let aggregate: Value = row.get("payload");
        Ok(SerializedSnapshot {
            aggregate_id,
            aggregate,
            current_sequence,
            current_snapshot,
        })
    }

    pub(crate) async fn persist_events<A: Aggregate>(
        &self,
        tx: &mut Transaction<'_, MySql>,
        events: &[SerializedEvent],
    ) -> Result<usize, MysqlAggregateError> {
        let mut current_sequence: usize = 0;
        for event in events {
            current_sequence = event.sequence;
            let event_type = &event.event_type;
            let event_version = &event.event_version;
            let payload = serde_json::to_value(&event.payload)?;
            let metadata = serde_json::to_value(&event.metadata)?;
            sqlx::query(INSERT_EVENT)
                .bind(A::aggregate_type())
                .bind(event.aggregate_id.as_str())
                .bind(event.sequence as u32)
                .bind(event_type)
                .bind(event_version)
                .bind(&payload)
                .bind(&metadata)
                .execute(&mut *tx)
                .await?;
        }
        Ok(current_sequence)
    }
}
