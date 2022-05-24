use std::collections::HashMap;

use async_trait::async_trait;
use cqrs_es::persist::{
    PersistedEventRepository, PersistenceError, SerializedEvent, SerializedSnapshot,
};
use cqrs_es::Aggregate;
use futures::TryStreamExt;
use serde_json::Value;
use sqlx::mysql::MySqlRow;
use sqlx::{MySql, Pool, QueryBuilder, Row, Transaction};

use crate::error::MysqlAggregateError;

const DEFAULT_EVENT_TABLE: &str = "events";
const DEFAULT_SNAPSHOT_TABLE: &str = "snapshots";

/// A snapshot backed event repository for use in backing a `PersistedSnapshotStore`.
pub struct MysqlEventRepository {
    ///
    pub pool: Pool<MySql>,
    ///
    pub event_table: String,
    ///
    pub insert_event: String,
    ///
    pub select_events: String,
    ///
    pub select_last_events: String,
    ///
    pub select_all_aggregate_events: String,
    ///
    pub select_multiple_aggregate_events: String,
    ///
    pub insert_snapshot: String,
    ///
    pub update_snapshot: String,
    ///
    pub select_snapshot: String,
}

#[async_trait]
impl PersistedEventRepository for MysqlEventRepository {
    async fn get_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.select_events::<A>(aggregate_id, &self.select_events)
            .await
    }

    async fn get_last_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.select_last_events::<A>(aggregate_id, last_sequence, &self.select_last_events)
            .await
    }

    async fn get_multiple_aggregate_events<A: Aggregate>(
        &self,
        _aggregate_ids: Vec<&str>,
    ) -> Result<HashMap<String, Vec<SerializedEvent>>, PersistenceError> {
        self.select_all_aggregate_events::<A>(&self.select_all_aggregate_events)
            .await
        // self.select_multiple_aggregate_events::<A>(
        //     aggregate_ids,
        //     &self.select_multiple_aggregate_events,
        // )
        // .await
    }

    async fn get_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        let row: MySqlRow = match sqlx::query(&self.select_snapshot)
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

    async fn select_last_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        sequence: usize,
        query: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let mut rows = sqlx::query(query)
            .bind(A::aggregate_type())
            .bind(aggregate_id)
            .bind(sequence as u32)
            .fetch(&self.pool);
        let mut result: Vec<SerializedEvent> = Default::default();
        while let Some(row) = rows.try_next().await.map_err(MysqlAggregateError::from)? {
            result.push(self.deser_event(row)?);
        }
        Ok(result)
    }

    async fn select_all_aggregate_events<A: Aggregate>(
        &self,
        query: &str,
    ) -> Result<HashMap<String, Vec<SerializedEvent>>, PersistenceError> {
        let mut rows = sqlx::query(query)
            .bind(A::aggregate_type())
            .fetch(&self.pool);
        let mut result: HashMap<String, Vec<SerializedEvent>> = HashMap::new();
        while let Some(row) = rows.try_next().await.map_err(MysqlAggregateError::from)? {
            let event = self.deser_event(row)?;
            if let Some(events) = result.get_mut(&event.aggregate_id) {
                events.push(event);
            } else {
                result.insert(event.aggregate_id.clone(), vec![event]);
            }
        }

        Ok(result)
    }

    async fn select_multiple_aggregate_events<A: Aggregate>(
        &self,
        aggregate_ids: Vec<&str>,
        query: &str,
    ) -> Result<HashMap<String, Vec<SerializedEvent>>, PersistenceError> {
        let query = query.to_string();
        let parts: Vec<&str> = query.split("?").collect();

        let mut query_builder: QueryBuilder<MySql> = QueryBuilder::new(parts[0]);
        query_builder.push_bind(A::aggregate_type());
        if parts.len() > 1 {
            query_builder.push(parts[1]);
            query_builder.push("(");
            let mut separated = query_builder.separated(", ");
            for aggregate_id in aggregate_ids {
                separated.push_bind(aggregate_id.to_string());
            }
            query_builder.push(")");
        }

        if parts.len() > 2 {
            query_builder.push(parts[2]);
        }

        let mut rows = query_builder.build().fetch(&self.pool);
        let mut result: HashMap<String, Vec<SerializedEvent>> = HashMap::new();
        while let Some(row) = rows.try_next().await.map_err(MysqlAggregateError::from)? {
            let event = self.deser_event(row)?;
            if let Some(events) = result.get_mut(&event.aggregate_id) {
                events.push(event);
            } else {
                result.insert(event.aggregate_id.clone(), vec![event]);
            }
        }

        Ok(result)
    }
}

impl MysqlEventRepository {
    /// Creates a new `MysqlEventRepository` from the provided database connection
    /// used for backing a `MysqlSnapshotStore`. This uses the default tables 'events'
    /// and 'snapshots'.
    ///
    /// ```
    /// use sqlx::{MySql, Pool};
    /// use mysql_es::MysqlEventRepository;
    ///
    /// fn configure_repo(pool: Pool<MySql>) -> MysqlEventRepository {
    ///     MysqlEventRepository::new(pool)
    /// }
    /// ```
    pub fn new(pool: Pool<MySql>) -> Self {
        Self::use_tables(pool, DEFAULT_EVENT_TABLE, DEFAULT_SNAPSHOT_TABLE)
    }

    /// Configures a `MysqlEventRepository` to use the provided table names.
    ///
    /// ```
    /// use sqlx::{MySql, Pool};
    /// use mysql_es::MysqlEventRepository;
    ///
    /// fn configure_repo(pool: Pool<MySql>) -> MysqlEventRepository {
    ///     let store = MysqlEventRepository::new(pool);
    ///     store.with_tables("my_event_table", "my_snapshot_table")
    /// }
    /// ```
    pub fn with_tables(self, events_table: &str, snapshots_table: &str) -> Self {
        Self::use_tables(self.pool, events_table, snapshots_table)
    }

    fn use_tables(pool: Pool<MySql>, events_table: &str, snapshots_table: &str) -> Self {
        Self {
            pool,
            event_table: events_table.to_string(),
            insert_event: format!("INSERT INTO {} (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata)
                                       VALUES (?, ?, ?, ?, ?, ?, ?)", events_table),
            select_events: format!("SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
                                        FROM {}
                                        WHERE aggregate_type = ?
                                          AND aggregate_id = ? ORDER BY sequence", events_table),
            insert_snapshot: format!("INSERT INTO {} (aggregate_type, aggregate_id, last_sequence, current_snapshot, payload)
                                       VALUES (?, ?, ?, ?, ?)", snapshots_table),
            update_snapshot: format!("UPDATE {}
                                           SET last_sequence= ? , payload= ?, current_snapshot= ?
                                           WHERE aggregate_type= ? AND aggregate_id= ? AND current_snapshot= ?", snapshots_table),
            select_snapshot: format!("SELECT aggregate_type, aggregate_id, last_sequence, current_snapshot, payload
                                        FROM {}
                                        WHERE aggregate_type = ? AND aggregate_id = ?", snapshots_table),
            select_last_events: format!("SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
                                        FROM {}
                                        WHERE aggregate_type = ?
                                            AND aggregate_id = ?
                                            AND sequence > ?
                                        ORDER BY sequence", events_table),
            select_all_aggregate_events:
                format!("SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
                        FROM {}
                        WHERE aggregate_type = ?
                        ORDER BY aggregate_id, sequence", events_table),
            select_multiple_aggregate_events: format!("SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
                                                        FROM {}
                                                        WHERE aggregate_type = ?
                                                            AND aggregate_id IN ?
                                                        ORDER BY aggregate_id, sequence", events_table)
        }
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
        if A::aggregate_type() == "" {
            sqlx::query(&self.insert_snapshot)
                .bind(aggregate_id.as_str())
                .bind(current_sequence as u32)
                .bind(current_snapshot as u32)
                .bind(&aggregate_payload)
                .execute(&mut tx)
                .await?;
        } else {
            sqlx::query(&self.insert_snapshot)
                .bind(A::aggregate_type())
                .bind(aggregate_id.as_str())
                .bind(current_sequence as u32)
                .bind(current_snapshot as u32)
                .bind(&aggregate_payload)
                .execute(&mut tx)
                .await?;
        }
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
        let result = sqlx::query(&self.update_snapshot)
            .bind(current_sequence as u32)
            .bind(&aggregate_payload)
            .bind(current_snapshot as u32)
            .bind(A::aggregate_type())
            .bind(aggregate_id.as_str())
            .bind((current_snapshot - 1) as u32)
            .execute(&mut tx)
            .await?;
        tx.commit().await?;
        match result.rows_affected() {
            1 => Ok(()),
            _ => Err(MysqlAggregateError::OptimisticLock),
        }
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
            if A::aggregate_type() == "" {
                sqlx::query(&self.insert_event)
                    .bind(event.aggregate_id.as_str())
                    .bind(event.sequence as u32)
                    .bind(event_type)
                    .bind(event_version)
                    .bind(&payload)
                    .bind(&metadata)
                    .execute(&mut *tx)
                    .await?;
            } else {
                sqlx::query(&self.insert_event)
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
        }
        Ok(current_sequence)
    }
}

#[cfg(test)]
mod test {
    use cqrs_es::persist::PersistedEventRepository;

    use crate::error::MysqlAggregateError;
    use crate::testing::tests::{
        snapshot_context, test_event_envelope, Created, SomethingElse, TestAggregate, TestEvent,
        Tested, TEST_CONNECTION_STRING,
    };
    use crate::{default_mysql_pool, MysqlEventRepository};

    #[tokio::test]
    async fn event_repositories() {
        let pool = default_mysql_pool(TEST_CONNECTION_STRING).await;
        let id = uuid::Uuid::new_v4().to_string();
        let event_repo =
            MysqlEventRepository::new(pool.clone()).with_tables("test_event", "test_snapshot");
        let events = event_repo.get_events::<TestAggregate>(&id).await.unwrap();
        assert!(events.is_empty());

        event_repo
            .insert_events::<TestAggregate>(&[
                test_event_envelope(&id, 1, TestEvent::Created(Created { id: id.clone() })),
                test_event_envelope(
                    &id,
                    2,
                    TestEvent::Tested(Tested {
                        test_name: "a test was run".to_string(),
                    }),
                ),
            ])
            .await
            .unwrap();
        let events = event_repo.get_events::<TestAggregate>(&id).await.unwrap();
        assert_eq!(2, events.len());
        events.iter().for_each(|e| assert_eq!(&id, &e.aggregate_id));

        let result = event_repo
            .insert_events::<TestAggregate>(&[
                test_event_envelope(
                    &id,
                    3,
                    TestEvent::SomethingElse(SomethingElse {
                        description: "this should not persist".to_string(),
                    }),
                ),
                test_event_envelope(
                    &id,
                    2,
                    TestEvent::SomethingElse(SomethingElse {
                        description: "bad sequence number".to_string(),
                    }),
                ),
            ])
            .await
            .unwrap_err();
        match result {
            MysqlAggregateError::OptimisticLock => {}
            _ => panic!("invalid error result found during insert: {}", result),
        };

        let events = event_repo.get_events::<TestAggregate>(&id).await.unwrap();
        assert_eq!(2, events.len());
    }

    #[tokio::test]
    async fn snapshot_repositories() {
        let pool = default_mysql_pool(TEST_CONNECTION_STRING).await;
        let id = uuid::Uuid::new_v4().to_string();
        let repo =
            MysqlEventRepository::new(pool.clone()).with_tables("test_event", "test_snapshot");
        let snapshot = repo.get_snapshot::<TestAggregate>(&id).await.unwrap();
        assert_eq!(None, snapshot);

        let test_description = "some test snapshot here".to_string();
        let test_tests = vec!["testA".to_string(), "testB".to_string()];
        repo.insert::<TestAggregate>(
            serde_json::to_value(TestAggregate {
                id: id.clone(),
                description: test_description.clone(),
                tests: test_tests.clone(),
            })
            .unwrap(),
            id.clone(),
            1,
            &vec![],
        )
        .await
        .unwrap();

        let snapshot = repo.get_snapshot::<TestAggregate>(&id).await.unwrap();
        assert_eq!(
            Some(snapshot_context(
                id.clone(),
                0,
                1,
                serde_json::to_value(TestAggregate {
                    id: id.clone(),
                    description: test_description.clone(),
                    tests: test_tests.clone(),
                })
                .unwrap()
            )),
            snapshot
        );

        // sequence iterated, does update
        repo.update::<TestAggregate>(
            serde_json::to_value(TestAggregate {
                id: id.clone(),
                description: "a test description that should be saved".to_string(),
                tests: test_tests.clone(),
            })
            .unwrap(),
            id.clone(),
            2,
            &vec![],
        )
        .await
        .unwrap();

        let snapshot = repo.get_snapshot::<TestAggregate>(&id).await.unwrap();
        assert_eq!(
            Some(snapshot_context(
                id.clone(),
                0,
                2,
                serde_json::to_value(TestAggregate {
                    id: id.clone(),
                    description: "a test description that should be saved".to_string(),
                    tests: test_tests.clone(),
                })
                .unwrap()
            )),
            snapshot
        );

        // sequence out of order or not iterated, does not update
        let result = repo
            .update::<TestAggregate>(
                serde_json::to_value(TestAggregate {
                    id: id.clone(),
                    description: "a test description that should not be saved".to_string(),
                    tests: test_tests.clone(),
                })
                .unwrap(),
                id.clone(),
                2,
                &vec![],
            )
            .await
            .unwrap_err();
        match result {
            MysqlAggregateError::OptimisticLock => {}
            _ => panic!("invalid error result found during insert: {}", result),
        };

        let snapshot = repo.get_snapshot::<TestAggregate>(&id).await.unwrap();
        assert_eq!(
            Some(snapshot_context(
                id.clone(),
                0,
                2,
                serde_json::to_value(TestAggregate {
                    id: id.clone(),
                    description: "a test description that should be saved".to_string(),
                    tests: test_tests.clone(),
                })
                .unwrap()
            )),
            snapshot
        );
    }
}
