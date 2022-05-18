use cqrs_es::persist::PersistedEventStore;
use cqrs_es::{Aggregate, CqrsFramework, Query};

use crate::{MysqlCqrs, MysqlEventRepository};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySql, Pool};

/// A convenience building a simple connection pool for MySql database.
pub async fn default_mysql_pool(connection_string: &str) -> Pool<MySql> {
    MySqlPoolOptions::new()
        .max_connections(10)
        .connect(connection_string)
        .await
        .expect("unable to connect to database")
}

/// A convenience method for building a simple connection pool for MySql.
/// A connection pool is needed for both the event and view repositories.
///
/// ```
/// use sqlx::{MySql, Pool};
/// use mysql_es::default_mysql_pool;
///
/// # async fn configure_pool() {
/// let connection_string = "mysql://test_user:test_pass@localhost:3306/test";
/// let pool: Pool<MySql> = default_mysql_pool(connection_string).await;
/// # }
/// ```
pub fn mysql_cqrs<A>(
    pool: Pool<MySql>,
    query_processor: Vec<Box<dyn Query<A>>>,
    services: A::Services,
) -> MysqlCqrs<A>
where
    A: Aggregate,
{
    let repo = MysqlEventRepository::new(pool);
    let store = PersistedEventStore::new_event_store(repo);
    CqrsFramework::new(store, query_processor, services)
}

/// A convenience function for creating a CqrsFramework using a snapshot store.
pub fn mysql_snapshot_cqrs<A>(
    pool: Pool<MySql>,
    query_processor: Vec<Box<dyn Query<A>>>,
    snapshot_size: usize,
    services: A::Services,
) -> MysqlCqrs<A>
where
    A: Aggregate,
{
    let repo = MysqlEventRepository::new(pool);
    let store = PersistedEventStore::new_snapshot_store(repo, snapshot_size);
    CqrsFramework::new(store, query_processor, services)
}

/// A convenience function for creating a CqrsFramework using an aggregate store.
pub fn mysql_aggregate_cqrs<A>(
    pool: Pool<MySql>,
    query_processor: Vec<Box<dyn Query<A>>>,
    services: A::Services,
) -> MysqlCqrs<A>
where
    A: Aggregate,
{
    let repo = MysqlEventRepository::new(pool);
    let store = PersistedEventStore::new_aggregate_store(repo);
    CqrsFramework::new(store, query_processor, services)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use cqrs_es::persist::ViewRepository;
    use sqlx::MySqlPool;

    use crate::testing::tests::{
        TestAggregate, TestCommand, TestQueryRepository, TestServices, TestView,
        TEST_CONNECTION_STRING,
    };
    use crate::{default_mysql_pool, mysql_cqrs, MysqlCqrs, MysqlViewRepository};

    #[tokio::test]
    async fn test_valid_cqrs_framework() {
        instantiate_cqrs_pool_repo().await;
    }

    #[tokio::test]
    async fn view_payload_gets_incompatible_if_read_model_changes_without_replay() {
        let (cqrs, repo, pool) = instantiate_cqrs_pool_repo().await;

        let id = uuid::Uuid::new_v4().to_string();

        let create_command = TestCommand::Create { id: id.clone() };
        cqrs.execute(id.clone().as_str(), create_command)
            .await
            .unwrap();
        let (_, view_context) = repo.load_with_context(&id).await.unwrap().unwrap();
        assert_eq!(1, view_context.version);

        // force incompatibility by directly updating previous view payload
        sqlx::query(
            "UPDATE test_view SET
                payload = JSON_SET(payload, '$.old_events', payload->'$.events'),
                payload = JSON_REMOVE(payload, '$.events')
            WHERE view_id = ?",
        )
        .bind(id.clone())
        .execute(&pool)
        .await
        .unwrap();

        let test_command = TestCommand::Test {
            test_name: "event replay".to_string(),
        };
        cqrs.execute(id.clone().as_str(), test_command)
            .await
            .unwrap();
        let result = repo.load_with_context(&id).await;
        assert_eq!(true, result.is_err());
    }

    async fn instantiate_cqrs_pool_repo() -> (
        MysqlCqrs<TestAggregate>,
        Arc<MysqlViewRepository<TestView, TestAggregate>>,
        MySqlPool,
    ) {
        let pool = default_mysql_pool(TEST_CONNECTION_STRING).await;
        let repo = Arc::new(MysqlViewRepository::<TestView, TestAggregate>::new(
            "test_view",
            pool.clone(),
        ));
        let query = TestQueryRepository::new(repo.clone());
        let cqrs = mysql_cqrs(pool.clone(), vec![Box::new(query)], TestServices);
        (cqrs, repo, pool)
    }
}
