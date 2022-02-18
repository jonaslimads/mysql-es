#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;

    use cqrs_es::{Aggregate, AggregateError, DomainEvent, EventEnvelope, UserErrorPayload, View};
    use persist_es::{
        GenericQuery, PersistedEventStore, PersistedSnapshotStore, SerializedEvent,
        SerializedSnapshot,
    };
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use sqlx::{MySql, Pool};

    use crate::query_repository::MysqlViewRepository;
    use crate::MysqlEventRepository;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    pub struct TestAggregate {
        pub(crate) id: String,
        pub(crate) description: String,
        pub(crate) tests: Vec<String>,
    }

    impl Aggregate for TestAggregate {
        type Command = TestCommand;
        type Event = TestEvent;
        type Error = UserErrorPayload;

        fn aggregate_type() -> &'static str {
            "TestAggregate"
        }

        fn handle(
            &self,
            _command: Self::Command,
        ) -> Result<Vec<Self::Event>, AggregateError<Self::Error>> {
            Ok(vec![])
        }

        fn apply(&mut self, _e: Self::Event) {}
    }

    impl Default for TestAggregate {
        fn default() -> Self {
            TestAggregate {
                id: "".to_string(),
                description: "".to_string(),
                tests: Vec::new(),
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
    pub enum TestEvent {
        Created(Created),
        Tested(Tested),
        SomethingElse(SomethingElse),
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
    pub struct Created {
        pub id: String,
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
    pub struct Tested {
        pub test_name: String,
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
    pub struct SomethingElse {
        pub description: String,
    }

    impl DomainEvent for TestEvent {
        fn event_type(&self) -> &'static str {
            match self {
                TestEvent::Created(_) => "Created",
                TestEvent::Tested(_) => "Tested",
                TestEvent::SomethingElse(_) => "SomethingElse",
            }
        }

        fn event_version(&self) -> &'static str {
            "1.0"
        }
    }

    pub enum TestCommand {}

    pub(crate) type TestQueryRepository =
        GenericQuery<MysqlViewRepository<TestView, TestAggregate>, TestView, TestAggregate>;

    #[derive(Debug, Default, Serialize, Deserialize)]
    pub(crate) struct TestView {
        events: Vec<TestEvent>,
    }

    impl View<TestAggregate> for TestView {
        fn update(&mut self, event: &EventEnvelope<TestAggregate>) {
            self.events.push(event.payload.clone());
        }
    }

    pub(crate) const TEST_CONNECTION_STRING: &str =
        "mysql://test_user:test_pass@localhost:3306/test";

    pub(crate) async fn new_test_event_store(
        pool: Pool<MySql>,
    ) -> PersistedEventStore<MysqlEventRepository, TestAggregate> {
        let repo = MysqlEventRepository::new(pool);
        PersistedEventStore::<MysqlEventRepository, TestAggregate>::new(repo)
    }

    pub(crate) async fn new_test_snapshot_store(
        pool: Pool<MySql>,
    ) -> PersistedSnapshotStore<MysqlEventRepository, TestAggregate> {
        let repo = MysqlEventRepository::new(pool.clone());
        PersistedSnapshotStore::<MysqlEventRepository, TestAggregate>::new(repo)
    }

    pub(crate) fn new_test_metadata() -> HashMap<String, String> {
        let now = "2021-03-18T12:32:45.930Z".to_string();
        let mut metadata = HashMap::new();
        metadata.insert("time".to_string(), now);
        metadata
    }

    pub(crate) fn test_event_envelope(
        id: &str,
        sequence: usize,
        event: TestEvent,
    ) -> SerializedEvent {
        let payload: Value = serde_json::to_value(&event).unwrap();
        SerializedEvent {
            aggregate_id: id.to_string(),
            sequence,
            aggregate_type: TestAggregate::aggregate_type().to_string(),
            event_type: event.event_type().to_string(),
            event_version: event.event_version().to_string(),
            payload,
            metadata: Default::default(),
        }
    }

    pub(crate) fn snapshot_context(
        aggregate_id: String,
        current_sequence: usize,
        current_snapshot: usize,
        aggregate: Value,
    ) -> SerializedSnapshot {
        SerializedSnapshot {
            aggregate_id,
            aggregate,
            current_sequence,
            current_snapshot,
        }
    }
}
