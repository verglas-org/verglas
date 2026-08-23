//! Non-durable query objects hold a cache pin without allocating commit authority.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use verglas_do_engine::{DatasetCache, DatasetCachePin, QueryObject, Result};

struct Pin {
    drops: Arc<AtomicUsize>,
}

impl DatasetCachePin for Pin {}

impl Drop for Pin {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct Cache {
    pins: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

#[async_trait]
impl DatasetCache for Cache {
    async fn pin(&self, dataset_id: &str) -> Result<Box<dyn DatasetCachePin>> {
        assert_eq!(dataset_id, "s3://managed-lake/events");
        self.pins.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Pin {
            drops: self.drops.clone(),
        }))
    }
}

#[tokio::test]
async fn query_object_pins_dataset_executes_datafusion_and_releases_on_drop() {
    let pins = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let cache = Arc::new(Cache {
        pins: pins.clone(),
        drops: drops.clone(),
    });
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![3, 4]))])
        .expect("batch");
    let table = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("table"));
    let query = QueryObject::new("s3://managed-lake/events", cache)
        .await
        .expect("query object");
    query.register_table("events", table).expect("register");

    let batches = query
        .execute("SELECT sum(value) AS total FROM events")
        .await
        .expect("query");
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int total")
        .value(0);
    assert_eq!(total, 7);
    assert_eq!(pins.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(query);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
