//! Narrow Iceberg capabilities used by the prebuilt Sink and Catalog products.
//!
//! The crate opens an Iceberg REST catalog, writes deterministic Parquet Sink
//! batches through OpenDAL, and records replay-safe snapshot receipts. It has no
//! query engine, generic ingestion framework, maintenance worker, or custom CAS.

pub mod catalog;
pub mod conn;
pub mod error;
pub mod ident;
pub mod storage;
pub mod tables_api;
pub mod write;

pub use catalog::open_catalog;
pub use conn::Connection;
pub use error::{AgentError, Result};
pub use ident::{ident_to_dotted, parse_table_ident};
pub use tables_api::{
    SINK_BATCH_ID_PROPERTY, SINK_COMPRESSION_PROPERTY, SINK_FILE_ID_PROPERTY, SINK_OWNER_PROPERTY,
    SINK_PAYLOAD_DIGEST_PROPERTY, SINK_ROW_COUNT_PROPERTY, SinkBatchConfig, SinkBatchRequest,
    SinkCommitReceipt, SinkCompression, commit_sink_batch, deterministic_sink_file_id,
};
pub use write::{TableCache, create_table_from_schema};
