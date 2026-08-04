//! Reading a source file into Arrow record batches.
//!
//! The ingest verbs read the inferred formats (CSV, JSONL, Parquet — issue
//! #287), which discover their own schema: CSV and JSONL infer with Arrow's
//! readers, Parquet self-describes. Every path returns one Arrow schema plus
//! the batches, which the write path turns into an Iceberg schema and Parquet
//! data files.

use std::fs::File;
use std::io::{BufReader, Seek};
use std::path::Path;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::error::{AgentError, Result};

/// The supported source formats, all chosen from a path's extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// Comma-separated values with a header row.
    Csv,
    /// One JSON object per line.
    Jsonl,
    /// Apache Parquet (schema is read from the file's footer).
    Parquet,
}

impl Format {
    /// Picks a format from a path's extension. `.json` is treated as JSONL.
    pub fn from_path(path: &Path) -> Result<Format> {
        match path
            .extension()
            .and_then(|e| e.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("csv") => Ok(Format::Csv),
            Some("jsonl" | "json" | "ndjson") => Ok(Format::Jsonl),
            Some("parquet" | "pq") => Ok(Format::Parquet),
            _ => Err(AgentError::UnknownFormat(path.display().to_string())),
        }
    }
}

/// A source file read into memory: its Arrow schema and every record batch.
pub struct Ingested {
    /// The inferred (CSV/JSONL) or read (Parquet) Arrow schema.
    pub schema: SchemaRef,
    /// The rows, as Arrow record batches.
    pub batches: Vec<RecordBatch>,
}

impl Ingested {
    /// Total row count across all batches.
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }
}

/// Reads `path`, inferring the format from the path's extension (CSV / JSONL /
/// Parquet).
pub fn read(path: &Path) -> Result<Ingested> {
    match Format::from_path(path)? {
        Format::Csv => read_csv(path),
        Format::Jsonl => read_jsonl(path),
        Format::Parquet => read_parquet(path),
    }
}

/// Reads and infers a CSV file (header row required). Inference samples the
/// whole file so a column that is integer-until-late is still typed correctly.
fn read_csv(path: &Path) -> Result<Ingested> {
    use arrow_csv::reader::{Format as CsvFormat, ReaderBuilder};

    let ingest_err = |detail: String| AgentError::Ingest {
        path: path.display().to_string(),
        detail,
    };

    let csv_format = CsvFormat::default().with_header(true);
    let infer_file = File::open(path).map_err(|e| ingest_err(e.to_string()))?;
    let (schema, _rows) = csv_format
        .infer_schema(BufReader::new(infer_file), None)
        .map_err(|e| ingest_err(e.to_string()))?;
    let schema = Arc::new(schema);

    let data_file = File::open(path).map_err(|e| ingest_err(e.to_string()))?;
    let reader = ReaderBuilder::new(schema.clone())
        .with_format(csv_format)
        .build(BufReader::new(data_file))
        .map_err(|e| ingest_err(e.to_string()))?;
    let batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| ingest_err(e.to_string()))?;
    Ok(Ingested { schema, batches })
}

/// Reads and infers a JSONL file. Nested objects infer as struct columns.
fn read_jsonl(path: &Path) -> Result<Ingested> {
    use arrow_json::reader::{ReaderBuilder, infer_json_schema_from_seekable};

    let ingest_err = |detail: String| AgentError::Ingest {
        path: path.display().to_string(),
        detail,
    };

    let mut infer_reader = BufReader::new(File::open(path).map_err(|e| ingest_err(e.to_string()))?);
    let (schema, _rows) = infer_json_schema_from_seekable(&mut infer_reader, None)
        .map_err(|e| ingest_err(e.to_string()))?;
    infer_reader
        .rewind()
        .map_err(|e| ingest_err(e.to_string()))?;
    let schema = Arc::new(schema);

    let reader = ReaderBuilder::new(schema.clone())
        .build(infer_reader)
        .map_err(|e| ingest_err(e.to_string()))?;
    let batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| ingest_err(e.to_string()))?;
    Ok(Ingested { schema, batches })
}

/// Reads a Parquet file, taking its schema from the footer.
fn read_parquet(path: &Path) -> Result<Ingested> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let ingest_err = |detail: String| AgentError::Ingest {
        path: path.display().to_string(),
        detail,
    };

    let file = File::open(path).map_err(|e| ingest_err(e.to_string()))?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| ingest_err(e.to_string()))?;
    let schema = builder.schema().clone();
    let reader = builder.build().map_err(|e| ingest_err(e.to_string()))?;
    let batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| ingest_err(e.to_string()))?;
    Ok(Ingested { schema, batches })
}
