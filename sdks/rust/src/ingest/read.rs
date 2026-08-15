//! Reading a source file into Arrow record batches (#145).
//!
//! Ingest reads the inferred formats (CSV, JSONL, Parquet), which discover
//! their own schema: CSV and JSONL infer with Arrow's readers, Parquet
//! self-describes. Every path returns one Arrow schema plus the batches, which
//! the write path turns into an Iceberg schema and Parquet data files.

use std::fs::File;
use std::io::{BufReader, Seek};
use std::path::Path;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use super::error::{IngestError, Result};

/// The supported source formats, chosen from a path's extension.
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
    /// Picks a format from a path's extension. `.json`/`.ndjson` are treated
    /// as JSONL; `.pq` is treated as Parquet.
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
            _ => Err(IngestError::UnknownFormat(path.display().to_string())),
        }
    }
}

/// A source file read into memory: its Arrow schema and every record batch.
#[derive(Debug)]
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

    let ingest_err = |detail: String| IngestError::Read {
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

    let ingest_err = |detail: String| IngestError::Read {
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

    let ingest_err = |detail: String| IngestError::Read {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_temp(name: &str, contents: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut file = File::create(dir.path().join(name)).expect("create");
        file.write_all(contents.as_bytes()).expect("write");
        dir
    }

    #[test]
    fn infers_csv_column_types() {
        let dir = write_temp("rows.csv", "id,name,score\n1,alpha,0.9\n2,beta,0.7\n");
        let ingested = read(&dir.path().join("rows.csv")).expect("read csv");
        assert_eq!(ingested.row_count(), 2);
        let names: Vec<&str> = ingested
            .schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, vec!["id", "name", "score"]);
        assert_eq!(
            ingested.schema.field(0).data_type(),
            &arrow_schema::DataType::Int64
        );
        assert_eq!(
            ingested.schema.field(2).data_type(),
            &arrow_schema::DataType::Float64
        );
    }

    #[test]
    fn infers_jsonl_schema() {
        let dir = write_temp(
            "docs.jsonl",
            "{\"id\":1,\"text\":\"a\"}\n{\"id\":2,\"text\":\"b\"}\n",
        );
        let ingested = read(&dir.path().join("docs.jsonl")).expect("read jsonl");
        assert_eq!(ingested.row_count(), 2);
        let names: Vec<&str> = ingested
            .schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"text"));
    }

    #[test]
    fn unknown_extension_is_rejected() {
        let dir = write_temp("data.tsv", "id\tname\n1\ta\n");
        let err = read(&dir.path().join("data.tsv")).expect_err("must reject");
        assert!(matches!(err, IngestError::UnknownFormat(_)));
    }

    #[test]
    fn json_and_ndjson_and_pq_extensions_map_to_known_formats() {
        assert_eq!(
            Format::from_path(Path::new("a.json")).expect("json"),
            Format::Jsonl
        );
        assert_eq!(
            Format::from_path(Path::new("a.ndjson")).expect("ndjson"),
            Format::Jsonl
        );
        assert_eq!(
            Format::from_path(Path::new("a.pq")).expect("pq"),
            Format::Parquet
        );
    }
}
