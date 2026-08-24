//! Iceberg snapshot materialization for verified Durable Object offload batches.
//!
//! This module turns relational INSERT mutations into immutable Parquet data files,
//! writes a manifest and manifest list, then atomically advances the catalog pointer
//! with Iceberg's table requirements. The deterministic materialization identity is
//! the retry fence: an exact retry verifies the existing snapshot, while a different
//! batch cannot reuse its data-file names.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use iceberg::arrow::{RecordBatchPartitionSplitter, schema_to_arrow_schema};
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, MAIN_BRANCH,
    ManifestFile, ManifestListWriter, ManifestStatus, ManifestWriter, ManifestWriterBuilder,
    Operation, Snapshot, SnapshotReference, SnapshotRetention, Struct, Summary,
};
use iceberg::{Catalog, TableCommit, TableIdent, TableRequirement, TableUpdate};
use parquet::arrow::ArrowWriter;
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::lakehouse::{PublicationAuthorization, StorageBinding};
use crate::offload::{OffloadBatch, OffloadBatchArchive, OffloadBatchReceipt};
use crate::transaction::{MutationDomain, MutationKind, TableId, TransactionEnvelope};

const COMMIT_ID_PROPERTY: &str = "verglas.materialization-id";
const DO_ID_PROPERTY: &str = "verglas.do-id";
const TIMELINE_PROPERTY: &str = "verglas.timeline";
const COMMIT_SEQUENCE_PROPERTY: &str = "verglas.commit-sequence";
const RANGE_START_PROPERTY: &str = "verglas.commit-range-start";
const RANGE_END_PROPERTY: &str = "verglas.commit-range-end";
const SCHEMA_DIGEST_PROPERTY: &str = "verglas.schema-digest";
const VAMANA_THROUGH_PROPERTY: &str = "verglas.vamana-through";
const GRAPH_THROUGH_PROPERTY: &str = "verglas.graph-through";
const COMMIT_ATTEMPTS: usize = 8;

/// Serializes create-or-verify publication within this process; content-addressed paths
/// make the same invariant hold across independent writers.
static OBJECT_WRITE_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

/// Index watermarks reflected by one Iceberg snapshot summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IcebergIndexCoverage {
    vamana_through: u64,
    graph_through: u64,
}

impl IcebergIndexCoverage {
    /// Creates index coverage for one snapshot.
    pub fn new(vamana_through: u64, graph_through: u64) -> Self {
        Self {
            vamana_through,
            graph_through,
        }
    }

    /// Creates coverage for a snapshot without published index artifacts.
    pub fn none() -> Self {
        Self::new(0, 0)
    }

    /// Returns the transaction sequence covered by the Vamana artifact.
    pub fn vamana_through(self) -> u64 {
        self.vamana_through
    }

    /// Returns the transaction sequence covered by the graph artifact.
    pub fn graph_through(self) -> u64 {
        self.graph_through
    }
}

/// Verified identity of one committed Iceberg snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcebergCommitReceipt {
    metadata_location: String,
    snapshot_id: i64,
    materialization_id: String,
    from_sequence: u64,
    through_sequence: u64,
    parquet_files: Vec<String>,
}

impl IcebergCommitReceipt {
    /// Returns the catalog metadata location read back after the commit.
    pub fn metadata_location(&self) -> &str {
        &self.metadata_location
    }

    /// Returns the deterministic snapshot ID committed to the table.
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    /// Returns the retry-stable materialization identity.
    pub fn materialization_id(&self) -> &str {
        &self.materialization_id
    }

    /// Returns the first transaction sequence represented by the snapshot.
    pub fn from_sequence(&self) -> u64 {
        self.from_sequence
    }

    /// Returns the final transaction sequence represented by the snapshot.
    pub fn through_sequence(&self) -> u64 {
        self.through_sequence
    }

    /// Returns the Parquet files covered by the committed manifest.
    pub fn parquet_files(&self) -> &[String] {
        &self.parquet_files
    }
}

/// Catalog-backed Iceberg materializer for one Durable Object and table.
pub struct IcebergCommitter {
    catalog: Arc<dyn Catalog>,
    table: TableIdent,
    do_id: String,
}

impl IcebergCommitter {
    /// Binds one Durable Object to an existing Iceberg table and catalog.
    pub fn new(catalog: Arc<dyn Catalog>, table: TableIdent, do_id: impl Into<String>) -> Self {
        Self {
            catalog,
            table,
            do_id: do_id.into(),
        }
    }

    /// Returns the table identifier used for every CAS commit.
    pub fn table_identifier(&self) -> &TableIdent {
        &self.table
    }

    /// Rejects direct publication without the LakehouseObject authorization boundary.
    pub async fn commit_batch(&self, _batch: &OffloadBatch) -> Result<IcebergCommitReceipt> {
        Err(Error::Materialization(
            "Iceberg commits require LakehouseObject authorization".to_owned(),
        ))
    }

    /// Rejects direct publication without the LakehouseObject authorization boundary.
    pub async fn commit_batch_with_coverage(
        &self,
        _batch: &OffloadBatch,
        _coverage: IcebergIndexCoverage,
    ) -> Result<IcebergCommitReceipt> {
        Err(Error::Materialization(
            "Iceberg commits require LakehouseObject authorization".to_owned(),
        ))
    }

    /// Commits after the LakehouseObject has checked ownership and invocation authority.
    pub(crate) async fn commit_batch_authorized(
        &self,
        batch: &OffloadBatch,
        coverage: IcebergIndexCoverage,
        binding: StorageBinding,
        authorization: PublicationAuthorization,
        authorized_do_id: &str,
    ) -> Result<IcebergCommitReceipt> {
        if binding == StorageBinding::Customer
            && authorization != PublicationAuthorization::Explicit
        {
            return Err(Error::Materialization(
                "customer storage publication requires explicit invocation".to_owned(),
            ));
        }
        if authorized_do_id != self.do_id {
            return Err(Error::WrongDo {
                expected: self.do_id.clone(),
                actual: authorized_do_id.to_owned(),
            });
        }
        if coverage.vamana_through() > batch.through_sequence()
            || coverage.graph_through() > batch.through_sequence()
        {
            return Err(Error::Materialization(
                "index coverage cannot exceed the materialized commit range".to_owned(),
            ));
        }
        let materialization_id = self.materialization_id(batch);
        let mut attempt = 0_usize;
        loop {
            attempt = attempt.saturating_add(1);
            let table = self
                .catalog
                .load_table(&self.table)
                .await
                .map_err(iceberg_error)?;
            if table.metadata().format_version() != FormatVersion::V2 {
                // Extension point: add a dedicated materializer for Iceberg V1/V3.
                return Err(Error::Materialization(
                    "Iceberg lake materialization currently supports format V2 only".to_owned(),
                ));
            }
            if !table
                .metadata()
                .default_partition_spec()
                .fields()
                .is_empty()
            {
                // Extension point: add partition-key fan-out with complete partition metadata.
                return Err(Error::Materialization(
                    "Iceberg lake materialization currently rejects partitioned tables".to_owned(),
                ));
            }
            self.ensure_non_overlapping(&table, batch, &materialization_id)?;
            let prepared = self.prepare_files(&table, batch).await?;
            let properties =
                self.summary_properties(&table, batch, coverage, &materialization_id)?;
            let artifact_digest = prepared_digest(&prepared);

            for file in &prepared {
                write_verified_file(table.file_io(), &file.path, &file.bytes).await?;
            }

            if let Some(snapshot) = table.metadata().snapshots().find(|snapshot| {
                snapshot
                    .summary()
                    .additional_properties
                    .get(COMMIT_ID_PROPERTY)
                    == Some(&materialization_id)
            }) {
                let properties = self
                    .snapshot_properties(&table, snapshot, properties)
                    .await?;
                return self
                    .verify_snapshot(&table, snapshot, &prepared, &properties, None)
                    .await;
            }

            let snapshot_id = snapshot_id(&materialization_id);
            let sequence_number = table.metadata().next_sequence_number();
            let manifest = self
                .write_manifest(
                    &table,
                    &prepared,
                    snapshot_id,
                    sequence_number,
                    &artifact_digest,
                )
                .await?;
            let (manifest_list_path, expected_manifest_paths) = self
                .write_manifest_list(
                    &table,
                    manifest,
                    snapshot_id,
                    sequence_number,
                    &artifact_digest,
                )
                .await?;
            let timestamp_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|error| Error::Materialization(error.to_string()))?
                .as_millis();
            let timestamp_ms = i64::try_from(timestamp_ms)
                .map_err(|_| Error::Materialization("snapshot timestamp exceeds i64".to_owned()))?;
            let snapshot = Snapshot::builder()
                .with_snapshot_id(snapshot_id)
                .with_parent_snapshot_id(table.metadata().current_snapshot_id())
                .with_sequence_number(sequence_number)
                .with_timestamp_ms(timestamp_ms)
                .with_manifest_list(manifest_list_path)
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: properties.clone(),
                })
                .with_schema_id(table.metadata().current_schema_id())
                .build();
            let updates = vec![
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_owned(),
                    reference: SnapshotReference::new(
                        snapshot_id,
                        SnapshotRetention::branch(None, None, None),
                    ),
                },
            ];
            let requirements = vec![
                TableRequirement::UuidMatch {
                    uuid: table.metadata().uuid(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_owned(),
                    snapshot_id: table.metadata().current_snapshot_id(),
                },
            ];
            let commit = TableCommit::from_parts(table.identifier().clone(), requirements, updates);
            match self.catalog.update_table(commit).await {
                Ok(_) => {
                    let committed = self
                        .catalog
                        .load_table(&self.table)
                        .await
                        .map_err(iceberg_error)?;
                    let snapshot = committed
                        .metadata()
                        .snapshot_by_id(snapshot_id)
                        .ok_or_else(|| {
                            Error::Materialization(format!(
                                "catalog commit returned without snapshot {snapshot_id}"
                            ))
                        })?;
                    return self
                        .verify_snapshot(
                            &committed,
                            snapshot,
                            &prepared,
                            &properties,
                            Some(&expected_manifest_paths),
                        )
                        .await;
                }
                Err(error) if error.kind() != iceberg::ErrorKind::CatalogCommitConflicts => {
                    return Err(iceberg_error(error));
                }
                Err(error) => {
                    let refreshed = self
                        .catalog
                        .load_table(&self.table)
                        .await
                        .map_err(iceberg_error)?;
                    if let Some(snapshot) = refreshed.metadata().snapshots().find(|snapshot| {
                        snapshot
                            .summary()
                            .additional_properties
                            .get(COMMIT_ID_PROPERTY)
                            == Some(&materialization_id)
                    }) {
                        let properties = self
                            .snapshot_properties(&refreshed, snapshot, properties)
                            .await?;
                        return self
                            .verify_snapshot(&refreshed, snapshot, &prepared, &properties, None)
                            .await;
                    }
                    if attempt >= COMMIT_ATTEMPTS {
                        return Err(iceberg_error(error));
                    }
                }
            }
        }
    }

    /// Materializes relational INSERT mutations into schema-bound Parquet files.
    async fn prepare_files(
        &self,
        table: &iceberg::table::Table,
        batch: &OffloadBatch,
    ) -> Result<Vec<PreparedFile>> {
        let table_id = TableId::new(self.table.name().to_owned());
        let path_prefix = format!(
            "{}/data/verglas/{}",
            table.metadata().location().trim_end_matches('/'),
            short_hash(&format!("{}:{}", self.do_id, self.table))
        );
        let iceberg_schema = table.metadata().current_schema().clone();
        let arrow_schema =
            Arc::new(schema_to_arrow_schema(&iceberg_schema).map_err(iceberg_error)?);
        let partition_spec = table.metadata().default_partition_spec();
        let splitter = if partition_spec.fields().is_empty() {
            None
        } else {
            Some(
                RecordBatchPartitionSplitter::try_new_with_computed_values(
                    iceberg_schema,
                    partition_spec.clone(),
                )
                .map_err(iceberg_error)?,
            )
        };
        let mut prepared = Vec::new();
        let mut mutation_index = 0_usize;
        for transaction in batch.transactions() {
            if transaction.do_id() != self.do_id {
                return Err(Error::WrongDo {
                    expected: self.do_id.clone(),
                    actual: transaction.do_id().to_owned(),
                });
            }
            let envelope =
                TransactionEnvelope::from_canonical_bytes(transaction.canonical_envelope())?;
            if envelope.do_id() != self.do_id {
                return Err(Error::WrongDo {
                    expected: self.do_id.clone(),
                    actual: envelope.do_id().to_owned(),
                });
            }
            if envelope.transaction_id() != transaction.transaction_id() {
                return Err(Error::Materialization(
                    "offload transaction identity does not match its canonical envelope".to_owned(),
                ));
            }
            for mutation in envelope.mutations() {
                if mutation.domain() != MutationDomain::Relational {
                    // Extension point: vector and graph snapshots need domain-specific lake files.
                    return Err(Error::Materialization(format!(
                        "Iceberg lake materialization rejects mutation domain {:?}",
                        mutation.domain()
                    )));
                }
                if mutation.table() != &table_id {
                    continue;
                }
                if mutation.kind() != MutationKind::Insert {
                    // Extension point: Replace/Upsert require a complete relational snapshot rewrite.
                    return Err(Error::Materialization(format!(
                        "Iceberg lake materialization supports only relational INSERT mutations; got {:?}",
                        mutation.kind()
                    )));
                }
                let batch = coerce_batch(mutation.batch(), arrow_schema.clone(), &self.table)?;
                let partitioned = if let Some(splitter) = &splitter {
                    let mut partitioned = splitter.split(&batch).map_err(iceberg_error)?;
                    partitioned.sort_by_key(|(key, _)| key.to_path());
                    partitioned
                        .into_iter()
                        .map(|(key, batch)| (key.data().clone(), batch))
                        .collect::<Vec<_>>()
                } else {
                    vec![(Struct::empty(), batch)]
                };
                for (partition_index, (partition, batch)) in partitioned.into_iter().enumerate() {
                    let bytes = parquet_bytes(&batch)?;
                    let content_identity = short_hash_bytes(&bytes);
                    let file_path = format!(
                        "{path_prefix}/{:020}-{}-{mutation_index}-{partition_index}-{content_identity}.parquet",
                        transaction.commit_sequence(),
                        transaction.transaction_id()
                    );
                    let record_count = u64::try_from(batch.num_rows())
                        .map_err(|_| Error::Materialization("row count exceeds u64".to_owned()))?;
                    let file_size = u64::try_from(bytes.len()).map_err(|_| {
                        Error::Materialization("Parquet size exceeds u64".to_owned())
                    })?;
                    let data_file = DataFileBuilder::default()
                        .content(DataContentType::Data)
                        .file_path(file_path.clone())
                        .file_format(DataFileFormat::Parquet)
                        .partition(partition)
                        .record_count(record_count)
                        .file_size_in_bytes(file_size)
                        .partition_spec_id(table.metadata().default_partition_spec_id())
                        .build()
                        .map_err(iceberg_error)?;
                    prepared.push(PreparedFile {
                        path: file_path,
                        bytes,
                        data_file,
                    });
                }
                mutation_index = mutation_index.saturating_add(1);
            }
        }
        if prepared.is_empty() {
            return Err(Error::Materialization(format!(
                "offload batch contains no relational INSERT rows for {}",
                self.table
            )));
        }
        Ok(prepared)
    }

    /// Derives the retry identity from the table, range, and transaction identities.
    fn materialization_id(&self, batch: &OffloadBatch) -> String {
        let mut digest = Sha256::new();
        digest.update(self.do_id.as_bytes());
        digest.update(self.table.to_string().as_bytes());
        digest.update(batch.from_sequence().to_le_bytes());
        digest.update(batch.through_sequence().to_le_bytes());
        for transaction in batch.transactions() {
            digest.update(transaction.commit_sequence().to_le_bytes());
            digest.update(transaction.transaction_id().as_bytes());
        }
        hex::encode(digest.finalize())
    }

    /// Rejects a regrouped batch that would duplicate an already committed range.
    fn ensure_non_overlapping(
        &self,
        table: &iceberg::table::Table,
        batch: &OffloadBatch,
        materialization_id: &str,
    ) -> Result<()> {
        for snapshot in table.metadata().snapshots() {
            let properties = &snapshot.summary().additional_properties;
            if properties
                .get(COMMIT_ID_PROPERTY)
                .is_some_and(|value| value == materialization_id)
                || properties.get(DO_ID_PROPERTY) != Some(&self.do_id)
            {
                continue;
            }
            let Some(start) = properties
                .get(RANGE_START_PROPERTY)
                .and_then(|value| value.parse::<u64>().ok())
            else {
                continue;
            };
            let Some(end) = properties
                .get(RANGE_END_PROPERTY)
                .and_then(|value| value.parse::<u64>().ok())
            else {
                continue;
            };
            if start <= batch.through_sequence() && batch.from_sequence() <= end {
                return Err(Error::Materialization(format!(
                    "materialization range {}..={} overlaps committed range {start}..={end}",
                    batch.from_sequence(),
                    batch.through_sequence()
                )));
            }
        }
        Ok(())
    }

    /// Builds the immutable summary properties for one materialization range.
    fn summary_properties(
        &self,
        table: &iceberg::table::Table,
        batch: &OffloadBatch,
        coverage: IcebergIndexCoverage,
        materialization_id: &str,
    ) -> Result<HashMap<String, String>> {
        let schema_digest = schema_digest(table.metadata().current_schema())?;
        Ok(HashMap::from([
            (COMMIT_ID_PROPERTY.to_owned(), materialization_id.to_owned()),
            (DO_ID_PROPERTY.to_owned(), self.do_id.clone()),
            (TIMELINE_PROPERTY.to_owned(), self.do_id.clone()),
            (
                COMMIT_SEQUENCE_PROPERTY.to_owned(),
                batch.through_sequence().to_string(),
            ),
            (
                RANGE_START_PROPERTY.to_owned(),
                batch.from_sequence().to_string(),
            ),
            (
                RANGE_END_PROPERTY.to_owned(),
                batch.through_sequence().to_string(),
            ),
            (
                SCHEMA_DIGEST_PROPERTY.to_owned(),
                hex::encode(schema_digest),
            ),
            (
                VAMANA_THROUGH_PROPERTY.to_owned(),
                coverage.vamana_through().to_string(),
            ),
            (
                GRAPH_THROUGH_PROPERTY.to_owned(),
                coverage.graph_through().to_string(),
            ),
        ]))
    }

    /// Recomputes verification properties against the snapshot's own schema.
    async fn snapshot_properties(
        &self,
        table: &iceberg::table::Table,
        snapshot: &iceberg::spec::SnapshotRef,
        mut properties: HashMap<String, String>,
    ) -> Result<HashMap<String, String>> {
        let schema = snapshot.schema(table.metadata()).map_err(iceberg_error)?;
        properties.insert(
            SCHEMA_DIGEST_PROPERTY.to_owned(),
            hex::encode(schema_digest(&schema)?),
        );
        Ok(properties)
    }

    /// Writes the manifest entries to a create-only, content-addressed path.
    async fn write_manifest(
        &self,
        table: &iceberg::table::Table,
        prepared: &[PreparedFile],
        snapshot_id: i64,
        sequence_number: i64,
        artifact_digest: &str,
    ) -> Result<ManifestFile> {
        let temporary_path =
            format!("memory://verglas-manifest-{snapshot_id}-{artifact_digest}.avro");
        let memory_io = iceberg::io::FileIO::new_with_memory();
        let builder = ManifestWriterBuilder::new(
            memory_io
                .new_output(&temporary_path)
                .map_err(iceberg_error)?,
            Some(snapshot_id),
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
        );
        let mut writer: ManifestWriter = builder.build_v2_data();
        for file in prepared {
            writer
                .add_file(file.data_file.clone(), sequence_number)
                .map_err(iceberg_error)?;
        }
        let mut manifest = writer.write_manifest_file().await.map_err(iceberg_error)?;
        let bytes = memory_io
            .new_input(&temporary_path)
            .map_err(iceberg_error)?
            .read()
            .await
            .map_err(iceberg_error)?;
        let path = format!(
            "{}/metadata/verglas-{snapshot_id}-{artifact_digest}-{}.manifest.avro",
            table.metadata().location().trim_end_matches('/'),
            short_hash_bytes(&bytes)
        );
        manifest.manifest_path = path.clone();
        write_verified_file(table.file_io(), &path, &bytes).await?;
        Ok(manifest)
    }

    /// Writes the manifest list to a create-only, content-addressed path.
    async fn write_manifest_list(
        &self,
        table: &iceberg::table::Table,
        manifest: ManifestFile,
        snapshot_id: i64,
        sequence_number: i64,
        artifact_digest: &str,
    ) -> Result<(String, Vec<String>)> {
        let existing = if let Some(current) = table.metadata().current_snapshot() {
            table
                .manifest_list_reader(current)
                .load()
                .await
                .map_err(iceberg_error)?
                .entries()
                .to_vec()
        } else {
            Vec::new()
        };
        let mut manifests = existing;
        manifests.push(manifest);
        let expected_paths = manifests
            .iter()
            .map(|manifest| manifest.manifest_path.clone())
            .collect::<Vec<_>>();
        let temporary_path =
            format!("memory://verglas-manifest-list-{snapshot_id}-{artifact_digest}.avro");
        let memory_io = iceberg::io::FileIO::new_with_memory();
        let output = memory_io
            .new_output(&temporary_path)
            .map_err(iceberg_error)?;
        let output_writer = output.writer().await.map_err(iceberg_error)?;
        let mut writer = ManifestListWriter::v2(
            output_writer,
            snapshot_id,
            table.metadata().current_snapshot_id(),
            sequence_number,
        );
        writer
            .add_manifests(manifests.into_iter())
            .map_err(iceberg_error)?;
        writer.close().await.map_err(iceberg_error)?;
        let bytes = memory_io
            .new_input(&temporary_path)
            .map_err(iceberg_error)?
            .read()
            .await
            .map_err(iceberg_error)?;
        let path = format!(
            "{}/metadata/verglas-{snapshot_id}-{artifact_digest}-{}.manifest-list.avro",
            table.metadata().location().trim_end_matches('/'),
            short_hash_bytes(&bytes)
        );
        write_verified_file(table.file_io(), &path, &bytes).await?;
        Ok((path, expected_paths))
    }

    /// Reads back metadata, manifest entries, and Parquet bytes before acknowledging.
    async fn verify_snapshot(
        &self,
        table: &iceberg::table::Table,
        snapshot: &iceberg::spec::SnapshotRef,
        prepared: &[PreparedFile],
        properties: &HashMap<String, String>,
        expected_manifest_paths: Option<&[String]>,
    ) -> Result<IcebergCommitReceipt> {
        for (key, expected) in properties {
            if snapshot.summary().additional_properties.get(key) != Some(expected) {
                return Err(Error::Materialization(format!(
                    "committed snapshot summary property {key} does not match"
                )));
            }
        }
        let manifest_list = table
            .manifest_list_reader(snapshot)
            .load()
            .await
            .map_err(iceberg_error)?;
        let mut actual_manifest_paths = manifest_list
            .entries()
            .iter()
            .map(|manifest| manifest.manifest_path.clone())
            .collect::<Vec<_>>();
        if let Some(expected) = expected_manifest_paths {
            let mut expected = expected.to_vec();
            actual_manifest_paths.sort_unstable();
            expected.sort_unstable();
            if actual_manifest_paths != expected {
                return Err(Error::Materialization(
                    "committed manifest list does not match the planned snapshot".to_owned(),
                ));
            }
        }
        let mut path_counts = HashMap::new();
        let mut manifest_files = HashMap::new();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .map_err(iceberg_error)?;
            for entry in manifest.entries() {
                if entry.status() != ManifestStatus::Deleted && entry.is_alive() {
                    let path = entry.file_path().to_owned();
                    let count = path_counts.entry(path.clone()).or_insert(0_usize);
                    *count = count.saturating_add(1);
                    if *count > 1 {
                        return Err(Error::Materialization(format!(
                            "committed manifest references {path} more than once"
                        )));
                    }
                    manifest_files.insert(path, entry.data_file().clone());
                }
            }
        }
        let prepared_by_path = prepared
            .iter()
            .map(|file| (file.path.as_str(), file))
            .collect::<HashMap<_, _>>();
        for file in prepared {
            if path_counts.get(&file.path) != Some(&1)
                || manifest_files.get(&file.path) != Some(&file.data_file)
            {
                return Err(Error::Materialization(format!(
                    "committed manifest metadata does not match {}",
                    file.path
                )));
            }
        }
        for (path, data_file) in &manifest_files {
            let actual = table
                .file_io()
                .new_input(path)
                .map_err(iceberg_error)?
                .read()
                .await
                .map_err(iceberg_error)?;
            let actual_size = u64::try_from(actual.len())
                .map_err(|_| Error::Materialization(format!("data file is too large: {path}")))?;
            if actual_size != data_file.file_size_in_bytes() {
                return Err(Error::Materialization(format!(
                    "committed data-file size does not match {path}"
                )));
            }
            if let Some(file) = prepared_by_path.get(path.as_str())
                && (actual.as_ref() != file.bytes.as_slice() || data_file != &file.data_file)
            {
                return Err(Error::Materialization(format!(
                    "committed Parquet verification mismatch for {path}"
                )));
            }
        }
        let metadata_location = table
            .metadata_location()
            .ok_or_else(|| {
                Error::Materialization("committed table has no metadata location".to_owned())
            })?
            .to_owned();
        Ok(IcebergCommitReceipt {
            metadata_location,
            snapshot_id: snapshot.snapshot_id(),
            materialization_id: properties.get(COMMIT_ID_PROPERTY).cloned().ok_or_else(|| {
                Error::Materialization("missing materialization identity".to_owned())
            })?,
            from_sequence: properties
                .get(RANGE_START_PROPERTY)
                .and_then(|value| value.parse().ok())
                .ok_or_else(|| Error::Materialization("invalid commit-range-start".to_owned()))?,
            through_sequence: properties
                .get(RANGE_END_PROPERTY)
                .and_then(|value| value.parse().ok())
                .ok_or_else(|| Error::Materialization("invalid commit-range-end".to_owned()))?,
            parquet_files: prepared.iter().map(|file| file.path.clone()).collect(),
        })
    }
}

/// Immutable Parquet data staged for one Iceberg snapshot.
struct PreparedFile {
    path: String,
    bytes: Vec<u8>,
    data_file: DataFile,
}

/// Archive adapter that advances an offload watermark only after Iceberg verification.
pub struct VerifiedIcebergArchive {
    archive: Arc<dyn OffloadBatchArchive>,
    committer: Arc<IcebergCommitter>,
    binding: StorageBinding,
    authorization: PublicationAuthorization,
    do_id: String,
    coverage: IcebergIndexCoverage,
}

impl VerifiedIcebergArchive {
    /// Combines an archive sink with an Iceberg commit and authorization fence.
    pub fn new(
        archive: Arc<dyn OffloadBatchArchive>,
        committer: Arc<IcebergCommitter>,
        binding: StorageBinding,
        authorization: PublicationAuthorization,
        do_id: String,
    ) -> Self {
        Self::new_with_coverage(
            archive,
            committer,
            binding,
            authorization,
            do_id,
            IcebergIndexCoverage::none(),
        )
    }

    /// Combines an archive sink with a verified commit carrying index coverage.
    pub fn new_with_coverage(
        archive: Arc<dyn OffloadBatchArchive>,
        committer: Arc<IcebergCommitter>,
        binding: StorageBinding,
        authorization: PublicationAuthorization,
        do_id: String,
        coverage: IcebergIndexCoverage,
    ) -> Self {
        Self {
            archive,
            committer,
            binding,
            authorization,
            do_id,
            coverage,
        }
    }
}

#[async_trait]
impl OffloadBatchArchive for VerifiedIcebergArchive {
    /// Archives the shared batch, then commits and reads back its Iceberg snapshot.
    async fn archive(&self, batch: &OffloadBatch) -> Result<OffloadBatchReceipt> {
        if self.binding == StorageBinding::Customer
            && self.authorization != PublicationAuthorization::Explicit
        {
            return Err(Error::Materialization(
                "customer storage publication requires explicit invocation".to_owned(),
            ));
        }
        let first = batch.transactions().first().ok_or_else(|| {
            Error::Materialization("cannot materialize an empty batch".to_owned())
        })?;
        if first.do_id() != self.do_id {
            return Err(Error::WrongDo {
                expected: self.do_id.clone(),
                actual: first.do_id().to_owned(),
            });
        }
        let receipt = self.archive.archive(batch).await?;
        self.committer
            .commit_batch_authorized(
                batch,
                self.coverage,
                self.binding,
                self.authorization,
                &self.do_id,
            )
            .await?;
        Ok(receipt)
    }
}

/// Writes one immutable object and verifies exact retry identity.
async fn write_verified_file(
    file_io: &iceberg::io::FileIO,
    path: &str,
    bytes: &[u8],
) -> Result<()> {
    let _guard = OBJECT_WRITE_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    let input = file_io.new_input(path).map_err(iceberg_error)?;
    if input.exists().await.map_err(iceberg_error)? {
        let actual = input.read().await.map_err(iceberg_error)?;
        if actual.as_ref() != bytes {
            return Err(Error::Materialization(format!(
                "immutable object conflict at {path}"
            )));
        }
        return Ok(());
    }
    file_io
        .new_output(path)
        .map_err(iceberg_error)?
        .write(Bytes::copy_from_slice(bytes))
        .await
        .map_err(iceberg_error)?;
    let actual = file_io
        .new_input(path)
        .map_err(iceberg_error)?
        .read()
        .await
        .map_err(iceberg_error)?;
    if actual.as_ref() != bytes {
        return Err(Error::Materialization(format!(
            "immutable object verification mismatch at {path}"
        )));
    }
    Ok(())
}

/// Reorders one mutation into the loaded Iceberg schema and preserves field IDs.
fn coerce_batch(
    batch: &RecordBatch,
    target: Arc<arrow_schema::Schema>,
    table: &TableIdent,
) -> Result<RecordBatch> {
    if batch.schema().fields().len() != target.fields().len() {
        // Extension point: add explicit schema evolution and coercion rules.
        return Err(Error::Materialization(format!(
            "source schema column count does not exactly match Iceberg table {}",
            table
        )));
    }
    let source_schema = batch.schema();
    let mut columns = Vec::with_capacity(target.fields().len());
    for (index, field) in target.fields().iter().enumerate() {
        let source_field = source_schema.field(index);
        if source_field.name() != field.name()
            || source_field.data_type() != field.data_type()
            || source_field.is_nullable() != field.is_nullable()
        {
            // Extension point: add explicit schema evolution and coercion rules.
            return Err(Error::Materialization(format!(
                "source schema does not exactly match Iceberg column {} in table {}",
                field.name(),
                table
            )));
        }
        let source = batch.column(index);
        if !field.is_nullable() && source.null_count() > 0 {
            return Err(Error::Materialization(format!(
                "required Iceberg column {} contains nulls",
                field.name()
            )));
        }
        columns.push(source.clone());
    }
    RecordBatch::try_new(target, columns).map_err(|error| Error::Materialization(error.to_string()))
}

/// Returns the content identity used to isolate immutable manifest paths.
fn prepared_digest(prepared: &[PreparedFile]) -> String {
    let mut digest = Sha256::new();
    for file in prepared {
        digest.update(file.path.as_bytes());
        digest.update(&file.bytes);
    }
    hex::encode(digest.finalize())
}

/// Returns a deterministic digest of serialized schema semantics.
fn schema_digest(schema: &iceberg::spec::Schema) -> Result<[u8; 32]> {
    let mut digest = Sha256::new();
    let serialized = serde_json::to_vec(schema.as_struct())
        .map_err(|error| Error::Materialization(error.to_string()))?;
    digest.update(serialized);
    digest.update(schema.schema_id().to_le_bytes());
    let mut identifiers = schema.identifier_field_ids().collect::<Vec<_>>();
    identifiers.sort_unstable();
    for identifier in identifiers {
        digest.update(identifier.to_le_bytes());
    }
    Ok(digest.finalize().into())
}

/// Encodes one Arrow mutation as canonical Parquet bytes.
fn parquet_bytes(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), None)
        .map_err(|error| Error::Materialization(error.to_string()))?;
    writer
        .write(batch)
        .map_err(|error| Error::Materialization(error.to_string()))?;
    writer
        .into_inner()
        .map_err(|error| Error::Materialization(error.to_string()))
}

/// Returns a short deterministic identity suitable for an object path.
fn short_hash(value: &str) -> String {
    hex::encode(&Sha256::digest(value.as_bytes())[..12])
}

/// Returns a short deterministic identity for immutable bytes.
fn short_hash_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Derives a positive deterministic snapshot ID from the materialization identity.
fn snapshot_id(materialization_id: &str) -> i64 {
    let digest = Sha256::digest(materialization_id.as_bytes());
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    let id = i64::from_le_bytes(bytes) & i64::MAX;
    if id == 0 { 1 } else { id }
}

/// Converts an Iceberg error into the engine's materialization error.
fn iceberg_error(error: impl std::fmt::Display) -> Error {
    Error::Materialization(error.to_string())
}

impl Clone for IcebergCommitter {
    /// Clones the catalog handle and immutable table identity for archive adapters.
    fn clone(&self) -> Self {
        Self {
            catalog: self.catalog.clone(),
            table: self.table.clone(),
            do_id: self.do_id.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};

    use super::*;

    async fn test_table() -> (Arc<dyn Catalog>, TableIdent) {
        let catalog = Arc::new(
            MemoryCatalogBuilder::default()
                .load(
                    "test",
                    HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_owned(),
                        "memory://warehouse".to_owned(),
                    )]),
                )
                .await
                .expect("memory catalog"),
        );
        let namespace = NamespaceIdent::new("managed".to_owned());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("namespace");
        let schema = iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(
            &arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "value",
                arrow_schema::DataType::Int64,
                false,
            )]),
        )
        .expect("schema");
        let table = TableIdent::new(namespace, "events".to_owned());
        catalog
            .create_table(
                table.namespace(),
                TableCreation::builder()
                    .name(table.name().to_owned())
                    .location("memory://warehouse/managed/events".to_owned())
                    .schema(schema)
                    .build(),
            )
            .await
            .expect("table");
        (catalog, table)
    }

    #[tokio::test]
    async fn conflicting_data_bytes_are_not_overwritten() {
        let file_io = iceberg::io::FileIO::new_with_memory();
        let path = "memory://warehouse/managed/events/data.parquet";
        write_verified_file(&file_io, path, &[1, 2, 3])
            .await
            .expect("first data object");
        let error = write_verified_file(&file_io, path, &[4, 5, 6])
            .await
            .expect_err("conflicting data object");
        assert!(error.to_string().contains("immutable object conflict"));
        let actual = file_io
            .new_input(path)
            .expect("data input")
            .read()
            .await
            .expect("data bytes");
        assert_eq!(actual.as_ref(), &[1, 2, 3]);
    }

    #[tokio::test]
    async fn conflicting_manifest_content_uses_a_distinct_immutable_path() {
        let (catalog, table_ident) = test_table().await;
        let committer = IcebergCommitter::new(catalog.clone(), table_ident, "lake-1");
        let table = catalog
            .load_table(committer.table_identifier())
            .await
            .expect("table");
        let file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("memory://warehouse/managed/events/data.parquet".to_owned())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .record_count(1)
            .file_size_in_bytes(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .build()
            .expect("data file");
        let prepared = PreparedFile {
            path: "memory://warehouse/managed/events/data.parquet".to_owned(),
            bytes: vec![1],
            data_file: file.clone(),
        };
        let first_manifest = committer
            .write_manifest(&table, &[prepared], 11, 1, "fixed")
            .await
            .expect("first manifest");
        let conflicting = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("memory://warehouse/managed/events/data.parquet".to_owned())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .record_count(2)
            .file_size_in_bytes(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .build()
            .expect("conflicting data file");
        let second_manifest = committer
            .write_manifest(
                &table,
                &[PreparedFile {
                    path: "memory://warehouse/managed/events/data.parquet".to_owned(),
                    bytes: vec![1],
                    data_file: conflicting,
                }],
                11,
                1,
                "fixed",
            )
            .await
            .expect("conflicting content gets a distinct path");
        assert_ne!(first_manifest.manifest_path, second_manifest.manifest_path);
        let first_bytes = table
            .file_io()
            .new_input(&first_manifest.manifest_path)
            .expect("first manifest input")
            .read()
            .await
            .expect("first manifest bytes");
        let second_bytes = table
            .file_io()
            .new_input(&second_manifest.manifest_path)
            .expect("second manifest input")
            .read()
            .await
            .expect("second manifest bytes");
        assert_ne!(first_bytes, second_bytes);
        let (first_list, _) = committer
            .write_manifest_list(&table, first_manifest, 11, 1, "fixed")
            .await
            .expect("first manifest list");
        let (second_list, _) = committer
            .write_manifest_list(&table, second_manifest, 11, 1, "fixed")
            .await
            .expect("second manifest list");
        assert_ne!(first_list, second_list);
    }
}
