//! Snapshot-bound Iceberg attachment and discovery for Vamana Puffin indexes.
//!
//! The customer table owns the durable index file. Each `verglas-vamana-v1`
//! blob is stored through the table's `FileIO`, advertised by a
//! `StatisticsFile`, and committed through the catalog for the exact snapshot
//! it reflects. Local copies are ordinary object-cache entries, never a second
//! source of truth.

use std::collections::HashMap;

use iceberg::Catalog;
use iceberg::puffin::{Blob, CompressionCodec, PuffinReader, PuffinWriter};
use iceberg::spec::{BlobMetadata, StatisticsFile};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};

use crate::BLOB_TYPE;
use crate::error::{Result, VectorError};
use crate::maintenance::{IdEncoding, MaintenanceConfig};
use crate::vamana::{VamanaIndex, VamanaParams};
use crate::{StringKeyIndex, StringKeyMap};

const FIELD_PROPERTY: &str = "verglas.vector.field";
const ID_FIELD_PROPERTY: &str = "verglas.vector.id-field";
const ID_ENCODING_PROPERTY: &str = "verglas.vector.id-encoding";
const METRIC_PROPERTY: &str = "verglas.vector.metric";
const DIM_PROPERTY: &str = "verglas.vector.dim";
const R_PROPERTY: &str = "verglas.vector.r";
const L_PROPERTY: &str = "verglas.vector.l";
const ALPHA_PROPERTY: &str = "verglas.vector.alpha";
/// A sibling Puffin blob containing exact string-key to Vamana-id mappings.
pub const STRING_KEY_MAP_BLOB_TYPE: &str = "verglas-vamana-string-key-map-v1";

const COMMIT_ATTEMPTS: u32 = 8;

/// A decoded Vamana index and the build configuration stored with its blob.
#[derive(Debug)]
pub struct AttachedIndex {
    /// The decoded ANN index.
    pub index: VamanaIndex,
    /// The source-column and construction configuration needed to refresh it.
    pub config: MaintenanceConfig,
}

/// A decoded Vamana index plus the lossless caller-key map paired with it.
#[derive(Debug)]
pub struct AttachedStringKeyIndex {
    /// The index and exact-key bridge ready for semantic serving.
    pub bridge: StringKeyIndex,
    /// The source-column and construction configuration stored with the index.
    pub config: MaintenanceConfig,
}

/// Writes `index` into the table's metadata area and attaches the resulting
/// Puffin statistics file to `snapshot`. Existing blobs for other index fields
/// on the same snapshot are preserved in the replacement statistics file.
pub async fn attach_index(
    catalog: &dyn Catalog,
    table: &Table,
    snapshot: i64,
    config: &MaintenanceConfig,
    index: &VamanaIndex,
) -> Result<String> {
    attach_with_optional_key_map(catalog, table, snapshot, config, index, None).await
}

/// Writes an ANN graph and its exact arbitrary-string key map into one
/// snapshot-bound statistics file. The map is durable Iceberg/Puffin state,
/// not a process-local resource registry.
pub async fn attach_string_key_index(
    catalog: &dyn Catalog,
    table: &Table,
    snapshot: i64,
    config: &MaintenanceConfig,
    index: &VamanaIndex,
    keys: &StringKeyMap,
) -> Result<String> {
    attach_with_optional_key_map(catalog, table, snapshot, config, index, Some(keys)).await
}

/// Performs the common attachment write with an optional lossless key-map blob.
async fn attach_with_optional_key_map(
    catalog: &dyn Catalog,
    table: &Table,
    snapshot: i64,
    config: &MaintenanceConfig,
    index: &VamanaIndex,
    keys: Option<&StringKeyMap>,
) -> Result<String> {
    let sequence_number = table
        .metadata()
        .snapshot_by_id(snapshot)
        .ok_or_else(|| VectorError::Field(format!("snapshot {snapshot} is not in the table")))?
        .sequence_number();
    let path = format!(
        "{}/metadata/verglas-vamana-{}-{}.puffin",
        table.metadata().location(),
        snapshot,
        uuid::Uuid::new_v4()
    );
    let output = table.file_io().new_output(&path)?;
    let mut writer = PuffinWriter::new(&output, HashMap::new(), false).await?;

    if let Some(statistics) = table.metadata().statistics_for_snapshot(snapshot) {
        let input = table.file_io().new_input(&statistics.statistics_path)?;
        let reader = PuffinReader::new(input);
        let metadata = reader.file_metadata().await?.blobs().to_vec();
        for blob_metadata in &metadata {
            let same_vector_field = (blob_metadata.blob_type() == BLOB_TYPE
                || blob_metadata.blob_type() == STRING_KEY_MAP_BLOB_TYPE)
                && blob_metadata
                    .properties()
                    .get(FIELD_PROPERTY)
                    .is_some_and(|field| field == &config.vec_field);
            if !same_vector_field {
                let blob = reader.blob(blob_metadata).await?;
                writer.add(blob, blob_metadata.compression_codec()).await?;
            }
        }
    }

    let properties = properties_for(config, index);
    let blob = Blob::builder()
        .r#type(BLOB_TYPE.to_owned())
        .fields(Vec::new())
        .snapshot_id(snapshot)
        .sequence_number(sequence_number)
        .data(crate::codec::encode(index))
        .properties(properties)
        .build();
    writer.add(blob, CompressionCodec::Zstd).await?;
    if let Some(keys) = keys {
        let key_blob = Blob::builder()
            .r#type(STRING_KEY_MAP_BLOB_TYPE.to_owned())
            .fields(Vec::new())
            .snapshot_id(snapshot)
            .sequence_number(sequence_number)
            .data(keys.encode()?)
            .properties(HashMap::from([(
                FIELD_PROPERTY.to_owned(),
                config.vec_field.clone(),
            )]))
            .build();
        writer.add(key_blob, CompressionCodec::Zstd).await?;
    }
    writer.close().await?;

    let (file_size, footer_size, blob_metadata) = inspect_file(table, &path).await?;
    let statistics = StatisticsFile {
        snapshot_id: snapshot,
        statistics_path: path.clone(),
        file_size_in_bytes: file_size,
        file_footer_size_in_bytes: footer_size,
        key_metadata: None,
        blob_metadata,
    };
    commit_statistics(catalog, table, statistics).await?;
    Ok(path)
}

/// Loads the exact-key bridge paired with a Vamana index at `snapshot`.
/// Missing either sibling is an error because serving opaque ids would violate
/// the S3 Vectors caller-key contract.
pub async fn load_string_key_index_for_snapshot(
    table: &Table,
    snapshot: i64,
    field: &str,
) -> Result<Option<AttachedStringKeyIndex>> {
    let Some(attached) = load_index_for_snapshot(table, snapshot, field).await? else {
        return Ok(None);
    };
    let statistics = table
        .metadata()
        .statistics_for_snapshot(snapshot)
        .ok_or_else(|| {
            VectorError::CorruptBlob(format!("missing statistics for Vamana snapshot {snapshot}"))
        })?;
    let reader = PuffinReader::new(table.file_io().new_input(&statistics.statistics_path)?);
    let metadata = reader.file_metadata().await?.blobs().to_vec();
    let map_metadata = metadata
        .iter()
        .find(|blob| {
            blob.blob_type() == STRING_KEY_MAP_BLOB_TYPE
                && blob
                    .properties()
                    .get(FIELD_PROPERTY)
                    .is_some_and(|stored| stored == field)
        })
        .ok_or_else(|| {
            VectorError::CorruptBlob(format!("missing string-key map for Vamana field {field}"))
        })?;
    if map_metadata.snapshot_id() != snapshot {
        return Err(VectorError::CorruptBlob(
            "string-key map attached to a different snapshot".to_owned(),
        ));
    }
    let keys = StringKeyMap::decode(reader.blob(map_metadata).await?.data())?;
    Ok(Some(AttachedStringKeyIndex {
        bridge: StringKeyIndex::from_parts(attached.index, keys)?,
        config: attached.config,
    }))
}

/// Loads the Vamana index for `field` attached to exactly `snapshot`.
pub async fn load_index_for_snapshot(
    table: &Table,
    snapshot: i64,
    field: &str,
) -> Result<Option<AttachedIndex>> {
    let Some(statistics) = table.metadata().statistics_for_snapshot(snapshot) else {
        return Ok(None);
    };
    let input = table.file_io().new_input(&statistics.statistics_path)?;
    let reader = PuffinReader::new(input);
    let metadata = reader.file_metadata().await?.blobs().to_vec();
    let Some(blob_metadata) = metadata.iter().find(|blob| {
        blob.blob_type() == BLOB_TYPE
            && blob
                .properties()
                .get(FIELD_PROPERTY)
                .is_some_and(|stored| stored == field)
    }) else {
        return Ok(None);
    };
    let config = config_from_properties(blob_metadata.properties())?;
    let blob = reader.blob(blob_metadata).await?;
    let index = crate::codec::decode(blob.data())?;
    if index.reflected_snapshot() != snapshot {
        return Err(VectorError::CorruptBlob(format!(
            "Vamana blob reflects snapshot {} but is attached to {snapshot}",
            index.reflected_snapshot()
        )));
    }
    Ok(Some(AttachedIndex { index, config }))
}

/// Finds the nearest ancestor, including `snapshot`, carrying an index for
/// `field`. Maintenance uses this exact lineage walk as its incremental base.
pub async fn load_latest_ancestor_index(
    table: &Table,
    snapshot: i64,
    field: &str,
) -> Result<Option<AttachedIndex>> {
    let mut cursor = Some(snapshot);
    while let Some(id) = cursor {
        if let Some(index) = load_index_for_snapshot(table, id, field).await? {
            return Ok(Some(index));
        }
        cursor = table
            .metadata()
            .snapshot_by_id(id)
            .and_then(|candidate| candidate.parent_snapshot_id());
    }
    Ok(None)
}

/// Lists all Vamana indexes attached to exactly `snapshot`.
pub async fn list_indexes_for_snapshot(table: &Table, snapshot: i64) -> Result<Vec<AttachedIndex>> {
    let Some(statistics) = table.metadata().statistics_for_snapshot(snapshot) else {
        return Ok(Vec::new());
    };
    let input = table.file_io().new_input(&statistics.statistics_path)?;
    let reader = PuffinReader::new(input);
    let metadata = reader.file_metadata().await?.blobs().to_vec();
    let mut indexes = Vec::new();
    for blob_metadata in metadata.iter().filter(|blob| blob.blob_type() == BLOB_TYPE) {
        let config = config_from_properties(blob_metadata.properties())?;
        let blob = reader.blob(blob_metadata).await?;
        let index = crate::codec::decode(blob.data())?;
        indexes.push(AttachedIndex { index, config });
    }
    Ok(indexes)
}

/// Encodes the complete build contract into Puffin blob properties so another
/// process can refresh the index without a side registry.
fn properties_for(config: &MaintenanceConfig, index: &VamanaIndex) -> HashMap<String, String> {
    let params = index.params();
    HashMap::from([
        (FIELD_PROPERTY.to_owned(), config.vec_field.clone()),
        (ID_FIELD_PROPERTY.to_owned(), config.id_field.clone()),
        (
            ID_ENCODING_PROPERTY.to_owned(),
            config.id_encoding.as_str().to_owned(),
        ),
        (
            METRIC_PROPERTY.to_owned(),
            index.metric().as_str().to_owned(),
        ),
        (DIM_PROPERTY.to_owned(), index.dim().to_string()),
        (R_PROPERTY.to_owned(), params.r.to_string()),
        (L_PROPERTY.to_owned(), params.l_build.to_string()),
        (ALPHA_PROPERTY.to_owned(), params.alpha.to_string()),
    ])
}

/// Reconstructs a maintenance configuration from one Vamana blob's properties.
fn config_from_properties(properties: &HashMap<String, String>) -> Result<MaintenanceConfig> {
    let required = |name: &str| {
        properties
            .get(name)
            .cloned()
            .ok_or_else(|| VectorError::CorruptBlob(format!("missing Puffin property {name}")))
    };
    let field = required(FIELD_PROPERTY)?;
    let id_field = required(ID_FIELD_PROPERTY)?;
    let metric_name = required(METRIC_PROPERTY)?;
    let metric = crate::Metric::parse(&metric_name)
        .ok_or_else(|| VectorError::CorruptBlob(format!("unknown Puffin metric {metric_name}")))?;
    let parse_usize = |name: &str| -> Result<usize> {
        required(name)?.parse().map_err(|error| {
            VectorError::CorruptBlob(format!("invalid Puffin property {name}: {error}"))
        })
    };
    let alpha = required(ALPHA_PROPERTY)?.parse().map_err(|error| {
        VectorError::CorruptBlob(format!("invalid Puffin property {ALPHA_PROPERTY}: {error}"))
    })?;
    let mut config = MaintenanceConfig::new(id_field, field, metric);
    config.id_encoding = IdEncoding::parse(&required(ID_ENCODING_PROPERTY)?);
    config.params = VamanaParams {
        r: parse_usize(R_PROPERTY)?,
        l_build: parse_usize(L_PROPERTY)?,
        alpha,
    };
    Ok(config)
}

/// Reads the completed Puffin footer and converts its entries into Iceberg
/// `StatisticsFile` metadata.
async fn inspect_file(table: &Table, path: &str) -> Result<(i64, i64, Vec<BlobMetadata>)> {
    let input = table.file_io().new_input(path)?;
    let bytes = input.read().await?;
    let file_size = bytes.len() as i64;
    let footer_size = if bytes.len() >= 12 {
        let start = bytes.len() - 12;
        let payload_len = u32::from_le_bytes([
            bytes[start],
            bytes[start + 1],
            bytes[start + 2],
            bytes[start + 3],
        ]);
        i64::from(payload_len) + 16
    } else {
        return Err(VectorError::CorruptBlob(
            "Puffin footer is truncated".to_owned(),
        ));
    };
    let reader = PuffinReader::new(table.file_io().new_input(path)?);
    let blob_metadata = reader
        .file_metadata()
        .await?
        .blobs()
        .iter()
        .map(|blob| BlobMetadata {
            r#type: blob.blob_type().to_owned(),
            snapshot_id: blob.snapshot_id(),
            sequence_number: blob.sequence_number(),
            fields: blob.fields().to_vec(),
            properties: blob.properties().clone(),
        })
        .collect();
    Ok((file_size, footer_size, blob_metadata))
}

/// Commits the snapshot's statistics attachment with optimistic-concurrency
/// retry, matching the graph index attachment path.
async fn commit_statistics(
    catalog: &dyn Catalog,
    table: &Table,
    statistics: StatisticsFile,
) -> Result<()> {
    let mut current = table.clone();
    let mut attempt = 1u32;
    loop {
        let tx = Transaction::new(&current);
        let tx = tx
            .update_statistics()
            .set_statistics(statistics.clone())
            .apply(tx)?;
        match tx.commit(catalog).await {
            Ok(_) => return Ok(()),
            Err(error)
                if error.kind() == iceberg::ErrorKind::CatalogCommitConflicts
                    && attempt < COMMIT_ATTEMPTS =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(100 * u64::from(attempt)))
                    .await;
                current = catalog.load_table(table.identifier()).await?;
                attempt += 1;
            }
            Err(error) => return Err(error.into()),
        }
    }
}
