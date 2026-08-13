//! Durable idempotency identities for hosted Iceberg mutations.
//!
//! The same atomic transaction stores idempotency with its hosted mutation in
//! one CRaft batch. The record is therefore either committed with the catalog
//! change or absent; no process-local check can race the authoritative
//! compare-and-write.

use std::collections::BTreeMap;

use lakekeeper::service::idempotency::IdempotencyKey;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;
use verglas_consensus::CatalogEntity;

use crate::{VerglasCatalog, VerglasCatalogError, VerglasTransaction};

/// The durable content associated with a successful hosted mutation.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct IdempotencyRecord {
    /// The endpoint operation that accepted this key.
    operation: String,
    /// Canonical digest of every operation input.
    fingerprint: String,
    /// The original successful endpoint result.
    result: String,
}

/// The exact durable identity that binds one optional client idempotency key.
#[derive(Clone, Debug)]
pub(crate) struct HostedIdempotency {
    key: IdempotencyKey,
    operation: String,
    fingerprint: String,
    result: String,
}

impl HostedIdempotency {
    /// Builds a canonical operation identity and captures its successful response before CRaft submission.
    pub(crate) fn new<I: Serialize, R: Serialize>(
        key: IdempotencyKey,
        operation: &str,
        input: &I,
        result: &R,
    ) -> Result<Self, VerglasCatalogError> {
        Ok(Self {
            key,
            operation: operation.to_owned(),
            fingerprint: fingerprint(input)?,
            result: canonical_json(result)?,
        })
    }

    /// Returns the deterministic CRaft request identity derived only from the idempotency key.
    pub(crate) fn request_id(&self) -> u128 {
        Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!("verglas:catalog:idempotency:{}", self.key.as_uuid()).as_bytes(),
        )
        .as_u128()
    }

    /// Returns the canonical input fingerprint retained in the durable record.
    #[cfg(test)]
    pub(crate) fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// Adds the absent-record requirement and result record to the hosted mutation's single CRaft batch.
    pub(crate) fn attach(
        &self,
        transaction: &mut VerglasTransaction,
    ) -> Result<(), VerglasCatalogError> {
        let key = self.record_key();
        transaction.require_absent(CatalogEntity::Idempotency, key.clone());
        transaction.put(
            CatalogEntity::Idempotency,
            key,
            serde_json::to_string(&IdempotencyRecord {
                operation: self.operation.clone(),
                fingerprint: self.fingerprint.clone(),
                result: self.result.clone(),
            })
            .map_err(VerglasCatalogError::Encode)?,
        );
        Ok(())
    }

    /// Resolves a final result only after a CRaft conflict, never before the mutation attempt.
    pub(crate) async fn replay<R: DeserializeOwned>(
        &self,
        catalog: &VerglasCatalog,
    ) -> Result<Option<R>, VerglasCatalogError> {
        let Some(record) = catalog
            .read::<IdempotencyRecord>(CatalogEntity::Idempotency, &self.record_key())
            .await?
        else {
            return Ok(None);
        };
        // A different operation or input fingerprint under the same key conflicts;
        // it must never be treated as a successful replay.
        if record.operation != self.operation || record.fingerprint != self.fingerprint {
            return Err(VerglasCatalogError::IdempotencyConflict);
        }
        serde_json::from_str(&record.result)
            .map(Some)
            .map_err(VerglasCatalogError::Decode)
    }

    /// Returns the typed stable record identity owned by the warehouse group.
    fn record_key(&self) -> String {
        format!("hosted:{}", self.key.as_uuid())
    }
}

/// Converts a serializable input to a stable JSON digest suitable for cross-instance retries.
fn fingerprint<T: Serialize>(input: &T) -> Result<String, VerglasCatalogError> {
    Ok(Uuid::new_v5(&Uuid::NAMESPACE_OID, canonical_json(input)?.as_bytes()).to_string())
}

/// Serializes JSON with object keys recursively sorted while preserving array order.
fn canonical_json<T: Serialize>(value: &T) -> Result<String, VerglasCatalogError> {
    let value = serde_json::to_value(value).map_err(VerglasCatalogError::Encode)?;
    serde_json::to_string(&canonical_value(value)).map_err(VerglasCatalogError::Encode)
}

/// Rebuilds JSON values with deterministic object member order.
fn canonical_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_value).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, canonical_value(value)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        scalar => scalar,
    }
}
