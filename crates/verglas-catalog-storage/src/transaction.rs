//! Buffered deterministic Catalog-to-CRaft catalog transactions.

use verglas_consensus::{CatalogAction, CatalogBatch, CatalogEntity, CatalogRequirement};

use crate::{VerglasCatalog, VerglasCatalogError};

/// One not-yet-submitted atomic catalog transaction.
pub struct VerglasTransaction {
    catalog: VerglasCatalog,
    request_id: u128,
    requirements: Vec<CatalogRequirement>,
    actions: Vec<CatalogAction>,
}

impl VerglasTransaction {
    /// Starts an empty transaction under an exact client retry identity.
    pub(crate) fn new(catalog: VerglasCatalog, request_id: u128) -> Self {
        Self {
            catalog,
            request_id,
            requirements: Vec::new(),
            actions: Vec::new(),
        }
    }

    /// Requires that a domain record has not already been created.
    pub fn require_absent(&mut self, entity: CatalogEntity, id: String) {
        self.requirements
            .push(CatalogRequirement::RecordAbsent { entity, id });
    }

    /// Requires that a domain record still contains this exact JSON document.
    pub fn require_document(&mut self, entity: CatalogEntity, id: String, expected: String) {
        self.requirements.push(CatalogRequirement::RecordDocument {
            entity,
            id,
            expected,
        });
    }

    /// Adds a complete replacement document to this atomic transaction.
    pub fn put(&mut self, entity: CatalogEntity, id: String, document: String) {
        self.actions.push(CatalogAction::PutRecord {
            entity,
            id,
            document,
        });
    }

    /// Adds a durable document deletion to this atomic transaction.
    pub fn delete(&mut self, entity: CatalogEntity, id: String) {
        self.actions
            .push(CatalogAction::DeleteRecord { entity, id });
    }

    /// Builds and commits the whole transaction through exactly one CRaft request.
    pub async fn commit(self) -> Result<(), VerglasCatalogError> {
        let batch = CatalogBatch::new(self.requirements, self.actions)
            .map_err(|_| VerglasCatalogError::InvalidBatch)?;
        self.catalog.commit(self.request_id, batch).await
    }
}

#[cfg(test)]
mod tests {
    //! Tests for local transaction validation before any consensus request.

    use verglas_consensus::{CatalogAction, CatalogEntity, CatalogRequirement};

    use super::VerglasTransaction;
    use crate::test_support::UnusedMetadataStore;
    use crate::{VerglasCatalog, VerglasCatalogError};

    /// One idempotency key always maps to the same CRaft request identity, even when the payload differs.
    #[test]
    fn idempotency_key_has_a_deterministic_craft_request_identity() {
        let key = verglas_catalog_core::service::idempotency::IdempotencyKey::parse(
            "550e8400-e29b-41d4-a716-446655440000",
        )
        .expect("valid key");
        let first = crate::idempotency::HostedIdempotency::new(
            key,
            "commit_table",
            &serde_json::json!({"snapshot": 1}),
            &serde_json::json!({"metadata_location": "s3://one"}),
        )
        .expect("first identity");
        let second = crate::idempotency::HostedIdempotency::new(
            key,
            "commit_table",
            &serde_json::json!({"snapshot": 2}),
            &serde_json::json!({"metadata_location": "s3://two"}),
        )
        .expect("second identity");
        assert_eq!(first.request_id(), second.request_id());
        assert_ne!(first.fingerprint(), second.fingerprint());
    }

    /// An idempotency result is part of the same atomic transaction as the hosted mutation.
    #[test]
    fn idempotency_record_is_staged_with_the_hosted_transaction() {
        let catalog = VerglasCatalog::with_ingresses(
            ["http://127.0.0.1:1".to_owned()],
            "tenant",
            "warehouse",
            UnusedMetadataStore,
        )
        .expect("one ingress is valid");
        let key = verglas_catalog_core::service::idempotency::IdempotencyKey::parse(
            "550e8400-e29b-41d4-a716-446655440000",
        )
        .expect("valid key");
        let identity = crate::idempotency::HostedIdempotency::new(
            key,
            "commit_transaction",
            &serde_json::json!({"tables": ["analytics.events"]}),
            &serde_json::json!({"metadata_locations": ["s3://warehouse/metadata.json"]}),
        )
        .expect("identity");
        let mut transaction = VerglasTransaction::new(catalog, identity.request_id());
        transaction.put(
            CatalogEntity::Table,
            "warehouse:analytics.events".to_owned(),
            r#"{"metadata_location":"s3://warehouse/metadata.json"}"#.to_owned(),
        );
        identity
            .attach(&mut transaction)
            .expect("serializable record");
        assert!(matches!(
            transaction.requirements.last(),
            Some(CatalogRequirement::RecordAbsent {
                entity: CatalogEntity::Idempotency,
                ..
            })
        ));
        let Some(CatalogAction::PutRecord {
            entity: CatalogEntity::Idempotency,
            document,
            ..
        }) = transaction.actions.last()
        else {
            panic!("idempotency record must be part of the transaction");
        };
        assert!(document.contains("commit_transaction"));
        assert!(document.contains("metadata_locations"));
    }

    /// An empty transaction fails locally and never attempts an ingress request.
    #[tokio::test]
    async fn empty_transaction_fails_before_consensus_submission() {
        let catalog = VerglasCatalog::with_ingresses(
            ["http://127.0.0.1:1".to_owned()],
            "tenant",
            "warehouse",
            UnusedMetadataStore,
        )
        .expect("one ingress is valid");
        let result = VerglasTransaction::new(catalog, 1).commit().await;
        assert!(matches!(result, Err(VerglasCatalogError::InvalidBatch)));
    }
}
