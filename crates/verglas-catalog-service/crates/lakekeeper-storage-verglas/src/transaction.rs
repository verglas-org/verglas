//! Buffered deterministic Lakekeeper-to-CRaft catalog transactions.

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
    use crate::{ImmutableMetadataStore, MetadataStoreError, VerglasCatalog, VerglasCatalogError};

    /// One idempotency key always maps to the same CRaft request identity, even when the payload differs.
    #[test]
    fn idempotency_key_has_a_deterministic_craft_request_identity() {
        let key = lakekeeper::service::idempotency::IdempotencyKey::parse(
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
        let key = lakekeeper::service::idempotency::IdempotencyKey::parse(
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

    /// A metadata authority that proves a transaction can fail before object access.
    struct UnusedMetadataStore;

    #[async_trait::async_trait]
    impl ImmutableMetadataStore for UnusedMetadataStore {
        /// Returns a deterministic unused table root.
        fn table_root(&self, request_id: u128, _namespace: &[String], _name: &str) -> String {
            format!("unused/tables/{request_id}")
        }
        /// Returns a deterministic unused view root.
        fn view_root(&self, request_id: u128, _namespace: &[String], _name: &str) -> String {
            format!("unused/views/{request_id}")
        }
        /// Rejects an unexpected table metadata read.
        async fn load_table(
            &self,
            _location: &str,
        ) -> Result<lakekeeper::iceberg::spec::TableMetadataRef, MetadataStoreError> {
            Err(MetadataStoreError {
                message: "unused".into(),
            })
        }
        /// Rejects an unexpected view metadata read.
        async fn load_view(
            &self,
            _location: &str,
        ) -> Result<lakekeeper::iceberg::spec::ViewMetadataRef, MetadataStoreError> {
            Err(MetadataStoreError {
                message: "unused".into(),
            })
        }
        /// Rejects an unexpected table metadata publication.
        async fn create_table(
            &self,
            _request_id: u128,
            _location: &str,
            _request: &lakekeeper::api::CreateTableRequest,
        ) -> Result<(String, lakekeeper::iceberg::spec::TableMetadataRef), MetadataStoreError>
        {
            Err(MetadataStoreError {
                message: "unused".into(),
            })
        }
        /// Rejects an unexpected table metadata commit.
        async fn commit_table(
            &self,
            _request_id: u128,
            _location: &str,
            _request: &lakekeeper::api::CommitTableRequest,
        ) -> Result<(String, lakekeeper::iceberg::spec::TableMetadataRef), MetadataStoreError>
        {
            Err(MetadataStoreError {
                message: "unused".into(),
            })
        }
        /// Rejects an unexpected multi-table metadata commit.
        async fn commit_tables(
            &self,
            _request_id: u128,
            _changes: &[(String, lakekeeper::api::CommitTableRequest)],
        ) -> Result<Vec<(String, lakekeeper::iceberg::spec::TableMetadataRef)>, MetadataStoreError>
        {
            Err(MetadataStoreError {
                message: "unused".into(),
            })
        }
        /// Rejects an unexpected view metadata publication.
        async fn create_view(
            &self,
            _request_id: u128,
            _location: &str,
            _request: &lakekeeper::api::CreateViewRequest,
        ) -> Result<(String, lakekeeper::iceberg::spec::ViewMetadataRef), MetadataStoreError>
        {
            Err(MetadataStoreError {
                message: "unused".into(),
            })
        }
        /// Rejects an unexpected view metadata commit.
        async fn commit_view(
            &self,
            _request_id: u128,
            _location: &str,
            _request: &lakekeeper::api::CommitViewRequest,
        ) -> Result<(String, lakekeeper::iceberg::spec::ViewMetadataRef), MetadataStoreError>
        {
            Err(MetadataStoreError {
                message: "unused".into(),
            })
        }
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
