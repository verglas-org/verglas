//! Deterministic authoritative Iceberg catalog transactions applied in Raft order.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// A durable Lakekeeper domain collection owned by one warehouse consensus group.
///
/// The document shape remains the Lakekeeper domain type serialized by its adapter;
/// this discriminator makes every collection explicit in the replicated state.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum CatalogEntity {
    /// Server bootstrap and configuration state.
    Server,
    /// Project declarations.
    Project,
    /// Warehouse declarations and storage profiles.
    Warehouse,
    /// Namespace metadata and properties.
    Namespace,
    /// Iceberg table registrations and metadata pointers.
    Table,
    /// Iceberg view registrations and metadata pointers.
    View,
    /// Generic table registrations.
    GenericTable,
    /// Role definitions.
    Role,
    /// Role membership and assignments.
    RoleAssignment,
    /// Authorization tags and assignments.
    Tag,
    /// Idempotency records.
    Idempotency,
    /// Maintenance task and queue state.
    Task,
    /// Catalog-owned secret references, never secret material.
    SecretReference,
}

/// JSON-safe, deterministic hosted-domain records grouped by their collection.
///
/// JSON object keys are catalog entities and record identities, so persisted Raft
/// state never depends on serializing a compound map key.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct CatalogRecords {
    collections: BTreeMap<CatalogEntity, BTreeMap<String, String>>,
}

impl CatalogRecords {
    /// Returns the document bound to one typed stable identity.
    pub(crate) fn get(&self, entity: CatalogEntity, id: &str) -> Option<&String> {
        self.collections.get(&entity)?.get(id)
    }

    /// Returns every record in a collection in deterministic identity order.
    pub(crate) fn list(&self, entity: CatalogEntity) -> Vec<(String, String)> {
        self.collections
            .get(&entity)
            .into_iter()
            .flat_map(|records| records.iter())
            .map(|(id, document)| (id.clone(), document.clone()))
            .collect()
    }

    /// Inserts a document, replacing an existing document for the same identity.
    fn put(&mut self, entity: CatalogEntity, id: String, document: String) {
        self.collections
            .entry(entity)
            .or_default()
            .insert(id, document);
    }

    /// Removes and returns a document while retaining no empty collection.
    fn remove(&mut self, entity: CatalogEntity, id: &str) -> Option<String> {
        let document = self.collections.get_mut(&entity)?.remove(id);
        if self
            .collections
            .get(&entity)
            .is_some_and(BTreeMap::is_empty)
        {
            self.collections.remove(&entity);
        }
        document
    }
}

/// Preconditions evaluated atomically against one applied catalog snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CatalogRequirement {
    /// The named namespace must not exist.
    NamespaceAbsent { namespace: String },
    /// The named namespace must exist.
    NamespacePresent { namespace: String },
    /// The named table must not exist.
    TableAbsent { table: String },
    /// The table must currently point at this exact immutable metadata object.
    TableMetadata { table: String, expected: String },
    /// A typed hosted-domain record must not exist.
    RecordAbsent { entity: CatalogEntity, id: String },
    /// A typed hosted-domain record must be present with this exact document.
    RecordDocument {
        /// Domain collection.
        entity: CatalogEntity,
        /// Stable domain identity.
        id: String,
        /// Exact JSON document bound by the optimistic requirement.
        expected: String,
    },
}

/// Catalog mutations applied only if every requirement succeeds.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CatalogAction {
    /// Creates one namespace.
    CreateNamespace { namespace: String },
    /// Drops one empty namespace.
    DropNamespace { namespace: String },
    /// Creates or atomically advances one table metadata pointer.
    PutTable {
        table: String,
        metadata_location: String,
    },
    /// Removes one table registration without deleting immutable objects.
    DropTable { table: String },
    /// Creates or replaces one typed hosted-domain record atomically.
    PutRecord {
        /// Domain collection.
        entity: CatalogEntity,
        /// Stable domain identity.
        id: String,
        /// JSON representation of the corresponding Lakekeeper domain object.
        document: String,
    },
    /// Removes one typed hosted-domain record without deleting customer objects.
    DeleteRecord {
        /// Domain collection.
        entity: CatalogEntity,
        /// Stable domain identity.
        id: String,
    },
}

/// One typed, deterministic catalog transaction batch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CatalogBatch {
    requirements: Vec<CatalogRequirement>,
    actions: Vec<CatalogAction>,
}

impl CatalogBatch {
    /// Builds a non-empty transaction after rejecting empty catalog identities.
    pub fn new(
        requirements: Vec<CatalogRequirement>,
        actions: Vec<CatalogAction>,
    ) -> Result<Self, CatalogBatchError> {
        if actions.is_empty()
            || requirements.iter().any(requirement_name_empty)
            || actions.iter().any(action_name_empty)
            || actions.iter().any(action_document_invalid)
        {
            return Err(CatalogBatchError);
        }
        Ok(Self {
            requirements,
            actions,
        })
    }

    /// Applies all requirements and actions atomically to cloned state.
    pub(crate) fn apply(
        &self,
        namespaces: &mut BTreeSet<String>,
        tables: &mut BTreeMap<String, String>,
    ) -> Result<(), CatalogBatchError> {
        for requirement in &self.requirements {
            let satisfied = match requirement {
                CatalogRequirement::NamespaceAbsent { namespace } => {
                    !namespaces.contains(namespace)
                }
                CatalogRequirement::NamespacePresent { namespace } => {
                    namespaces.contains(namespace)
                }
                CatalogRequirement::TableAbsent { table } => !tables.contains_key(table),
                CatalogRequirement::TableMetadata { table, expected } => {
                    tables.get(table) == Some(expected)
                }
                CatalogRequirement::RecordAbsent { .. }
                | CatalogRequirement::RecordDocument { .. } => true,
            };
            if !satisfied {
                return Err(CatalogBatchError);
            }
        }
        let mut next_namespaces = namespaces.clone();
        let mut next_tables = tables.clone();
        for action in &self.actions {
            match action {
                CatalogAction::CreateNamespace { namespace } => {
                    if !next_namespaces.insert(namespace.clone()) {
                        return Err(CatalogBatchError);
                    }
                }
                CatalogAction::DropNamespace { namespace } => {
                    let prefix = format!("{namespace}.");
                    if next_tables.keys().any(|table| table.starts_with(&prefix))
                        || !next_namespaces.remove(namespace)
                    {
                        return Err(CatalogBatchError);
                    }
                }
                CatalogAction::PutTable {
                    table,
                    metadata_location,
                } => {
                    let namespace = table
                        .rsplit_once('.')
                        .map(|(namespace, _)| namespace)
                        .ok_or(CatalogBatchError)?;
                    if !next_namespaces.contains(namespace) || metadata_location.is_empty() {
                        return Err(CatalogBatchError);
                    }
                    next_tables.insert(table.clone(), metadata_location.clone());
                }
                CatalogAction::DropTable { table } => {
                    if next_tables.remove(table).is_none() {
                        return Err(CatalogBatchError);
                    }
                }
                CatalogAction::PutRecord { .. } | CatalogAction::DeleteRecord { .. } => {}
            }
        }
        *namespaces = next_namespaces;
        *tables = next_tables;
        Ok(())
    }

    /// Applies typed hosted-domain records atomically against a cloned document state.
    pub(crate) fn apply_records(
        &self,
        records: &mut CatalogRecords,
    ) -> Result<(), CatalogBatchError> {
        for requirement in &self.requirements {
            let valid = match requirement {
                CatalogRequirement::RecordAbsent { entity, id } => {
                    records.get(*entity, id).is_none()
                }
                CatalogRequirement::RecordDocument {
                    entity,
                    id,
                    expected,
                } => records.get(*entity, id) == Some(expected),
                _ => true,
            };
            if !valid {
                return Err(CatalogBatchError);
            }
        }
        let mut next = records.clone();
        for action in &self.actions {
            match action {
                CatalogAction::PutRecord {
                    entity,
                    id,
                    document,
                } => {
                    next.put(*entity, id.clone(), document.clone());
                }
                CatalogAction::DeleteRecord { entity, id }
                    if next.remove(*entity, id).is_none() =>
                {
                    return Err(CatalogBatchError);
                }
                _ => {}
            }
        }
        *records = next;
        Ok(())
    }
}

/// An invalid or conflicting deterministic catalog transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("catalog transaction requirements or actions are invalid")]
pub struct CatalogBatchError;

fn requirement_name_empty(requirement: &CatalogRequirement) -> bool {
    match requirement {
        CatalogRequirement::NamespaceAbsent { namespace }
        | CatalogRequirement::NamespacePresent { namespace } => namespace.is_empty(),
        CatalogRequirement::TableAbsent { table }
        | CatalogRequirement::TableMetadata { table, .. } => table.is_empty(),
        CatalogRequirement::RecordAbsent { id, .. }
        | CatalogRequirement::RecordDocument { id, .. } => id.is_empty(),
    }
}

fn action_name_empty(action: &CatalogAction) -> bool {
    match action {
        CatalogAction::CreateNamespace { namespace }
        | CatalogAction::DropNamespace { namespace } => namespace.is_empty(),
        CatalogAction::PutTable { table, .. } | CatalogAction::DropTable { table } => {
            table.is_empty()
        }
        CatalogAction::PutRecord { id, .. } | CatalogAction::DeleteRecord { id, .. } => {
            id.is_empty()
        }
    }
}

/// Rejects any document that cannot be decoded as JSON before replication.
fn action_document_invalid(action: &CatalogAction) -> bool {
    match action {
        CatalogAction::PutRecord { document, .. } => {
            serde_json::from_str::<serde_json::Value>(document).is_err()
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for deterministic hosted-domain document transactions.

    use super::{CatalogAction, CatalogBatch, CatalogEntity, CatalogRecords, CatalogRequirement};

    /// Records are guarded and changed atomically without touching table-pointer state.
    #[test]
    fn record_transaction_applies_atomically() {
        let batch = CatalogBatch::new(
            vec![CatalogRequirement::RecordAbsent {
                entity: CatalogEntity::Namespace,
                id: "default.analytics".to_owned(),
            }],
            vec![CatalogAction::PutRecord {
                entity: CatalogEntity::Namespace,
                id: "default.analytics".to_owned(),
                document: r#"{"properties":{"owner":"data"}}"#.to_owned(),
            }],
        )
        .expect("valid record transaction");
        let mut records = CatalogRecords::default();
        batch.apply_records(&mut records).expect("apply record");
        assert_eq!(
            records.get(CatalogEntity::Namespace, "default.analytics"),
            Some(&r#"{"properties":{"owner":"data"}}"#.to_owned())
        );
    }
}
