//! Database-scoped catalog bindings for multi-database tenant runtimes.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::CatalogGateway;

/// Defines one validated non-empty identifier newtype.
macro_rules! identifier {
    ($name:ident, $label:literal) => {
        #[doc = concat!("Stable ", $label, " identifier.")]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Validates and constructs a ", $label, " identifier.")]
            pub fn new(value: impl Into<String>) -> Result<Self, RegistryError> {
                let value = value.into();
                if value.trim().is_empty() {
                    return Err(RegistryError::InvalidIdentifier($label));
                }
                Ok(Self(value))
            }

            #[doc = concat!("Returns this ", $label, " identifier as text.")]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

identifier!(DatabaseId, "database");
identifier!(CatalogBindingId, "catalog binding");
identifier!(StorageBindingId, "storage binding");

/// One database's explicit catalog and object-storage routing decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogBinding {
    database_id: DatabaseId,
    catalog_binding_id: CatalogBindingId,
    storage_binding_id: StorageBindingId,
    endpoint: String,
    warehouse: Option<String>,
    credential_secret_id: Option<String>,
}

impl CatalogBinding {
    /// Creates a binding. Only Iceberg REST HTTP endpoints are accepted.
    pub fn new(
        database_id: DatabaseId,
        catalog_binding_id: CatalogBindingId,
        storage_binding_id: StorageBindingId,
        endpoint: impl Into<String>,
        warehouse: Option<String>,
        credential_secret_id: Option<String>,
    ) -> Result<Self, RegistryError> {
        let endpoint = endpoint.into();
        if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
            return Err(RegistryError::InvalidEndpoint(endpoint));
        }
        Ok(Self {
            database_id,
            catalog_binding_id,
            storage_binding_id,
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            warehouse,
            credential_secret_id,
        })
    }

    /// Returns the database routed by this binding.
    pub fn database_id(&self) -> &DatabaseId {
        &self.database_id
    }

    /// Returns the stable catalog deployment or external-catalog identity.
    pub fn catalog_binding_id(&self) -> &CatalogBindingId {
        &self.catalog_binding_id
    }

    /// Returns the storage origin used to namespace every cache key.
    pub fn storage_binding_id(&self) -> &StorageBindingId {
        &self.storage_binding_id
    }

    /// Returns the Iceberg REST endpoint without a trailing slash.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Returns the database-specific Catalog/external warehouse.
    pub fn warehouse(&self) -> Option<&str> {
        self.warehouse.as_deref()
    }

    /// Returns the stable secret resource authorized for this catalog.
    pub fn credential_secret_id(&self) -> Option<&str> {
        self.credential_secret_id.as_deref()
    }
}

/// In-memory routing index populated from durable database resources.
#[derive(Debug, Default)]
pub struct CatalogRegistry {
    bindings: RwLock<HashMap<DatabaseId, CatalogBinding>>,
}

/// Live catalog gateways selected by database ID for query and write routes.
#[derive(Debug, Clone, Default)]
pub struct CatalogRuntimeRegistry {
    gateways: Arc<RwLock<HashMap<DatabaseId, CatalogGateway>>>,
}

impl CatalogRuntimeRegistry {
    /// Installs a live gateway exactly once for a database.
    pub fn insert(
        &self,
        database_id: DatabaseId,
        gateway: CatalogGateway,
    ) -> Result<(), RegistryError> {
        self.insert_bound(database_id.clone(), database_id, gateway)
    }

    /// Installs a route-name gateway bound to its immutable authorization identity.
    pub fn insert_bound(
        &self,
        route_id: DatabaseId,
        authorization_id: DatabaseId,
        gateway: CatalogGateway,
    ) -> Result<(), RegistryError> {
        let gateway = gateway.bind_database(authorization_id.as_str());
        let mut gateways = self
            .gateways
            .write()
            .map_err(|_| RegistryError::LockPoisoned)?;
        if gateways.contains_key(&route_id) {
            return Err(RegistryError::AlreadyExists(route_id.as_str().to_owned()));
        }
        gateways.insert(route_id, gateway);
        Ok(())
    }

    /// Resolves a cloned live gateway for one database.
    pub fn get(&self, database_id: &DatabaseId) -> Option<CatalogGateway> {
        self.gateways
            .read()
            .ok()
            .and_then(|gateways| gateways.get(database_id).cloned())
    }

    /// Resolves the immutable authorization identity behind one tenant-local route name.
    pub fn authorization_id(&self, route_id: &DatabaseId) -> Option<DatabaseId> {
        self.gateways
            .read()
            .ok()
            .and_then(|gateways| gateways.get(route_id).cloned())
            .and_then(|gateway| gateway.database_id().map(str::to_owned))
            .and_then(|id| DatabaseId::new(id).ok())
    }

    /// Removes a live gateway when a database is deleted or suspended.
    pub fn remove(&self, database_id: &DatabaseId) -> Option<CatalogGateway> {
        self.gateways
            .write()
            .ok()
            .and_then(|mut gateways| gateways.remove(database_id))
    }

    /// Atomically replaces the complete live routing set discovered from durable databases.
    pub fn replace_all<I>(&self, gateways: I) -> Result<(), RegistryError>
    where
        I: IntoIterator<Item = (DatabaseId, CatalogGateway)>,
    {
        self.replace_all_bound(
            gateways
                .into_iter()
                .map(|(database_id, gateway)| (database_id.clone(), database_id, gateway)),
        )
    }

    /// Replaces all route-name gateways while retaining immutable authorization identities.
    pub fn replace_all_bound<I>(&self, gateways: I) -> Result<(), RegistryError>
    where
        I: IntoIterator<Item = (DatabaseId, DatabaseId, CatalogGateway)>,
    {
        let mut replacement = HashMap::new();
        for (route_id, authorization_id, gateway) in gateways {
            let gateway = gateway.bind_database(authorization_id.as_str());
            if replacement.insert(route_id.clone(), gateway).is_some() {
                return Err(RegistryError::AlreadyExists(route_id.as_str().to_owned()));
            }
        }
        let mut current = self
            .gateways
            .write()
            .map_err(|_| RegistryError::LockPoisoned)?;
        *current = replacement;
        Ok(())
    }

    /// Returns the number of database gateways in the current routing snapshot.
    pub fn len(&self) -> Result<usize, RegistryError> {
        self.gateways
            .read()
            .map(|gateways| gateways.len())
            .map_err(|_| RegistryError::LockPoisoned)
    }

    /// Reports whether the current routing snapshot contains no database gateways.
    pub fn is_empty(&self) -> Result<bool, RegistryError> {
        self.gateways
            .read()
            .map(|gateways| gateways.is_empty())
            .map_err(|_| RegistryError::LockPoisoned)
    }
}

impl CatalogRegistry {
    /// Installs a database binding exactly once.
    pub fn insert(&self, binding: CatalogBinding) -> Result<(), RegistryError> {
        let mut bindings = self
            .bindings
            .write()
            .map_err(|_| RegistryError::LockPoisoned)?;
        if bindings.contains_key(binding.database_id()) {
            return Err(RegistryError::AlreadyExists(
                binding.database_id().as_str().to_owned(),
            ));
        }
        bindings.insert(binding.database_id().clone(), binding);
        Ok(())
    }

    /// Resolves one database without consulting process-global catalog config.
    pub fn get(&self, database_id: &DatabaseId) -> Option<CatalogBinding> {
        self.bindings
            .read()
            .ok()
            .and_then(|bindings| bindings.get(database_id).cloned())
    }

    /// Removes a database binding when its resource is deleted.
    pub fn remove(&self, database_id: &DatabaseId) -> Option<CatalogBinding> {
        self.bindings
            .write()
            .ok()
            .and_then(|mut bindings| bindings.remove(database_id))
    }
}

/// Catalog binding validation and registration failures.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RegistryError {
    /// An identifier contained no usable characters.
    #[error("{0} identifier must not be empty")]
    InvalidIdentifier(&'static str),
    /// The catalog endpoint was not an HTTP Iceberg REST endpoint.
    #[error("catalog endpoint `{0}` must use http or https")]
    InvalidEndpoint(String),
    /// A database already has a binding and cannot be silently rebound.
    #[error("database `{0}` already has a catalog binding")]
    AlreadyExists(String),
    /// A prior panic poisoned the registry lock.
    #[error("catalog registry lock is poisoned")]
    LockPoisoned,
}
