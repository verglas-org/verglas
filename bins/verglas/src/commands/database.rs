//! Creates local database resources through the server's database API.
//!
//! This command only validates and serializes intent. The server resolves
//! scoped secrets and persists immutable binding identities.

use reqwest::Url;
use serde::Serialize;

use crate::cli::{DatabaseType, DbCommand, DbCreateArgs};

/// Request sent to the local database resource API.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum CreateDatabaseRequest {
    /// An Iceberg Lakehouse with explicit storage and catalog ownership.
    Lakehouse {
        /// Stable database name.
        name: String,
        /// Storage selection resolved by the server.
        storage: StorageRequest,
        /// Catalog selection resolved by the server.
        catalog: CatalogRequest,
    },
    /// An independent managed Neon database.
    Postgres {
        /// Stable database name.
        name: String,
        /// Postgres engine provisioned for the database.
        engine: PostgresEngineRequest,
    },
}

/// Lakehouse storage selection.
#[derive(Debug, Serialize)]
#[serde(tag = "mode", rename_all = "kebab-case")]
enum StorageRequest {
    /// Verglas-managed object storage.
    Managed,
    /// Customer storage selected through longest-scope secret resolution.
    ScopedSecret {
        /// S3 prefix whose authorized secret must be bound.
        data_path: String,
    },
}

/// Lakehouse catalog selection.
#[derive(Debug, Serialize)]
#[serde(tag = "mode", rename_all = "kebab-case")]
enum CatalogRequest {
    /// A managed Lakekeeper warehouse.
    ManagedLakekeeper,
    /// A customer Iceberg REST service selected through a scoped secret.
    External {
        /// Catalog base URI.
        uri: String,
        /// Warehouse name within that catalog.
        warehouse: String,
    },
}

/// Managed Postgres engine selection.
#[derive(Debug, Serialize)]
#[serde(tag = "mode", rename_all = "kebab-case")]
enum PostgresEngineRequest {
    /// Verglas' managed Neon composition.
    ManagedNeon,
}

/// Runs one database command against the selected local server.
pub async fn run(
    command: DbCommand,
    endpoint: &str,
    token: Option<&str>,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        DbCommand::Create(args) => create(endpoint, token, args, json).await,
    }
}

/// Validates a database declaration and sends its typed request.
async fn create(
    endpoint: &str,
    token: Option<&str>,
    args: DbCreateArgs,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    validate_name(&args.name)?;
    let name = args.name.clone();
    let request = request_from_args(args)?;
    let response = post_json(endpoint, "/v1/databases", token, &request).await?;
    if json {
        println!("{}", serde_json::to_string(&response)?);
    } else {
        println!("created database {name}");
    }
    Ok(())
}

/// Converts CLI arguments into one unambiguous database composition.
fn request_from_args(
    args: DbCreateArgs,
) -> Result<CreateDatabaseRequest, Box<dyn std::error::Error>> {
    match args.database_type {
        DatabaseType::Postgres => {
            if args.data_path.is_some() || args.catalog.is_some() || args.warehouse.is_some() {
                return Err("Postgres databases do not accept Lakehouse binding options".into());
            }
            Ok(CreateDatabaseRequest::Postgres {
                name: args.name,
                engine: PostgresEngineRequest::ManagedNeon,
            })
        }
        DatabaseType::Lakehouse => {
            let storage = match args.data_path {
                Some(data_path) => {
                    validate_uri(&data_path, &["s3"], "data path")?;
                    StorageRequest::ScopedSecret { data_path }
                }
                None => StorageRequest::Managed,
            };
            let catalog = match (args.catalog, args.warehouse) {
                (None, None) => CatalogRequest::ManagedLakekeeper,
                (Some(uri), Some(warehouse)) => {
                    if matches!(&storage, StorageRequest::Managed) {
                        return Err("an external catalog requires --data-path".into());
                    }
                    validate_uri(&uri, &["http", "https"], "catalog")?;
                    if warehouse.trim().is_empty() {
                        return Err("warehouse must not be empty".into());
                    }
                    CatalogRequest::External { uri, warehouse }
                }
                (Some(_), None) => return Err("--catalog requires --warehouse".into()),
                (None, Some(_)) => return Err("--warehouse requires --catalog".into()),
            };
            Ok(CreateDatabaseRequest::Lakehouse {
                name: args.name,
                storage,
                catalog,
            })
        }
    }
}

/// Rejects names that cannot be stable local resource identifiers.
fn validate_name(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut characters = name.chars();
    let Some(first) = characters.next() else {
        return Err("resource name must not be empty".into());
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !characters.all(|character| {
            character.is_ascii_alphanumeric() || character == '_' || character == '-'
        })
    {
        return Err("resource name must start with a letter or underscore and contain only ASCII letters, digits, underscores, or hyphens".into());
    }
    Ok(())
}

/// Validates a binding URI against its allowed schemes and required authority.
fn validate_uri(
    value: &str,
    schemes: &[&str],
    label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let parsed = Url::parse(value).map_err(|error| format!("invalid {label} URI: {error}"))?;
    if !schemes.contains(&parsed.scheme()) || parsed.host_str().is_none() {
        return Err(format!("{label} must be an absolute {} URI", schemes.join(" or ")).into());
    }
    Ok(())
}

/// Sends a typed create request and returns the server's JSON response.
async fn post_json<T: Serialize>(
    endpoint: &str,
    path: &str,
    token: Option<&str>,
    body: &T,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let url = Url::parse(endpoint)?.join(path)?;
    let client = reqwest::Client::new();
    let mut request = client.post(url).json(body);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?.error_for_status()?;
    Ok(response.json().await?)
}
