//! Publishes Iceberg table commits onto the tenant Cloudflare Queue.
//!
//! After a hosted catalog commit succeeds, Lakekeeper POSTs one message per
//! table. The URL is either the control-plane producer
//! (`https://api.verglas.dev/v1/table-commits`) or the Cloudflare Queues HTTP
//! API. Both accept `{ "body": { eventType, tenant_id, table, ... } }`.

use std::sync::OnceLock;

use lakekeeper::api::iceberg::v1::TableIdent;
use serde_json::{Value, json};

const EVENT_TYPE: &str = "org.verglas.table.commit";

/// One table-commit message body placed on the tenant queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableCommitEvent {
    pub tenant_id: String,
    pub table: String,
    pub metadata_location: String,
}

/// Formats an Iceberg table identifier as the Verglas `ns.table` subject.
pub fn table_subject(ident: &TableIdent) -> String {
    format!(
        "{}.{}",
        ident.namespace.to_url_string().replace('\u{1f}', "."),
        ident.name
    )
}

/// Builds the Cloudflare Queues HTTP publish envelope.
pub fn queue_message(event: &TableCommitEvent) -> Value {
    json!({
        "body": {
            "eventType": EVENT_TYPE,
            "tenant_id": event.tenant_id,
            "table": event.table,
            "metadata_location": event.metadata_location,
        }
    })
}

/// Fire-and-forget publish after a durable catalog commit. Missing queue
/// configuration is a no-op so local catalogs stay silent.
pub fn enqueue_table_commit(ident: &TableIdent, metadata_location: &str) {
    let Some(event) = configured_event(ident, metadata_location) else {
        return;
    };
    tokio::spawn(async move {
        if let Err(error) = publish(&event).await {
            tracing::warn!(
                table = %event.table,
                tenant_id = %event.tenant_id,
                "table commit queue publish failed: {error}"
            );
        }
    });
}

fn configured_event(ident: &TableIdent, metadata_location: &str) -> Option<TableCommitEvent> {
    let tenant_id = std::env::var("VERGLAS_TENANT_ID")
        .or_else(|_| std::env::var("VERGLAS_CATALOG_TENANT"))
        .ok()?
        .trim()
        .to_owned();
    if tenant_id.is_empty()
        || std::env::var("VERGLAS_TABLE_COMMIT_QUEUE_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())
            .is_none()
        || std::env::var("VERGLAS_TABLE_COMMIT_QUEUE_TOKEN")
            .ok()
            .filter(|token| !token.trim().is_empty())
            .is_none()
    {
        return None;
    }
    Some(TableCommitEvent {
        tenant_id,
        table: table_subject(ident),
        metadata_location: metadata_location.to_owned(),
    })
}

async fn publish(event: &TableCommitEvent) -> Result<(), String> {
    let url = std::env::var("VERGLAS_TABLE_COMMIT_QUEUE_URL")
        .map_err(|_| "VERGLAS_TABLE_COMMIT_QUEUE_URL is unset".to_owned())?;
    let token = std::env::var("VERGLAS_TABLE_COMMIT_QUEUE_TOKEN")
        .map_err(|_| "VERGLAS_TABLE_COMMIT_QUEUE_TOKEN is unset".to_owned())?;
    let client = CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("table-commit queue client")
    });
    let response = client
        .post(url.trim())
        .bearer_auth(token.trim())
        .json(&queue_message(event))
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("queue publish {status}: {body}"));
    }
    Ok(())
}

static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

#[cfg(test)]
mod tests {
    use lakekeeper::api::iceberg::v1::{NamespaceIdent, TableIdent};

    use super::{TableCommitEvent, queue_message, table_subject};

    fn ident(namespace: &[&str], name: &str) -> TableIdent {
        TableIdent {
            namespace: NamespaceIdent::from_vec(
                namespace.iter().map(|part| (*part).to_owned()).collect(),
            )
            .expect("nonempty namespace"),
            name: name.to_owned(),
        }
    }

    #[test]
    fn table_subject_joins_namespace_levels_with_dots() {
        assert_eq!(
            table_subject(&ident(&["analytics"], "events")),
            "analytics.events"
        );
        assert_eq!(
            table_subject(&ident(&["finance", "daily"], "orders")),
            "finance.daily.orders"
        );
    }

    #[test]
    fn queue_message_matches_cloudflare_http_publish() {
        let event = TableCommitEvent {
            tenant_id: "tenant-a".into(),
            table: "analytics.events".into(),
            metadata_location: "s3://wh/meta.json".into(),
        };
        assert_eq!(
            queue_message(&event),
            serde_json::json!({
                "body": {
                    "eventType": "org.verglas.table.commit",
                    "tenant_id": "tenant-a",
                    "table": "analytics.events",
                    "metadata_location": "s3://wh/meta.json",
                }
            })
        );
    }
}
