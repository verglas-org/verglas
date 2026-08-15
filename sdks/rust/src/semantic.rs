//! Direct SigV4 clients for the cache listener semantic REST-JSON APIs.
//!
//! The service models own operation paths. These clients use them directly and
//! never send semantic requests through a bearer-token platform endpoint.

use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::{Client, Method};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::{ClientError, semantic_types::*};

/// Credentials used to sign cache-listener semantic requests with AWS SigV4.
#[derive(Debug, Clone)]
pub struct SigV4Credentials {
    /// Credential identifier configured on the cache node.
    pub access_key_id: String,
    /// Secret used only to derive request signatures.
    pub secret_access_key: String,
    /// AWS-style signing region accepted by the listener.
    pub region: String,
}

impl SigV4Credentials {
    /// Creates credentials for the listener default region.
    pub fn new(access_key_id: impl Into<String>, secret_access_key: impl Into<String>) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            region: "us-east-1".to_owned(),
        }
    }

    /// Selects the region carried in the SigV4 credential scope.
    #[must_use]
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = region.into();
        self
    }
}

/// Shared signed REST-JSON transport for a single semantic service.
#[derive(Debug, Clone)]
struct SemanticClient {
    endpoint: String,
    http: Client,
    credentials: SigV4Credentials,
    service: &'static str,
}

impl SemanticClient {
    /// Builds a transport bound to an S3 listener signing service.
    fn new(
        endpoint: impl Into<String>,
        credentials: SigV4Credentials,
        service: &'static str,
    ) -> Self {
        Self {
            endpoint: endpoint.into().trim_end_matches('/').to_owned(),
            http: Client::new(),
            credentials,
            service,
        }
    }

    /// Signs and sends a single exact model operation.
    async fn call<I: Serialize, O: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        input: &I,
    ) -> Result<O, ClientError> {
        let body = if matches!(method, Method::GET | Method::DELETE) {
            Vec::new()
        } else {
            serde_json::to_vec(&input).map_err(ClientError::NamespaceJson)?
        };
        let timestamp = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let date = &timestamp[..8];
        let url = format!("{}{path}", self.endpoint);
        let parsed = reqwest::Url::parse(&url)
            .map_err(|error| ClientError::Configuration(error.to_string()))?;
        let bare_host = parsed.host_str().ok_or_else(|| {
            ClientError::Configuration("semantic endpoint needs a host".to_owned())
        })?;
        let host = parsed.port().map_or_else(
            || bare_host.to_owned(),
            |port| format!("{bare_host}:{port}"),
        );
        let payload_hash = hex::encode(Sha256::digest(&body));
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let canonical = format!(
            "{method}\n{}\n{}\nhost:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{timestamp}\n\n{signed_headers}\n{payload_hash}",
            canonical_uri(parsed.path()),
            canonical_query(parsed.query().unwrap_or_default())
        );
        let scope = format!(
            "{date}/{}/{}/aws4_request",
            self.credentials.region, self.service
        );
        let text = format!(
            "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical.as_bytes()))
        );
        let signature = signature(
            &self.credentials.secret_access_key,
            date,
            &self.credentials.region,
            self.service,
            &text,
        )?;
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.credentials.access_key_id
        );
        let response = self
            .http
            .request(method, url)
            .header("host", host)
            .header("x-amz-date", timestamp)
            .header("x-amz-content-sha256", payload_hash)
            .header("authorization", authorization)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(ClientError::Transport)?;
        if !response.status().is_success() {
            return Err(ClientError::Http {
                status: response.status(),
                message: response.text().await.unwrap_or_default(),
            });
        }
        response.json().await.map_err(ClientError::Transport)
    }
}

/// Direct client for the exact AWS S3 Vectors REST-JSON surface.
#[derive(Debug, Clone)]
pub struct S3VectorsClient(SemanticClient);

impl S3VectorsClient {
    /// Builds a signed S3 Vectors client for the cache-node S3 listener.
    pub fn new(endpoint: impl Into<String>, credentials: SigV4Credentials) -> Self {
        Self(SemanticClient::new(endpoint, credentials, "s3vectors"))
    }
    /// Returns the required SigV4 signing name.
    pub const fn signing_name(&self) -> &'static str {
        "s3vectors"
    }
    /// Calls CreateIndex.
    pub async fn create_index(
        &self,
        input: CreateIndexInput,
    ) -> Result<CreateIndexOutput, ClientError> {
        self.0.call(Method::POST, "/CreateIndex", &input).await
    }
    /// Calls CreateVectorBucket.
    pub async fn create_vector_bucket(
        &self,
        input: CreateVectorBucketInput,
    ) -> Result<CreateVectorBucketOutput, ClientError> {
        self.0
            .call(Method::POST, "/CreateVectorBucket", &input)
            .await
    }
    /// Calls DeleteIndex.
    pub async fn delete_index(
        &self,
        input: DeleteIndexInput,
    ) -> Result<DeleteIndexOutput, ClientError> {
        self.0.call(Method::POST, "/DeleteIndex", &input).await
    }
    /// Calls DeleteVectorBucket.
    pub async fn delete_vector_bucket(
        &self,
        input: DeleteVectorBucketInput,
    ) -> Result<DeleteVectorBucketOutput, ClientError> {
        self.0
            .call(Method::POST, "/DeleteVectorBucket", &input)
            .await
    }
    /// Calls DeleteVectorBucketPolicy.
    pub async fn delete_vector_bucket_policy(
        &self,
        input: DeleteVectorBucketPolicyInput,
    ) -> Result<DeleteVectorBucketPolicyOutput, ClientError> {
        self.0
            .call(Method::POST, "/DeleteVectorBucketPolicy", &input)
            .await
    }
    /// Calls DeleteVectors.
    pub async fn delete_vectors(
        &self,
        input: DeleteVectorsInput,
    ) -> Result<DeleteVectorsOutput, ClientError> {
        self.0.call(Method::POST, "/DeleteVectors", &input).await
    }
    /// Calls GetIndex.
    pub async fn get_index(&self, input: GetIndexInput) -> Result<GetIndexOutput, ClientError> {
        self.0.call(Method::POST, "/GetIndex", &input).await
    }
    /// Calls GetVectorBucket.
    pub async fn get_vector_bucket(
        &self,
        input: GetVectorBucketInput,
    ) -> Result<GetVectorBucketOutput, ClientError> {
        self.0.call(Method::POST, "/GetVectorBucket", &input).await
    }
    /// Calls GetVectorBucketPolicy.
    pub async fn get_vector_bucket_policy(
        &self,
        input: GetVectorBucketPolicyInput,
    ) -> Result<GetVectorBucketPolicyOutput, ClientError> {
        self.0
            .call(Method::POST, "/GetVectorBucketPolicy", &input)
            .await
    }
    /// Calls GetVectors.
    pub async fn get_vectors(
        &self,
        input: GetVectorsInput,
    ) -> Result<GetVectorsOutput, ClientError> {
        self.0.call(Method::POST, "/GetVectors", &input).await
    }
    /// Calls ListIndexes.
    pub async fn list_indexes(
        &self,
        input: ListIndexesInput,
    ) -> Result<ListIndexesOutput, ClientError> {
        self.0.call(Method::POST, "/ListIndexes", &input).await
    }
    /// Calls ListTagsForResource.
    pub async fn list_tags_for_resource(
        &self,
        input: ListTagsForResourceInput,
    ) -> Result<ListTagsForResourceOutput, ClientError> {
        self.0
            .call(
                Method::GET,
                &format!("/tags/{}", encode(&input.resource_arn)),
                &input,
            )
            .await
    }
    /// Calls ListVectorBuckets.
    pub async fn list_vector_buckets(
        &self,
        input: ListVectorBucketsInput,
    ) -> Result<ListVectorBucketsOutput, ClientError> {
        self.0
            .call(Method::POST, "/ListVectorBuckets", &input)
            .await
    }
    /// Calls ListVectors.
    pub async fn list_vectors(
        &self,
        input: ListVectorsInput,
    ) -> Result<ListVectorsOutput, ClientError> {
        self.0.call(Method::POST, "/ListVectors", &input).await
    }
    /// Calls PutVectorBucketPolicy.
    pub async fn put_vector_bucket_policy(
        &self,
        input: PutVectorBucketPolicyInput,
    ) -> Result<PutVectorBucketPolicyOutput, ClientError> {
        self.0
            .call(Method::POST, "/PutVectorBucketPolicy", &input)
            .await
    }
    /// Calls PutVectors.
    pub async fn put_vectors(
        &self,
        input: PutVectorsInput,
    ) -> Result<PutVectorsOutput, ClientError> {
        self.0.call(Method::POST, "/PutVectors", &input).await
    }
    /// Calls QueryVectors.
    pub async fn query_vectors(
        &self,
        input: QueryVectorsInput,
    ) -> Result<QueryVectorsOutput, ClientError> {
        self.0.call(Method::POST, "/QueryVectors", &input).await
    }
    /// Calls TagResource.
    pub async fn tag_resource(
        &self,
        input: TagResourceInput,
    ) -> Result<TagResourceOutput, ClientError> {
        self.0
            .call(
                Method::POST,
                &format!("/tags/{}", encode(&input.resource_arn)),
                &input,
            )
            .await
    }
    /// Calls UntagResource using repeated tagKeys query fields.
    pub async fn untag_resource(
        &self,
        input: UntagResourceInput,
    ) -> Result<UntagResourceOutput, ClientError> {
        let query = input
            .tag_keys
            .iter()
            .map(|key| format!("tagKeys={}", encode(key)))
            .collect::<Vec<_>>()
            .join("&");
        self.0
            .call(
                Method::DELETE,
                &format!("/tags/{}?{query}", encode(&input.resource_arn)),
                &input,
            )
            .await
    }
}

/// Direct client for the exact Verglas Graph REST-JSON surface.
#[derive(Debug, Clone)]
pub struct VerglasGraphsClient(SemanticClient);

impl VerglasGraphsClient {
    /// Builds a signed Graph client for the cache-node S3 listener.
    pub fn new(endpoint: impl Into<String>, credentials: SigV4Credentials) -> Self {
        Self(SemanticClient::new(endpoint, credentials, "verglasgraphs"))
    }
    /// Returns the required SigV4 signing name.
    pub const fn signing_name(&self) -> &'static str {
        "verglasgraphs"
    }
    /// Calls BuildGraphIndex.
    pub async fn build_graph_index(
        &self,
        input: BuildGraphIndexInput,
    ) -> Result<BuildGraphIndexOutput, ClientError> {
        self.0.call(Method::POST, "/BuildGraphIndex", &input).await
    }
    /// Calls CreateGraph.
    pub async fn create_graph(
        &self,
        input: CreateGraphInput,
    ) -> Result<CreateGraphOutput, ClientError> {
        self.0.call(Method::POST, "/CreateGraph", &input).await
    }
    /// Calls DeleteGraph.
    pub async fn delete_graph(
        &self,
        input: DeleteGraphInput,
    ) -> Result<DeleteGraphOutput, ClientError> {
        self.0.call(Method::POST, "/DeleteGraph", &input).await
    }
    /// Calls GetGraph.
    pub async fn get_graph(&self, input: GetGraphInput) -> Result<GetGraphOutput, ClientError> {
        self.0.call(Method::POST, "/GetGraph", &input).await
    }
    /// Calls GetNeighbors.
    pub async fn get_neighbors(
        &self,
        input: GetNeighborsInput,
    ) -> Result<GetNeighborsOutput, ClientError> {
        self.0.call(Method::POST, "/GetNeighbors", &input).await
    }
    /// Calls ListGraphs.
    pub async fn list_graphs(
        &self,
        input: ListGraphsInput,
    ) -> Result<ListGraphsOutput, ClientError> {
        self.0.call(Method::POST, "/ListGraphs", &input).await
    }
    /// Calls PutEdges.
    pub async fn put_edges(&self, input: PutEdgesInput) -> Result<PutEdgesOutput, ClientError> {
        self.0.call(Method::POST, "/PutEdges", &input).await
    }
    /// Calls PutNodes.
    pub async fn put_nodes(&self, input: PutNodesInput) -> Result<PutNodesOutput, ClientError> {
        self.0.call(Method::POST, "/PutNodes", &input).await
    }
    /// Calls QueryKHop.
    pub async fn query_k_hop(&self, input: QueryKHopInput) -> Result<QueryKHopOutput, ClientError> {
        self.0.call(Method::POST, "/QueryKHop", &input).await
    }
    /// Calls QueryNeighborhood.
    pub async fn query_neighborhood(
        &self,
        input: QueryNeighborhoodInput,
    ) -> Result<QueryNeighborhoodOutput, ClientError> {
        self.0
            .call(Method::POST, "/QueryNeighborhood", &input)
            .await
    }
    /// Calls QueryPaths.
    pub async fn query_paths(
        &self,
        input: QueryPathsInput,
    ) -> Result<QueryPathsOutput, ClientError> {
        self.0.call(Method::POST, "/QueryPaths", &input).await
    }
    /// Calls QueryPrecedents.
    pub async fn query_precedents(
        &self,
        input: QueryPrecedentsInput,
    ) -> Result<QueryPrecedentsOutput, ClientError> {
        self.0.call(Method::POST, "/QueryPrecedents", &input).await
    }
}

/// Computes a hexadecimal SigV4 request signature.
fn signature(
    secret: &str,
    date: &str,
    region: &str,
    service: &str,
    text: &str,
) -> Result<String, ClientError> {
    let date_key = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes())?;
    let region_key = hmac(&date_key, region.as_bytes())?;
    let service_key = hmac(&region_key, service.as_bytes())?;
    let signing_key = hmac(&service_key, b"aws4_request")?;
    Ok(hex::encode(hmac(&signing_key, text.as_bytes())?))
}

/// Computes an HMAC-SHA256 digest without panicking.
fn hmac(key: &[u8], value: &[u8]) -> Result<Vec<u8>, ClientError> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key)
        .map_err(|error| ClientError::Configuration(error.to_string()))?;
    mac.update(value);
    Ok(mac.finalize().into_bytes().to_vec())
}

/// Percent-encodes a model URI binding.
fn encode(value: &str) -> String {
    canonical_component(value)
}

/// Canonicalizes a URI path by decoding transport escapes then AWS-encoding components.
fn canonical_uri(path: &str) -> String {
    (if path.is_empty() { "/" } else { path })
        .split('/')
        .map(canonical_component)
        .collect::<Vec<_>>()
        .join("/")
}

/// Orders decoded-and-re-encoded query fields according to SigV4 byte ordering.
fn canonical_query(query: &str) -> String {
    let mut pairs = query
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| match part.split_once('=') {
            Some((key, value)) => (canonical_component(key), canonical_component(value)),
            None => (canonical_component(part), String::new()),
        })
        .collect::<Vec<_>>();
    pairs.sort_unstable();
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// AWS-encodes one path or query component after decoding transport escapes.
fn canonical_component(value: &str) -> String {
    percent_encoding::percent_decode_str(value)
        .collect::<Vec<_>>()
        .into_iter()
        .fold(String::new(), |mut output, byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
                output.push(char::from(byte));
            } else {
                output.push_str(&format!("%{byte:02X}"));
            }
            output
        })
}

#[cfg(test)]
mod tests {
    use super::{canonical_query, canonical_uri, encode};

    /// Keeps SDK canonicalization byte-for-byte aligned with listener SigV4 rules.
    #[test]
    fn canonicalizes_aws_path_and_repeated_query_components() {
        assert_eq!(canonical_uri("/tags/a-b%2Fc_~.x"), "/tags/a-b%2Fc_~.x");
        assert_eq!(
            canonical_query("tagKeys=z%20x&tagKeys=a-b&tagKeys=a%2Fb"),
            "tagKeys=a%2Fb&tagKeys=a-b&tagKeys=z%20x"
        );
        assert_eq!(
            encode("arn:aws:s3vectors:a-b"),
            "arn%3Aaws%3As3vectors%3Aa-b"
        );
    }
}
