//! `verglas vector` — vector bucket/index ingestion and nearest-neighbor
//! search over the S3 Vectors semantic listener (issue #144). Every verb
//! wraps the pre-signed `verglas_sdk::semantic::S3VectorsClient`; this module
//! builds typed DTOs from parsed CLI/stdin JSON and renders the response. It
//! signs nothing.

use std::error::Error;

use verglas_sdk::{
    CreateIndexInput, CreateVectorBucketInput, DataType, DeleteIndexInput, DeleteVectorBucketInput,
    DeleteVectorsInput, DistanceMetric, GetVectorsInput, ListIndexesInput, ListVectorBucketsInput,
    ListVectorsInput, PutInputVector, PutVectorsInput, QueryVectorsInput, VectorData,
};

use crate::cli::{
    VectorBucketArgs, VectorCommand, VectorCreateIndexArgs, VectorIndexArgs, VectorInputArgs,
    VectorKeysArgs, VectorMetric, VectorQueryArgs,
};
use crate::commands::semantic::{read_json_input, vector_client};

/// Dispatches a `verglas vector` subcommand against the S3 Vectors semantic listener.
pub async fn run(
    command: VectorCommand,
    s3_endpoint: &str,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let client = vector_client(s3_endpoint)?;
    match command {
        VectorCommand::CreateBucket(args) => create_bucket(&client, args, json).await,
        VectorCommand::CreateIndex(args) => create_index(&client, args, json).await,
        VectorCommand::Put(args) => put(&client, args, json).await,
        VectorCommand::Query(args) => query(&client, args, json).await,
        VectorCommand::List(args) => list(&client, args, json).await,
        VectorCommand::Get(args) => get(&client, args, json).await,
        VectorCommand::Delete(args) => delete(&client, args, json).await,
        VectorCommand::DeleteIndex(args) => delete_index(&client, args, json).await,
        VectorCommand::DeleteBucket(args) => delete_bucket(&client, args, json).await,
        VectorCommand::ListBuckets => list_buckets(&client, json).await,
        VectorCommand::ListIndexes(args) => list_indexes(&client, args, json).await,
    }
}

/// Creates a vector bucket.
async fn create_bucket(
    client: &verglas_sdk::S3VectorsClient,
    args: VectorBucketArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let output = client
        .create_vector_bucket(CreateVectorBucketInput {
            vector_bucket_name: args.bucket.clone(),
            encryption_configuration: None,
            tags: None,
        })
        .await?;
    emit(json, &output, |_| {
        println!("created vector bucket {}", args.bucket)
    })
}

/// Creates a vector index with the given dimension and distance metric.
async fn create_index(
    client: &verglas_sdk::S3VectorsClient,
    args: VectorCreateIndexArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let distance_metric = match args.metric {
        VectorMetric::Cosine => DistanceMetric::Cosine,
        VectorMetric::Euclidean => DistanceMetric::Euclidean,
    };
    let output = client
        .create_index(CreateIndexInput {
            vector_bucket_name: Some(args.bucket.clone()),
            vector_bucket_arn: None,
            index_name: args.index.clone(),
            data_type: DataType::Float32,
            dimension: args.dimension,
            distance_metric,
            metadata_configuration: None,
            encryption_configuration: None,
            tags: None,
        })
        .await?;
    emit(json, &output, |_| {
        println!("created index {}/{}", args.bucket, args.index)
    })
}

/// Reads a JSON array of vectors (file or stdin) and puts them into the index.
async fn put(
    client: &verglas_sdk::S3VectorsClient,
    args: VectorInputArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let vectors: Vec<PutInputVector> = serde_json::from_str(&read_json_input(&args.input)?)?;
    let count = vectors.len();
    let output = client
        .put_vectors(PutVectorsInput {
            vector_bucket_name: Some(args.bucket.clone()),
            index_name: Some(args.index.clone()),
            index_arn: None,
            vectors,
        })
        .await?;
    emit(json, &output, |_| {
        println!("put {count} vector(s) into {}/{}", args.bucket, args.index)
    })
}

/// Queries the nearest vectors to `--query-vector`.
async fn query(
    client: &verglas_sdk::S3VectorsClient,
    args: VectorQueryArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let values: Vec<f32> = serde_json::from_str(&args.query_vector)?;
    let output = client
        .query_vectors(QueryVectorsInput {
            vector_bucket_name: Some(args.bucket.clone()),
            index_name: Some(args.index.clone()),
            index_arn: None,
            top_k: args.top_k,
            query_vector: VectorData::Float32 { float32: values },
            filter: None,
            return_metadata: Some(true),
            return_distance: Some(true),
            next_token: None,
        })
        .await?;
    emit(json, &output, |output| {
        if output.vectors.is_empty() {
            println!("(no results)");
        } else {
            for vector in &output.vectors {
                let distance = vector
                    .distance
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "-".to_owned());
                println!("  {} (distance={distance})", vector.key);
            }
        }
    })
}

/// Lists vectors in an index.
async fn list(
    client: &verglas_sdk::S3VectorsClient,
    args: VectorIndexArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let output = client
        .list_vectors(ListVectorsInput {
            vector_bucket_name: Some(args.bucket.clone()),
            index_name: Some(args.index.clone()),
            index_arn: None,
            max_results: None,
            next_token: None,
            segment_count: None,
            segment_index: None,
            return_data: Some(false),
            return_metadata: Some(true),
        })
        .await?;
    emit(json, &output, |output| {
        if output.vectors.is_empty() {
            println!("(no vectors)");
        } else {
            for vector in &output.vectors {
                println!("{}", vector.key);
            }
        }
    })
}

/// Gets vectors by key.
async fn get(
    client: &verglas_sdk::S3VectorsClient,
    args: VectorKeysArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let output = client
        .get_vectors(GetVectorsInput {
            vector_bucket_name: Some(args.bucket.clone()),
            index_name: Some(args.index.clone()),
            index_arn: None,
            keys: args.keys.clone(),
            return_data: Some(true),
            return_metadata: Some(true),
        })
        .await?;
    emit(json, &output, |output| {
        if output.vectors.is_empty() {
            println!("(no vectors)");
        } else {
            for vector in &output.vectors {
                println!("{}", vector.key);
            }
        }
    })
}

/// Deletes vectors by key.
async fn delete(
    client: &verglas_sdk::S3VectorsClient,
    args: VectorKeysArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let count = args.keys.len();
    let output = client
        .delete_vectors(DeleteVectorsInput {
            vector_bucket_name: Some(args.bucket.clone()),
            index_name: Some(args.index.clone()),
            index_arn: None,
            keys: args.keys,
        })
        .await?;
    emit(json, &output, |_| {
        println!(
            "deleted {count} vector(s) from {}/{}",
            args.bucket, args.index
        )
    })
}

/// Deletes a vector index.
async fn delete_index(
    client: &verglas_sdk::S3VectorsClient,
    args: VectorIndexArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let output = client
        .delete_index(DeleteIndexInput {
            vector_bucket_name: Some(args.bucket.clone()),
            index_name: Some(args.index.clone()),
            index_arn: None,
        })
        .await?;
    emit(json, &output, |_| {
        println!("deleted index {}/{}", args.bucket, args.index)
    })
}

/// Deletes a vector bucket.
async fn delete_bucket(
    client: &verglas_sdk::S3VectorsClient,
    args: VectorBucketArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let output = client
        .delete_vector_bucket(DeleteVectorBucketInput {
            vector_bucket_name: Some(args.bucket.clone()),
            vector_bucket_arn: None,
        })
        .await?;
    emit(json, &output, |_| {
        println!("deleted vector bucket {}", args.bucket)
    })
}

/// Lists every vector bucket.
async fn list_buckets(
    client: &verglas_sdk::S3VectorsClient,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let output = client
        .list_vector_buckets(ListVectorBucketsInput {
            max_results: None,
            next_token: None,
            prefix: None,
        })
        .await?;
    emit(json, &output, |output| {
        if output.vector_buckets.is_empty() {
            println!("(no vector buckets)");
        } else {
            for bucket in &output.vector_buckets {
                println!("{}", bucket.vector_bucket_name);
            }
        }
    })
}

/// Lists every index in a bucket.
async fn list_indexes(
    client: &verglas_sdk::S3VectorsClient,
    args: VectorBucketArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let output = client
        .list_indexes(ListIndexesInput {
            vector_bucket_name: Some(args.bucket.clone()),
            vector_bucket_arn: None,
            max_results: None,
            next_token: None,
            prefix: None,
        })
        .await?;
    emit(json, &output, |output| {
        if output.indexes.is_empty() {
            println!("(no indexes)");
        } else {
            for index in &output.indexes {
                println!("{}", index.index_name);
            }
        }
    })
}

/// Serializes `output` as pretty JSON when `json` is set, else calls `render`.
fn emit<T, F>(json: bool, output: &T, render: F) -> Result<(), Box<dyn Error>>
where
    T: serde::Serialize,
    F: FnOnce(&T),
{
    if json {
        println!("{}", serde_json::to_string_pretty(output)?);
    } else {
        render(output);
    }
    Ok(())
}
