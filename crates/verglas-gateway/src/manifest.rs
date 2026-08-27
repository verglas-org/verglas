//! Strict parsing for the Wrangler manifest and product deployment contract.
//!
//! Durable-object namespaces, Stream, Vectorize, Graph, and Query bindings, system product
//! services, and the exact runtime host capability are separate maps. Each selected product resolves to
//! one immutable artifact; unknown fields fail before process launch.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};
use thiserror::Error;

/// One named Durable Object binding from the manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Binding {
    name: String,
    class_name: String,
    origin: Option<String>,
}

impl Binding {
    /// Returns the binding name used in gateway URLs.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the guest class name selected by the build pipeline.
    pub fn class_name(&self) -> &str {
        &self.class_name
    }

    /// Returns the remote Worker origin when this namespace lives in another microVM.
    pub fn origin(&self) -> Option<&str> {
        self.origin.as_deref()
    }
}

/// One Wrangler `pipelines` binding targeting the prebuilt Stream deployment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PipelineBinding {
    binding: String,
    stream: String,
    origin: Option<String>,
}

/// One Wrangler `vectorize` binding targeting a prebuilt Vectorize index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorizeBinding {
    binding: String,
    index_name: String,
    origin: Option<String>,
}

/// One Verglas `graphs` binding targeting a prebuilt named property graph.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphBinding {
    binding: String,
    graph_name: String,
    origin: Option<String>,
}

/// One Verglas `queries` binding targeting a named Query materialization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryBinding {
    binding: String,
    query_name: String,
    origin: Option<String>,
}

impl QueryBinding {
    /// Returns the environment binding exposed by the Worker shim.
    pub fn binding(&self) -> &str {
        &self.binding
    }
    /// Returns the fixed named Query identity.
    pub fn query_name(&self) -> &str {
        &self.query_name
    }
    /// Returns the remote Query Worker origin, if split from this Worker.
    pub fn origin(&self) -> Option<&str> {
        self.origin.as_deref()
    }
}

impl GraphBinding {
    /// Returns the environment binding name exposed by the Worker shim.
    pub fn binding(&self) -> &str {
        &self.binding
    }

    /// Returns the fixed named graph identity.
    pub fn graph_name(&self) -> &str {
        &self.graph_name
    }

    /// Returns the remote Graph Worker origin, if split from this Worker.
    pub fn origin(&self) -> Option<&str> {
        self.origin.as_deref()
    }
}

impl VectorizeBinding {
    /// Returns the environment binding name exposed by the Worker shim.
    pub fn binding(&self) -> &str {
        &self.binding
    }

    /// Returns the fixed named Vectorize index identity.
    pub fn index_name(&self) -> &str {
        &self.index_name
    }

    /// Returns the remote Vectorize Worker origin, if split from this Worker.
    pub fn origin(&self) -> Option<&str> {
        self.origin.as_deref()
    }
}

impl PipelineBinding {
    /// Returns the environment binding name exposed by the Worker shim.
    pub fn binding(&self) -> &str {
        &self.binding
    }

    /// Returns the Stream object identity used by `PipelineBinding.send`.
    pub fn stream(&self) -> &str {
        &self.stream
    }

    /// Returns the remote Stream Worker origin, if split from this Worker.
    pub fn origin(&self) -> Option<&str> {
        self.origin.as_deref()
    }
}

/// The nine product artifact identities in a deployment.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ArtifactProduct {
    /// Stateless public Worker artifact.
    Worker,
    /// Tenant stateful Durable Object artifact.
    DurableObject,
    /// Ordered JSON Stream artifact.
    Stream,
    /// Stream-consuming Pipeline artifact.
    Pipeline,
    /// Idempotent delivery Sink artifact.
    Sink,
    /// Iceberg REST Catalog artifact.
    Catalog,
    /// Turso-backed Cloudflare Vectorize artifact.
    Vectorize,
    /// Turso-backed bounded property-graph artifact.
    Graph,
    /// Turso-backed Pipeline materialization and bounded query artifact.
    Query,
}

impl ArtifactProduct {
    /// Returns the strict manifest key for this product.
    pub const fn manifest_key(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::DurableObject => "durable_object",
            Self::Stream => "stream",
            Self::Pipeline => "pipeline",
            Self::Sink => "sink",
            Self::Catalog => "catalog",
            Self::Vectorize => "vectorize",
            Self::Graph => "graph",
            Self::Query => "query",
        }
    }
}

/// One immutable, digest-addressed WASM artifact descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactDescriptor {
    digest: String,
    component_dir: PathBuf,
    cwasm_cache_dir: Option<PathBuf>,
}

impl ArtifactDescriptor {
    /// Returns the verified SHA-256 digest used to select the component.
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Returns the directory containing this product's digest-named component.
    pub fn component_dir(&self) -> &Path {
        &self.component_dir
    }

    /// Returns the optional compiled component cache directory.
    pub fn cwasm_cache_dir(&self) -> Option<&Path> {
        self.cwasm_cache_dir.as_deref()
    }
}

/// One named system product service binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SystemBinding {
    binding: String,
    product: ArtifactProduct,
    object: String,
    origin: Option<String>,
}

impl SystemBinding {
    /// Returns the environment binding name exposed to the caller.
    pub fn binding(&self) -> &str {
        &self.binding
    }

    /// Returns the prebuilt product selected by this service binding.
    pub fn product(&self) -> ArtifactProduct {
        self.product
    }

    /// Returns the strict wire product name from the manifest.
    pub fn service(&self) -> &'static str {
        self.product.manifest_key()
    }

    /// Returns the named product object identity.
    pub fn object(&self) -> &str {
        &self.object
    }

    /// Returns the remote system Worker origin, if split from this Worker.
    pub fn origin(&self) -> Option<&str> {
        self.origin.as_deref()
    }
}

/// One declared privileged service intercepted by the resident runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostServiceBinding {
    binding: String,
    service: String,
}

impl HostServiceBinding {
    /// Returns the exact guest environment binding name.
    pub fn binding(&self) -> &str {
        &self.binding
    }

    /// Returns the infrastructure service target.
    pub fn service(&self) -> &str {
        &self.service
    }
}

/// One accepted Durable Object migration declaration from a Wrangler manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Migration {
    tag: String,
    new_classes: Vec<String>,
    new_sqlite_classes: Vec<String>,
}

impl Migration {
    /// Returns the migration tag used to order deployment changes.
    pub fn tag(&self) -> &str {
        &self.tag
    }

    /// Returns classes introduced with the default Durable Object storage kind.
    pub fn new_classes(&self) -> &[String] {
        &self.new_classes
    }

    /// Returns classes introduced with SQLite-backed Durable Object storage.
    pub fn new_sqlite_classes(&self) -> &[String] {
        &self.new_sqlite_classes
    }
}

/// The product selected after a binding identity check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BindingTarget {
    product: ArtifactProduct,
}

/// Borrowed binding namespaces passed together through cross-namespace validation.
struct BindingNamespaces<'a> {
    durable_objects: &'a [Binding],
    pipelines: &'a [PipelineBinding],
    vectorize: &'a [VectorizeBinding],
    graphs: &'a [GraphBinding],
    queries: &'a [QueryBinding],
    services: &'a [SystemBinding],
    host_services: &'a [HostServiceBinding],
}

/// The validated subset of a Wrangler-style deployment manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Manifest {
    name: String,
    main: String,
    compatibility_date: Option<String>,
    compatibility_flags: Vec<String>,
    crons: Vec<String>,
    bindings: Vec<Binding>,
    pipelines: Vec<PipelineBinding>,
    vectorize: Vec<VectorizeBinding>,
    graphs: Vec<GraphBinding>,
    queries: Vec<QueryBinding>,
    services: Vec<SystemBinding>,
    host_services: Vec<HostServiceBinding>,
    migrations: Vec<Migration>,
    vars: Map<String, Value>,
    artifacts: BTreeMap<ArtifactProduct, ArtifactDescriptor>,
    data_root: PathBuf,
}

impl Manifest {
    /// Parses one JSON or JSONC document without consulting the filesystem.
    pub fn parse(source: &str) -> Result<Self, ManifestError> {
        let source = strip_jsonc(source)?;
        let value = serde_json::from_str::<Value>(&source).map_err(|source| {
            ManifestError::InvalidJson {
                message: source.to_string(),
            }
        })?;
        Self::from_value(value)
    }

    /// Reads and parses a `.json` or `.jsonc` manifest file.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ManifestError> {
        let path = path.as_ref();
        let extension = path.extension().and_then(|value| value.to_str());
        if !matches!(extension, Some(value) if value.eq_ignore_ascii_case("json") || value.eq_ignore_ascii_case("jsonc"))
        {
            return Err(ManifestError::UnsupportedExtension {
                path: path.to_path_buf(),
            });
        }
        let source = std::fs::read_to_string(path).map_err(|source| ManifestError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Self::parse(&source)
    }

    /// Returns the deployment name from the manifest.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the build entry point, which the gateway does not execute.
    pub fn main(&self) -> &str {
        &self.main
    }

    /// Returns the optional Cloudflare compatibility date.
    pub fn compatibility_date(&self) -> Option<&str> {
        self.compatibility_date.as_deref()
    }

    /// Returns compatibility flags in manifest order.
    pub fn compatibility_flags(&self) -> &[String] {
        &self.compatibility_flags
    }

    /// Returns scheduled cron expressions in manifest order.
    pub fn crons(&self) -> &[String] {
        &self.crons
    }

    /// Returns all declared Durable Object bindings in manifest order.
    pub fn bindings(&self) -> &[Binding] {
        &self.bindings
    }

    /// Returns all declared system Stream bindings in manifest order.
    pub fn pipelines(&self) -> &[PipelineBinding] {
        &self.pipelines
    }

    /// Returns all declared Vectorize bindings in manifest order.
    pub fn vectorize_bindings(&self) -> &[VectorizeBinding] {
        &self.vectorize
    }

    /// Returns all declared Graph bindings in manifest order.
    pub fn graph_bindings(&self) -> &[GraphBinding] {
        &self.graphs
    }

    /// Returns all declared Query bindings in manifest order.
    pub fn query_bindings(&self) -> &[QueryBinding] {
        &self.queries
    }

    /// Returns all declared Pipeline, Sink, and Catalog service bindings.
    pub fn services(&self) -> &[SystemBinding] {
        &self.services
    }

    /// Returns declared privileged services intercepted by `verglas-runtime`.
    pub fn host_services(&self) -> &[HostServiceBinding] {
        &self.host_services
    }

    /// Returns the immutable artifact descriptor for one product.
    pub fn artifact_for_product(
        &self,
        product: ArtifactProduct,
    ) -> Result<&ArtifactDescriptor, ManifestError> {
        self.artifacts
            .get(&product)
            .ok_or(ManifestError::MissingArtifact {
                product: product.manifest_key(),
            })
    }

    /// Resolves the product selected by one binding and object identity.
    pub fn product_for_binding(
        &self,
        binding: &str,
        object: &str,
    ) -> Result<ArtifactProduct, ManifestError> {
        self.binding_target(binding, object)
            .map(|target| target.product)
    }

    /// Resolves a remote Worker origin after enforcing the binding/object identity.
    pub fn origin_for_binding(
        &self,
        binding: &str,
        object: &str,
    ) -> Result<Option<&str>, ManifestError> {
        if let Some(item) = self.bindings.iter().find(|item| item.name == binding) {
            return Ok(item.origin());
        }
        if let Some(item) = self.pipelines.iter().find(|item| item.binding == binding) {
            if item.stream != object {
                return Err(ManifestError::WrongBindingObject {
                    binding: binding.to_owned(),
                    expected: item.stream.clone(),
                    actual: object.to_owned(),
                });
            }
            return Ok(item.origin());
        }
        if let Some(item) = self.vectorize.iter().find(|item| item.binding == binding) {
            if item.index_name != object {
                return Err(ManifestError::WrongBindingObject {
                    binding: binding.to_owned(),
                    expected: item.index_name.clone(),
                    actual: object.to_owned(),
                });
            }
            return Ok(item.origin());
        }
        if let Some(item) = self.graphs.iter().find(|item| item.binding == binding) {
            if item.graph_name != object {
                return Err(ManifestError::WrongBindingObject {
                    binding: binding.to_owned(),
                    expected: item.graph_name.clone(),
                    actual: object.to_owned(),
                });
            }
            return Ok(item.origin());
        }
        if let Some(item) = self.queries.iter().find(|item| item.binding == binding) {
            if item.query_name != object {
                return Err(ManifestError::WrongBindingObject {
                    binding: binding.to_owned(),
                    expected: item.query_name.clone(),
                    actual: object.to_owned(),
                });
            }
            return Ok(item.origin());
        }
        if let Some(item) = self.services.iter().find(|item| item.binding == binding) {
            if item.object != object {
                return Err(ManifestError::WrongBindingObject {
                    binding: binding.to_owned(),
                    expected: item.object.clone(),
                    actual: object.to_owned(),
                });
            }
            return Ok(item.origin());
        }
        Err(ManifestError::UnknownBinding {
            binding: binding.to_owned(),
        })
    }

    /// Resolves the immutable artifact selected by one binding and object identity.
    pub fn artifact_for_binding(
        &self,
        binding: &str,
        object: &str,
    ) -> Result<&ArtifactDescriptor, ManifestError> {
        let product = self.product_for_binding(binding, object)?;
        self.artifact_for_product(product)
    }

    /// Returns accepted migration declarations in manifest order.
    pub fn migrations(&self) -> &[Migration] {
        &self.migrations
    }

    /// Returns worker environment values exactly as declared by Wrangler.
    pub fn vars(&self) -> &Map<String, Value> {
        &self.vars
    }

    /// Looks up one binding in the durable-object namespace only.
    pub fn binding(&self, name: &str) -> Option<&Binding> {
        self.bindings.iter().find(|binding| binding.name == name)
    }

    /// Looks up one system pipeline binding without merging namespaces.
    pub fn pipeline(&self, name: &str) -> Option<&PipelineBinding> {
        self.pipelines
            .iter()
            .find(|pipeline| pipeline.binding == name)
    }

    /// Looks up one Vectorize binding without merging it with DO namespaces.
    pub fn vectorize(&self, name: &str) -> Option<&VectorizeBinding> {
        self.vectorize.iter().find(|item| item.binding == name)
    }

    /// Looks up one Graph binding without merging it with DO namespaces.
    pub fn graph(&self, name: &str) -> Option<&GraphBinding> {
        self.graphs.iter().find(|item| item.binding == name)
    }

    /// Looks up one Query binding without merging it with DO namespaces.
    pub fn query(&self, name: &str) -> Option<&QueryBinding> {
        self.queries.iter().find(|item| item.binding == name)
    }

    /// Resolves one binding while enforcing its declared object identity.
    fn binding_target(&self, binding: &str, object: &str) -> Result<BindingTarget, ManifestError> {
        if self.bindings.iter().any(|item| item.name == binding) {
            return Ok(BindingTarget {
                product: ArtifactProduct::DurableObject,
            });
        }
        if let Some(stream) = self.pipelines.iter().find(|item| item.binding == binding) {
            if stream.stream != object {
                return Err(ManifestError::WrongBindingObject {
                    binding: binding.to_owned(),
                    expected: stream.stream.clone(),
                    actual: object.to_owned(),
                });
            }
            return Ok(BindingTarget {
                product: ArtifactProduct::Stream,
            });
        }
        if let Some(index) = self.vectorize.iter().find(|item| item.binding == binding) {
            if index.index_name != object {
                return Err(ManifestError::WrongBindingObject {
                    binding: binding.to_owned(),
                    expected: index.index_name.clone(),
                    actual: object.to_owned(),
                });
            }
            return Ok(BindingTarget {
                product: ArtifactProduct::Vectorize,
            });
        }
        if let Some(graph) = self.graphs.iter().find(|item| item.binding == binding) {
            if graph.graph_name != object {
                return Err(ManifestError::WrongBindingObject {
                    binding: binding.to_owned(),
                    expected: graph.graph_name.clone(),
                    actual: object.to_owned(),
                });
            }
            return Ok(BindingTarget {
                product: ArtifactProduct::Graph,
            });
        }
        if let Some(query) = self.queries.iter().find(|item| item.binding == binding) {
            if query.query_name != object {
                return Err(ManifestError::WrongBindingObject {
                    binding: binding.to_owned(),
                    expected: query.query_name.clone(),
                    actual: object.to_owned(),
                });
            }
            return Ok(BindingTarget {
                product: ArtifactProduct::Query,
            });
        }
        if let Some(service) = self.services.iter().find(|item| item.binding == binding) {
            if service.object != object {
                return Err(ManifestError::WrongBindingObject {
                    binding: binding.to_owned(),
                    expected: service.object.clone(),
                    actual: object.to_owned(),
                });
            }
            return Ok(BindingTarget {
                product: service.product,
            });
        }
        Err(ManifestError::UnknownBinding {
            binding: binding.to_owned(),
        })
    }

    /// Returns the manifest's runtime data root.
    pub fn data_root(&self) -> &Path {
        &self.data_root
    }

    /// Builds a validated manifest from a parsed JSON value.
    fn from_value(value: Value) -> Result<Self, ManifestError> {
        let Value::Object(mut object) = value else {
            return Err(ManifestError::RootNotObject);
        };
        let allowed = [
            "name",
            "main",
            "compatibility_date",
            "compatibility_flags",
            "triggers",
            "durable_objects",
            "pipelines",
            "vectorize",
            "graphs",
            "queries",
            "services",
            "migrations",
            "vars",
            "artifacts",
            "data_root",
        ]
        .into_iter()
        .collect::<HashSet<_>>();
        if let Some(key) = object
            .keys()
            .find(|key| !allowed.contains(key.as_str()))
            .cloned()
        {
            return Err(ManifestError::UnknownTopLevelKey { key });
        }

        let name = required_string(&mut object, "name")?;
        let main = required_string(&mut object, "main")?;
        let compatibility_date = object
            .remove("compatibility_date")
            .map(|value| parse_nonempty_string(value, "compatibility_date"))
            .transpose()?;
        let compatibility_flags = object
            .remove("compatibility_flags")
            .map(|value| parse_string_array(value, "compatibility_flags"))
            .transpose()?
            .unwrap_or_default();
        let crons = object
            .remove("triggers")
            .map(parse_triggers)
            .transpose()?
            .unwrap_or_default();
        let durable_objects = required_object(&mut object, "durable_objects")?;
        let bindings = parse_durable_objects(durable_objects)?;
        let pipelines = object
            .remove("pipelines")
            .map(parse_pipelines)
            .transpose()?
            .unwrap_or_default();
        let vectorize = object
            .remove("vectorize")
            .map(parse_vectorize)
            .transpose()?
            .unwrap_or_default();
        let graphs = object
            .remove("graphs")
            .map(parse_graphs)
            .transpose()?
            .unwrap_or_default();
        let queries = object
            .remove("queries")
            .map(parse_queries)
            .transpose()?
            .unwrap_or_default();
        let (services, host_services) = object
            .remove("services")
            .map(parse_services)
            .transpose()?
            .unwrap_or_default();
        let namespaces = BindingNamespaces {
            durable_objects: &bindings,
            pipelines: &pipelines,
            vectorize: &vectorize,
            graphs: &graphs,
            queries: &queries,
            services: &services,
            host_services: &host_services,
        };
        reject_binding_collisions(&namespaces)?;
        let migrations = object
            .remove("migrations")
            .map(parse_migrations)
            .transpose()?
            .unwrap_or_default();
        let vars = object
            .remove("vars")
            .map(parse_vars)
            .transpose()?
            .unwrap_or_default();
        let artifacts = required_object(&mut object, "artifacts").and_then(parse_artifacts)?;
        require_artifacts(&artifacts, &namespaces)?;
        let data_root = PathBuf::from(required_string(&mut object, "data_root")?);
        Ok(Self {
            name,
            main,
            compatibility_date,
            compatibility_flags,
            crons,
            bindings,
            pipelines,
            vectorize,
            graphs,
            queries,
            services,
            host_services,
            migrations,
            vars,
            artifacts,
            data_root,
        })
    }
}

/// A manifest document was malformed or violated the strict prototype schema.
#[derive(Debug, Error)]
pub enum ManifestError {
    /// Reading a manifest file failed.
    #[error("failed to read manifest {path}: {source}")]
    Io {
        /// Path that could not be read.
        path: PathBuf,
        /// Filesystem error returned by the operating system.
        #[source]
        source: std::io::Error,
    },
    /// The input path is not one of the supported manifest extensions.
    #[error("manifest {path} must have a .json or .jsonc extension")]
    UnsupportedExtension {
        /// Path whose extension was rejected.
        path: PathBuf,
    },
    /// JSON syntax or JSONC syntax after comment removal was invalid.
    #[error("manifest JSON is invalid: {message}")]
    InvalidJson {
        /// Parser-provided syntax detail.
        message: String,
    },
    /// A block comment was not terminated.
    #[error("manifest JSONC has an unterminated block comment")]
    UnterminatedComment,
    /// The document root must be a JSON object.
    #[error("manifest root must be a JSON object")]
    RootNotObject,
    /// A top-level key is outside the prototype manifest subset.
    #[error("unknown top-level manifest key: {key}")]
    UnknownTopLevelKey {
        /// Key that was not recognized.
        key: String,
    },
    /// A scheduled-trigger object contains an unsupported key.
    #[error("unknown triggers manifest key: {key}")]
    UnknownTriggersKey {
        /// Key that was not recognized.
        key: String,
    },
    /// A nested durable-object section contains an unknown key.
    #[error("unknown durable_objects manifest key: {key}")]
    UnknownDurableObjectsKey {
        /// Key that was not recognized.
        key: String,
    },
    /// A binding object contains an unknown key.
    #[error("unknown durable_objects.bindings key: {key}")]
    UnknownBindingKey {
        /// Key that was not recognized.
        key: String,
    },
    /// A pipeline object contains an unknown key.
    #[error("unknown pipelines manifest key: {key}")]
    UnknownPipelineKey {
        /// Key that was not recognized.
        key: String,
    },
    /// A Vectorize binding contains an unknown key.
    #[error("unknown vectorize manifest key: {key}")]
    UnknownVectorizeKey {
        /// Key that was not recognized.
        key: String,
    },
    /// A Graph binding contains an unknown key.
    #[error("unknown graphs manifest key: {key}")]
    UnknownGraphKey {
        /// Key that was not recognized.
        key: String,
    },
    /// A Query binding contains an unknown key.
    #[error("unknown queries manifest key: {key}")]
    UnknownQueryKey {
        /// Key that was not recognized.
        key: String,
    },
    /// A system service binding contains an unknown key.
    #[error("unknown services manifest key: {key}")]
    UnknownServiceKey {
        /// Key that was not recognized.
        key: String,
    },
    /// A system service binding names an unsupported product.
    #[error("unknown service product: {product}")]
    UnknownServiceProduct {
        /// Product string that was not one of the nine products.
        product: String,
    },
    /// The artifacts object contains an unknown product key.
    #[error("unknown artifacts manifest key: {key}")]
    UnknownArtifactKey {
        /// Key that was not recognized.
        key: String,
    },
    /// An artifact descriptor contains an unknown key.
    #[error("unknown artifact descriptor key: {key}")]
    UnknownArtifactDescriptorKey {
        /// Key that was not recognized.
        key: String,
    },
    /// A required product artifact is missing.
    #[error("artifact descriptor is missing for product {product}")]
    MissingArtifact {
        /// Product whose descriptor was absent.
        product: &'static str,
    },
    /// A binding and object identity pair was not declared.
    #[error("unknown service or Durable Object binding: {binding}")]
    UnknownBinding {
        /// Binding that was not declared.
        binding: String,
    },
    /// A binding was addressed with a different object identity.
    #[error("binding {binding} requires object {expected}, not {actual}")]
    WrongBindingObject {
        /// Binding whose identity was checked.
        binding: String,
        /// Declared object identity.
        expected: String,
        /// Requested object identity.
        actual: String,
    },
    /// A required field was absent.
    #[error("manifest field {field} is required")]
    MissingField {
        /// Dotted field name that was absent.
        field: &'static str,
    },
    /// A field had a JSON type other than required.
    #[error("manifest field {field} must be a {expected}")]
    InvalidType {
        /// Dotted field name with the wrong type.
        field: &'static str,
        /// Human-readable expected JSON type.
        expected: &'static str,
    },
    /// A required string was empty.
    #[error("manifest field {field} must not be empty")]
    EmptyField {
        /// Field whose value was empty.
        field: &'static str,
    },
    /// A remote binding origin was not an absolute HTTP(S) origin.
    #[error("remote Worker origin must start with http:// or https://: {value}")]
    InvalidRemoteOrigin {
        /// Rejected origin text.
        value: String,
    },
    /// An artifact identity was not exactly 32 bytes of hexadecimal text.
    #[error("artifact digest must be exactly 64 hexadecimal characters: {value}")]
    InvalidComponentDigest {
        /// Rejected digest text.
        value: String,
    },
    /// A binding did not have a unique URL-visible name.
    #[error("duplicate environment binding: {name}")]
    DuplicateBinding {
        /// Name repeated by more than one binding.
        name: String,
    },
    /// A pipeline did not have a unique environment binding name.
    #[error("duplicate pipeline binding: {name}")]
    DuplicatePipelineBinding {
        /// Name repeated by more than one pipeline.
        name: String,
    },
    /// A system service binding did not have a unique environment name.
    #[error("duplicate service binding: {name}")]
    DuplicateServiceBinding {
        /// Name repeated by more than one service.
        name: String,
    },
    /// A migration object contains an unsupported migration kind or key.
    #[error("unknown migrations manifest key: {key}")]
    UnknownMigrationKey {
        /// Key that was not recognized.
        key: String,
    },
}

/// Parses one nonempty string value for an optional manifest field.
fn parse_nonempty_string(value: Value, field: &'static str) -> Result<String, ManifestError> {
    let Value::String(value) = value else {
        return Err(ManifestError::InvalidType {
            field,
            expected: "string",
        });
    };
    if value.is_empty() {
        return Err(ManifestError::EmptyField { field });
    }
    Ok(value)
}

/// Parses an ordered array of nonempty strings.
fn parse_string_array(value: Value, field: &'static str) -> Result<Vec<String>, ManifestError> {
    let Value::Array(values) = value else {
        return Err(ManifestError::InvalidType {
            field,
            expected: "array",
        });
    };
    values
        .into_iter()
        .map(|value| parse_nonempty_string(value, field))
        .collect()
}

/// Parses Wrangler's scheduled-trigger block while retaining strict key validation.
fn parse_triggers(value: Value) -> Result<Vec<String>, ManifestError> {
    let Value::Object(mut object) = value else {
        return Err(ManifestError::InvalidType {
            field: "triggers",
            expected: "object",
        });
    };
    if let Some(key) = object.keys().find(|key| key.as_str() != "crons").cloned() {
        return Err(ManifestError::UnknownTriggersKey { key });
    }
    object
        .remove("crons")
        .map(|value| parse_string_array(value, "triggers.crons"))
        .transpose()
        .map(Option::unwrap_or_default)
}

/// Parses the worker environment object without interpreting or dropping values.
fn parse_vars(value: Value) -> Result<Map<String, Value>, ManifestError> {
    let Value::Object(values) = value else {
        return Err(ManifestError::InvalidType {
            field: "vars",
            expected: "object",
        });
    };
    Ok(values)
}

/// Parses accepted migration entries and rejects unsupported migration kinds.
fn parse_migrations(value: Value) -> Result<Vec<Migration>, ManifestError> {
    let Value::Array(values) = value else {
        return Err(ManifestError::InvalidType {
            field: "migrations",
            expected: "array",
        });
    };
    values.into_iter().map(parse_migration).collect()
}

/// Parses one migration entry from Wrangler's accepted class-introduction forms.
fn parse_migration(value: Value) -> Result<Migration, ManifestError> {
    let Value::Object(mut object) = value else {
        return Err(ManifestError::InvalidType {
            field: "migrations[]",
            expected: "object",
        });
    };
    let allowed = ["tag", "new_classes", "new_sqlite_classes"]
        .into_iter()
        .collect::<HashSet<_>>();
    if let Some(key) = object
        .keys()
        .find(|key| !allowed.contains(key.as_str()))
        .cloned()
    {
        return Err(ManifestError::UnknownMigrationKey { key });
    }
    let tag = required_string(&mut object, "tag")?;
    let new_classes = object
        .remove("new_classes")
        .map(|value| parse_string_array(value, "migrations[].new_classes"))
        .transpose()?
        .unwrap_or_default();
    let new_sqlite_classes = object
        .remove("new_sqlite_classes")
        .map(|value| parse_string_array(value, "migrations[].new_sqlite_classes"))
        .transpose()?
        .unwrap_or_default();
    Ok(Migration {
        tag,
        new_classes,
        new_sqlite_classes,
    })
}

/// Parses the exact Cloudflare pipelines binding array.
fn parse_pipelines(value: Value) -> Result<Vec<PipelineBinding>, ManifestError> {
    let Value::Array(values) = value else {
        return Err(ManifestError::InvalidType {
            field: "pipelines",
            expected: "array",
        });
    };
    let mut pipelines = Vec::with_capacity(values.len());
    let mut names = HashSet::with_capacity(values.len());
    for value in values {
        let Value::Object(mut object) = value else {
            return Err(ManifestError::InvalidType {
                field: "pipelines[]",
                expected: "object",
            });
        };
        let allowed = ["binding", "stream", "origin"]
            .into_iter()
            .collect::<HashSet<_>>();
        if let Some(key) = object
            .keys()
            .find(|key| !allowed.contains(key.as_str()))
            .cloned()
        {
            return Err(ManifestError::UnknownPipelineKey { key });
        }
        let binding = required_string(&mut object, "binding")?;
        let stream = required_string(&mut object, "stream")?;
        let origin = optional_origin(&mut object)?;
        if !names.insert(binding.clone()) {
            return Err(ManifestError::DuplicatePipelineBinding { name: binding });
        }
        pipelines.push(PipelineBinding {
            binding,
            stream,
            origin,
        });
    }
    Ok(pipelines)
}

/// Parses the exact Cloudflare Vectorize binding array.
fn parse_vectorize(value: Value) -> Result<Vec<VectorizeBinding>, ManifestError> {
    let Value::Array(values) = value else {
        return Err(ManifestError::InvalidType {
            field: "vectorize",
            expected: "array",
        });
    };
    let mut bindings = Vec::with_capacity(values.len());
    let mut names = HashSet::with_capacity(values.len());
    for value in values {
        let Value::Object(mut object) = value else {
            return Err(ManifestError::InvalidType {
                field: "vectorize[]",
                expected: "object",
            });
        };
        if let Some(key) = object
            .keys()
            .find(|key| !["binding", "index_name", "origin"].contains(&key.as_str()))
            .cloned()
        {
            return Err(ManifestError::UnknownVectorizeKey { key });
        }
        let binding = required_string(&mut object, "binding")?;
        let index_name = required_string(&mut object, "index_name")?;
        let origin = optional_origin(&mut object)?;
        if !names.insert(binding.clone()) {
            return Err(ManifestError::DuplicateBinding { name: binding });
        }
        bindings.push(VectorizeBinding {
            binding,
            index_name,
            origin,
        });
    }
    Ok(bindings)
}

/// Parses the strict Verglas Graph binding array.
fn parse_graphs(value: Value) -> Result<Vec<GraphBinding>, ManifestError> {
    let Value::Array(values) = value else {
        return Err(ManifestError::InvalidType {
            field: "graphs",
            expected: "array",
        });
    };
    let mut bindings = Vec::with_capacity(values.len());
    let mut names = HashSet::with_capacity(values.len());
    for value in values {
        let Value::Object(mut object) = value else {
            return Err(ManifestError::InvalidType {
                field: "graphs[]",
                expected: "object",
            });
        };
        if let Some(key) = object
            .keys()
            .find(|key| !["binding", "graph_name", "origin"].contains(&key.as_str()))
            .cloned()
        {
            return Err(ManifestError::UnknownGraphKey { key });
        }
        let binding = required_string(&mut object, "binding")?;
        let graph_name = required_string(&mut object, "graph_name")?;
        let origin = optional_origin(&mut object)?;
        if !names.insert(binding.clone()) {
            return Err(ManifestError::DuplicateBinding { name: binding });
        }
        bindings.push(GraphBinding {
            binding,
            graph_name,
            origin,
        });
    }
    Ok(bindings)
}

/// Parses the strict Verglas Query binding array.
fn parse_queries(value: Value) -> Result<Vec<QueryBinding>, ManifestError> {
    let Value::Array(values) = value else {
        return Err(ManifestError::InvalidType {
            field: "queries",
            expected: "array",
        });
    };
    let mut bindings = Vec::with_capacity(values.len());
    let mut names = HashSet::with_capacity(values.len());
    for value in values {
        let Value::Object(mut object) = value else {
            return Err(ManifestError::InvalidType {
                field: "queries[]",
                expected: "object",
            });
        };
        if let Some(key) = object
            .keys()
            .find(|key| !["binding", "query_name", "origin"].contains(&key.as_str()))
            .cloned()
        {
            return Err(ManifestError::UnknownQueryKey { key });
        }
        let binding = required_string(&mut object, "binding")?;
        let query_name = required_string(&mut object, "query_name")?;
        let origin = optional_origin(&mut object)?;
        if !names.insert(binding.clone()) {
            return Err(ManifestError::DuplicateBinding { name: binding });
        }
        bindings.push(QueryBinding {
            binding,
            query_name,
            origin,
        });
    }
    Ok(bindings)
}

/// Parses explicit prebuilt Pipeline, Sink, and Catalog service bindings.
fn parse_services(
    value: Value,
) -> Result<(Vec<SystemBinding>, Vec<HostServiceBinding>), ManifestError> {
    let Value::Array(values) = value else {
        return Err(ManifestError::InvalidType {
            field: "services",
            expected: "array",
        });
    };
    let mut services = Vec::with_capacity(values.len());
    let mut host_services = Vec::new();
    let mut names = HashSet::with_capacity(values.len());
    for value in values {
        let Value::Object(mut object) = value else {
            return Err(ManifestError::InvalidType {
                field: "services[]",
                expected: "object",
            });
        };
        let allowed = ["binding", "service", "object", "origin"]
            .into_iter()
            .collect::<HashSet<_>>();
        if let Some(key) = object
            .keys()
            .find(|key| !allowed.contains(key.as_str()))
            .cloned()
        {
            return Err(ManifestError::UnknownServiceKey { key });
        }
        let binding = required_string(&mut object, "binding")?;
        let service = required_string(&mut object, "service")?;
        let origin = optional_origin(&mut object)?;
        if !names.insert(binding.clone()) {
            return Err(ManifestError::DuplicateServiceBinding { name: binding });
        }
        if service == "verglas-runtime" {
            if binding != "ICEBERG_COMMIT" || object.contains_key("object") {
                return Err(ManifestError::UnknownServiceProduct { product: service });
            }
            if origin.is_some() {
                return Err(ManifestError::UnknownServiceProduct { product: service });
            }
            host_services.push(HostServiceBinding { binding, service });
            continue;
        }
        let product = match service.as_str() {
            "pipeline" => ArtifactProduct::Pipeline,
            "sink" => ArtifactProduct::Sink,
            "catalog" => ArtifactProduct::Catalog,
            _ => return Err(ManifestError::UnknownServiceProduct { product: service }),
        };
        let object = required_string(&mut object, "object")?;
        services.push(SystemBinding {
            binding,
            product,
            object,
            origin,
        });
    }
    Ok((services, host_services))
}

/// Rejects one environment binding from selecting multiple product namespaces.
fn reject_binding_collisions(namespaces: &BindingNamespaces<'_>) -> Result<(), ManifestError> {
    let mut names = HashSet::new();
    for name in namespaces
        .durable_objects
        .iter()
        .map(|binding| binding.name.as_str())
    {
        if !names.insert(name) {
            return Err(ManifestError::DuplicateBinding {
                name: name.to_owned(),
            });
        }
    }
    for name in namespaces
        .pipelines
        .iter()
        .map(|pipeline| pipeline.binding.as_str())
    {
        if !names.insert(name) {
            return Err(ManifestError::DuplicateBinding {
                name: name.to_owned(),
            });
        }
    }
    for name in namespaces
        .vectorize
        .iter()
        .map(|binding| binding.binding.as_str())
    {
        if !names.insert(name) {
            return Err(ManifestError::DuplicateBinding {
                name: name.to_owned(),
            });
        }
    }
    for name in namespaces
        .graphs
        .iter()
        .map(|binding| binding.binding.as_str())
    {
        if !names.insert(name) {
            return Err(ManifestError::DuplicateBinding {
                name: name.to_owned(),
            });
        }
    }
    for name in namespaces
        .queries
        .iter()
        .map(|binding| binding.binding.as_str())
    {
        if !names.insert(name) {
            return Err(ManifestError::DuplicateBinding {
                name: name.to_owned(),
            });
        }
    }
    for name in namespaces
        .services
        .iter()
        .map(|service| service.binding.as_str())
    {
        if !names.insert(name) {
            return Err(ManifestError::DuplicateBinding {
                name: name.to_owned(),
            });
        }
    }
    for name in namespaces
        .host_services
        .iter()
        .map(|service| service.binding.as_str())
    {
        if !names.insert(name) {
            return Err(ManifestError::DuplicateBinding {
                name: name.to_owned(),
            });
        }
    }
    Ok(())
}

/// Parses all explicitly declared product artifact descriptors.
fn parse_artifacts(
    object: Map<String, Value>,
) -> Result<BTreeMap<ArtifactProduct, ArtifactDescriptor>, ManifestError> {
    let allowed = [
        "worker",
        "durable_object",
        "stream",
        "pipeline",
        "sink",
        "catalog",
        "vectorize",
        "graph",
        "query",
    ]
    .into_iter()
    .collect::<HashSet<_>>();
    if let Some(key) = object
        .keys()
        .find(|key| !allowed.contains(key.as_str()))
        .cloned()
    {
        return Err(ManifestError::UnknownArtifactKey { key });
    }
    let mut artifacts = BTreeMap::new();
    for (key, value) in object {
        let product = match key.as_str() {
            "worker" => ArtifactProduct::Worker,
            "durable_object" => ArtifactProduct::DurableObject,
            "stream" => ArtifactProduct::Stream,
            "pipeline" => ArtifactProduct::Pipeline,
            "sink" => ArtifactProduct::Sink,
            "catalog" => ArtifactProduct::Catalog,
            "vectorize" => ArtifactProduct::Vectorize,
            "graph" => ArtifactProduct::Graph,
            "query" => ArtifactProduct::Query,
            _ => unreachable!("artifact keys were checked above"),
        };
        artifacts.insert(product, parse_artifact_descriptor(value)?);
    }
    Ok(artifacts)
}

/// Parses one digest, component directory, and optional compiled-cache descriptor.
fn parse_artifact_descriptor(value: Value) -> Result<ArtifactDescriptor, ManifestError> {
    let Value::Object(mut object) = value else {
        return Err(ManifestError::InvalidType {
            field: "artifacts[]",
            expected: "object",
        });
    };
    let allowed = ["digest", "component_dir", "cwasm_cache_dir"]
        .into_iter()
        .collect::<HashSet<_>>();
    if let Some(key) = object
        .keys()
        .find(|key| !allowed.contains(key.as_str()))
        .cloned()
    {
        return Err(ManifestError::UnknownArtifactDescriptorKey { key });
    }
    let digest = required_string(&mut object, "digest")?;
    validate_digest(&digest)?;
    let component_dir = PathBuf::from(required_string(&mut object, "component_dir")?);
    let cwasm_cache_dir = object
        .remove("cwasm_cache_dir")
        .map(|value| parse_nonempty_string(value, "artifacts[].cwasm_cache_dir").map(PathBuf::from))
        .transpose()?;
    Ok(ArtifactDescriptor {
        digest,
        component_dir,
        cwasm_cache_dir,
    })
}

/// Requires the Worker and every artifact selected by a declared binding.
fn require_artifacts(
    artifacts: &BTreeMap<ArtifactProduct, ArtifactDescriptor>,
    namespaces: &BindingNamespaces<'_>,
) -> Result<(), ManifestError> {
    if !artifacts.contains_key(&ArtifactProduct::Worker) {
        return Err(ManifestError::MissingArtifact {
            product: ArtifactProduct::Worker.manifest_key(),
        });
    }
    if namespaces
        .durable_objects
        .iter()
        .any(|binding| binding.origin.is_none())
        && !artifacts.contains_key(&ArtifactProduct::DurableObject)
    {
        return Err(ManifestError::MissingArtifact {
            product: ArtifactProduct::DurableObject.manifest_key(),
        });
    }
    if namespaces
        .pipelines
        .iter()
        .any(|pipeline| pipeline.origin.is_none())
        && !artifacts.contains_key(&ArtifactProduct::Stream)
    {
        return Err(ManifestError::MissingArtifact {
            product: ArtifactProduct::Stream.manifest_key(),
        });
    }
    if namespaces
        .vectorize
        .iter()
        .any(|binding| binding.origin.is_none())
        && !artifacts.contains_key(&ArtifactProduct::Vectorize)
    {
        return Err(ManifestError::MissingArtifact {
            product: ArtifactProduct::Vectorize.manifest_key(),
        });
    }
    if namespaces
        .graphs
        .iter()
        .any(|binding| binding.origin.is_none())
        && !artifacts.contains_key(&ArtifactProduct::Graph)
    {
        return Err(ManifestError::MissingArtifact {
            product: ArtifactProduct::Graph.manifest_key(),
        });
    }
    if namespaces
        .queries
        .iter()
        .any(|binding| binding.origin.is_none())
        && !artifacts.contains_key(&ArtifactProduct::Query)
    {
        return Err(ManifestError::MissingArtifact {
            product: ArtifactProduct::Query.manifest_key(),
        });
    }
    for service in namespaces
        .services
        .iter()
        .filter(|service| service.origin.is_none())
    {
        if !artifacts.contains_key(&service.product) {
            return Err(ManifestError::MissingArtifact {
                product: service.product.manifest_key(),
            });
        }
    }
    Ok(())
}

/// Extracts a required nonempty string from an object.
fn required_string(
    object: &mut Map<String, Value>,
    field: &'static str,
) -> Result<String, ManifestError> {
    let value = object
        .remove(field)
        .ok_or(ManifestError::MissingField { field })?;
    let Value::String(value) = value else {
        return Err(ManifestError::InvalidType {
            field,
            expected: "string",
        });
    };
    if value.is_empty() {
        return Err(ManifestError::EmptyField { field });
    }
    Ok(value)
}

/// Extracts a required object from an object.
fn required_object(
    object: &mut Map<String, Value>,
    field: &'static str,
) -> Result<Map<String, Value>, ManifestError> {
    let value = object
        .remove(field)
        .ok_or(ManifestError::MissingField { field })?;
    let Value::Object(value) = value else {
        return Err(ManifestError::InvalidType {
            field,
            expected: "object",
        });
    };
    Ok(value)
}

/// Parses the only supported durable-object section.
fn parse_durable_objects(mut object: Map<String, Value>) -> Result<Vec<Binding>, ManifestError> {
    if let Some(key) = object
        .keys()
        .find(|key| key.as_str() != "bindings")
        .cloned()
    {
        return Err(ManifestError::UnknownDurableObjectsKey { key });
    }
    let value = object
        .remove("bindings")
        .ok_or(ManifestError::MissingField {
            field: "durable_objects.bindings",
        })?;
    let Value::Array(values) = value else {
        return Err(ManifestError::InvalidType {
            field: "durable_objects.bindings",
            expected: "array",
        });
    };
    let mut bindings = Vec::with_capacity(values.len());
    let mut names = HashSet::with_capacity(values.len());
    for value in values {
        let binding = parse_binding(value)?;
        if !names.insert(binding.name.clone()) {
            return Err(ManifestError::DuplicateBinding { name: binding.name });
        }
        bindings.push(binding);
    }
    Ok(bindings)
}

/// Parses one durable-object binding object.
fn parse_binding(value: Value) -> Result<Binding, ManifestError> {
    let Value::Object(mut object) = value else {
        return Err(ManifestError::InvalidType {
            field: "durable_objects.bindings[]",
            expected: "object",
        });
    };
    if let Some(key) = object
        .keys()
        .find(|key| !["name", "class_name", "origin"].contains(&key.as_str()))
        .cloned()
    {
        return Err(ManifestError::UnknownBindingKey { key });
    }
    let name = required_string(&mut object, "name")?;
    let class_name = required_string(&mut object, "class_name")?;
    let origin = optional_origin(&mut object)?;
    Ok(Binding {
        name,
        class_name,
        origin,
    })
}

fn optional_origin(object: &mut Map<String, Value>) -> Result<Option<String>, ManifestError> {
    let origin = object
        .remove("origin")
        .map(|value| parse_nonempty_string(value, "origin"))
        .transpose()?;
    if let Some(value) = &origin
        && !(value.starts_with("http://") || value.starts_with("https://"))
    {
        return Err(ManifestError::InvalidRemoteOrigin {
            value: value.clone(),
        });
    }
    Ok(origin.map(|value| value.trim_end_matches('/').to_owned()))
}

/// Validates a SHA-256 component identity without a runtime dependency.
fn validate_digest(value: &str) -> Result<(), ManifestError> {
    if value.len() != 64
        || value != value.to_ascii_lowercase()
        || hex::decode(value).map_or(true, |bytes| bytes.len() != 32)
    {
        return Err(ManifestError::InvalidComponentDigest {
            value: value.to_owned(),
        });
    }
    Ok(())
}

/// Removes comments and trailing commas accepted by Wrangler's JSONC subset.
fn strip_jsonc(source: &str) -> Result<String, ManifestError> {
    let characters = source.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(source.len());
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;
    while let Some(character) = characters.get(index).copied() {
        if in_string {
            output.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
            }
            index += 1;
            continue;
        }
        if character == '"' {
            in_string = true;
            output.push(character);
            index += 1;
            continue;
        }
        if character == '/' && characters.get(index + 1) == Some(&'/') {
            index += 2;
            while let Some(next) = characters.get(index).copied() {
                index += 1;
                if next == '\n' {
                    output.push('\n');
                    break;
                }
            }
            continue;
        }
        if character == '/' && characters.get(index + 1) == Some(&'*') {
            index += 2;
            let mut terminated = false;
            while let Some(next) = characters.get(index).copied() {
                if next == '*' && characters.get(index + 1) == Some(&'/') {
                    index += 2;
                    terminated = true;
                    output.push(' ');
                    break;
                }
                if next == '\n' {
                    output.push('\n');
                }
                index += 1;
            }
            if !terminated {
                return Err(ManifestError::UnterminatedComment);
            }
            continue;
        }
        output.push(character);
        index += 1;
    }
    Ok(strip_trailing_commas(&output))
}

/// Removes commas immediately before a closing array or object delimiter.
fn strip_trailing_commas(source: &str) -> String {
    let characters = source.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(source.len());
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;
    while let Some(character) = characters.get(index).copied() {
        if in_string {
            output.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
            }
            index += 1;
            continue;
        }
        if character == '"' {
            in_string = true;
            output.push(character);
            index += 1;
            continue;
        }
        if character == ',' {
            let mut lookahead = index + 1;
            while matches!(characters.get(lookahead), Some(value) if value.is_whitespace()) {
                lookahead += 1;
            }
            if matches!(characters.get(lookahead), Some('}' | ']')) {
                index += 1;
                continue;
            }
        }
        output.push(character);
        index += 1;
    }
    output
}
