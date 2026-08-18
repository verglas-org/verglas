#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![forbid(unsafe_code)]
#![allow(clippy::module_name_repetitions, clippy::similar_names)]

// Use jemalloc as the global allocator to avoid glibc malloc fragmentation
// which causes monotonic growth of container_memory_working_set_bytes.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::{Parser, Subcommand};
use lakekeeper::{CONFIG, tokio, tracing};
use tracing_subscriber::{EnvFilter, filter::LevelFilter};

mod config;
mod serve_craft;

pub(crate) use config::CONFIG_BIN;
const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Serve one CRaft-backed Iceberg warehouse without PostgreSQL.
    ServeCraft {
        /// Ordered, comma-separated Verglas catalog ingress URLs.
        #[clap(long, env = "VERGLAS_CATALOG_ENDPOINTS", value_delimiter = ',')]
        endpoints: Vec<String>,
        /// Tenant that owns the CRaft catalog groups.
        #[clap(long, env = "VERGLAS_CATALOG_TENANT")]
        tenant: String,
        /// Hosted warehouse group name.
        #[clap(long, env = "VERGLAS_CATALOG_WAREHOUSE")]
        warehouse: String,
        /// Lakekeeper S3 storage profile JSON using the AWS credential chain.
        ///
        /// `--managed-s3-profile` is the boot-contract name (verglas-cloud's
        /// `images/lakekeeper/boot.sh` renders it from the tenant's managed
        /// storage coordinates); `--metadata-s3-profile` is kept as an alias
        /// for direct/manual invocations that predate that contract.
        #[clap(
            long,
            alias = "metadata-s3-profile",
            env = "VERGLAS_MANAGED_S3_PROFILE"
        )]
        managed_s3_profile: String,
    },
    /// Print the version of the server
    Version {},
    /// Print the management OpenAPI specification
    #[cfg(feature = "open-api")]
    ManagementOpenapi {},
    /// Print the generic-table OpenAPI specification
    #[cfg(feature = "open-api")]
    GenericTableOpenapi {},
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(true)
        .with_file(CONFIG_BIN.debug.extended_logs)
        .with_line_number(CONFIG_BIN.debug.extended_logs)
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    match cli.command {
        Some(Commands::ServeCraft {
            endpoints,
            tenant,
            warehouse,
            managed_s3_profile,
        }) => {
            print_info();
            serve_craft::serve_craft(
                std::net::SocketAddr::from((CONFIG.bind_ip, CONFIG.listen_port)),
                endpoints,
                tenant,
                warehouse,
                managed_s3_profile,
            )
            .await?;
        }
        Some(Commands::Version {}) => {
            println!("{}", env!("CARGO_PKG_VERSION"));
        }
        #[cfg(feature = "open-api")]
        Some(Commands::ManagementOpenapi {}) => {
            use lakekeeper::{
                AuthZBackend, api::management::v1::api_doc, service::authz::AllowAllAuthorizer,
            };
            use lakekeeper_authz_openfga::OpenFGAAuthorizer;
            use lakekeeper_authz_verglas::VerglasAuthorizer;

            let queue_configs_ref = &lakekeeper::service::tasks::BUILT_IN_API_CONFIGS;
            let queue_configs: Vec<&_> = queue_configs_ref.iter().collect();
            let project_queue_configs_ref =
                &lakekeeper::service::tasks::BUILT_IN_PROJECT_API_CONFIGS;
            let project_queue_configs: Vec<&_> = project_queue_configs_ref.iter().collect();
            let doc = match &CONFIG.authz_backend {
                AuthZBackend::AllowAll => {
                    api_doc::<AllowAllAuthorizer>(&queue_configs, &project_queue_configs)
                }
                AuthZBackend::External(e) if e == "openfga" => {
                    api_doc::<OpenFGAAuthorizer>(&queue_configs, &project_queue_configs)
                }
                AuthZBackend::External(e) if e == "verglas" => {
                    api_doc::<VerglasAuthorizer>(&queue_configs, &project_queue_configs)
                }
                AuthZBackend::External(e) => anyhow::bail!("Unsupported authz backend `{e}`"),
            };
            println!("{}", doc.to_yaml()?);
        }
        #[cfg(feature = "open-api")]
        Some(Commands::GenericTableOpenapi {}) => {
            let doc = lakekeeper::api::data::v1::generic_tables::api_doc();
            println!("{}", doc.to_yaml()?);
        }
        None => {
            eprintln!("No subcommand provided. Use --help for more information.");
            anyhow::bail!("No subcommand provided");
        }
    }

    Ok(())
}

fn print_info() {
    let console_span = r" _      ___  _   _______ _   _______ ___________ ___________ 
| |    / _ \| | / |  ___| | / |  ___|  ___| ___ |  ___| ___ \
| |   / /_\ | |/ /| |__ | |/ /| |__ | |__ | |_/ | |__ | |_/ /
| |   |  _  |    \|  __||    \|  __||  __||  __/|  __||    / 
| |___| | | | |\  | |___| |\  | |___| |___| |   | |___| |\ \ 
\_____\_| |_\_| \_\____/\_| \_\____/\____/\_|   \____/\_| \_|

 _____ ___________ _____ 
/  __ |  _  | ___ |  ___|
| /  \| | | | |_/ | |__  
| |   | | | |    /|  __|
| \__/\ \_/ | |\ \| |___
 \____/\___/\_| \_\____/

Created with ❤️ by Vakamo
Docs: https://docs.lakekeeper.io
Enterprise Support: https://vakamo.com
";
    let console_span = format!("{console_span}\nLakekeeper Version: {VERSION}\n");
    println!("{console_span}");
    tracing::info!("Lakekeeper Version: {VERSION}");
}
