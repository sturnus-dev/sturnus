use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

use llmrouter::gcp_auth::GcpTokenProvider;
use llmrouter::metrics::Metrics;
use llmrouter::model_map::{ModelMap, ProviderKind};
use llmrouter::router::RoundRobinState;
use llmrouter::server::AppState;
use llmrouter::tracker::Tracker;

#[derive(Parser)]
#[command(name = "llmrouter", about = "Lightweight LLM load-balancing sidecar")]
struct Cli {
    /// Path to config TOML file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    /// Load a .env file into process env vars before reading config
    #[arg(short, long = "env-file")]
    env_file: Option<PathBuf>,

    /// Load env vars from a directory of secret files (one file per var)
    #[arg(long = "env-dir")]
    env_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "llmrouter=info".parse().unwrap()),
        )
        .init();

    eprintln!(
        "\n\
         \x20     ┌► llm\n\
         \x20  ►──┼► llm\n\
         \x20     └► llm\n\
         \x20  llmrouter v{}\n",
        env!("CARGO_PKG_VERSION")
    );

    let cli = Cli::parse();

    if let Some(ref env_path) = cli.env_file {
        llmrouter::config::Config::load_env_file(env_path)?;
        info!(path = %env_path.display(), "loaded env file");
    }

    if let Some(ref env_dir) = cli.env_dir {
        llmrouter::config::Config::load_env_dir(env_dir)?;
        info!(path = %env_dir.display(), "loaded env dir");
    }

    let config = llmrouter::config::Config::load(&cli.config)?;

    info!(listen = %config.listen, "starting llmrouter");
    info!(
        providers = config.provider.len(),
        models = config.model.len(),
        "loaded config"
    );

    let mut tracker = Tracker::new(
        config.routing.ewma_alpha,
        config.routing.error_decay_secs,
        config.routing.error_threshold,
        config.routing.max_error_window_entries,
    );
    let model_map = ModelMap::from_config(&config, &mut tracker);

    let mut rr_state = RoundRobinState::new();
    for alias in config.model.keys() {
        rr_state.register_alias(alias.clone());
    }

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(10)
        .connect_timeout(std::time::Duration::from_secs(
            config.routing.connect_timeout_secs,
        ))
        .read_timeout(std::time::Duration::from_secs(
            config.routing.read_timeout_secs,
        ))
        .build()?;

    let needs_gcp = config
        .provider
        .values()
        .any(|p| p.resolved_kind() == ProviderKind::GcpMetadata);
    let gcp_token_provider = if needs_gcp {
        info!("GCP metadata auth enabled");
        Some(GcpTokenProvider::new(client.clone()))
    } else {
        None
    };

    let explore_ratio = config.routing.explore_ratio;
    let max_body_bytes = config.routing.max_body_bytes;

    let metrics = Metrics::new();
    let label_triples: Vec<(&str, &str, &str)> = model_map
        .iter()
        .map(|(alias, c)| (alias, c.provider_name.as_str(), c.model.as_str()))
        .collect();
    metrics.init_zero(&label_triples);

    let shutdown_timeout = std::time::Duration::from_secs(config.routing.shutdown_timeout_secs);

    let state = Arc::new(AppState {
        model_map,
        tracker,
        rr_state,
        client,
        explore_ratio,
        gcp_token_provider,
        max_body_bytes,
        metrics,
        shutting_down: AtomicBool::new(false),
    });

    let addr: SocketAddr = config.listen.parse()?;
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "listening");

    let shutdown = async {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).expect("failed to listen for SIGTERM");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = sigterm.recv() => {},
        }
        info!("received shutdown signal");
    };

    llmrouter::server::run_server(listener, state, shutdown, shutdown_timeout).await;

    info!("server stopped");
    Ok(())
}
