use clap::Parser;
use std::io::IsTerminal;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{info, warn};

use llmrouter::gcp_auth::GcpTokenProvider;
use llmrouter::metrics::Metrics;
use llmrouter::model_map::{ModelMap, ProviderKind};
use llmrouter::router::RoundRobinState;
use llmrouter::server::{AppState, BufferBudget};
use llmrouter::tracker::Tracker;

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum LogFormat {
    /// Pretty on a terminal, JSON when piped/redirected.
    Auto,
    Pretty,
    Json,
}

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

    /// Log output format
    #[arg(long, value_enum, default_value = "auto", env = "LLMROUTER_LOG_FORMAT")]
    log_format: LogFormat,
}

// startup banner, shown only on an interactive terminal so it stays out of structured logs.
#[allow(clippy::print_stderr)]
fn print_banner() {
    if !std::io::stderr().is_terminal() {
        return;
    }
    eprintln!(
        "\n\
         \x20     ┌► llm\n\
         \x20  ►──┼► llm\n\
         \x20     └► llm\n\
         \x20  llmrouter v{}\n",
        env!("CARGO_PKG_VERSION")
    );
}

fn init_logging(format: LogFormat) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("llmrouter=info"));
    let json = match format {
        LogFormat::Json => true,
        LogFormat::Pretty => false,
        LogFormat::Auto => !std::io::stdout().is_terminal(),
    };
    if json {
        tracing_subscriber::fmt()
            .json()
            // One request span only; the span list would just duplicate it.
            .with_span_list(false)
            .with_env_filter(filter)
            .init();
    } else {
        let use_color = std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none();
        tracing_subscriber::fmt()
            .with_ansi(use_color)
            .with_env_filter(filter)
            .init();
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_logging(cli.log_format);
    print_banner();
    llmrouter::init_crypto();

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

    let mut tracker = Tracker::new(config.routing.ewma_alpha, config.routing.error_threshold);
    let model_map = ModelMap::from_config(&config, &mut tracker)?;

    let mut rr_state = RoundRobinState::new();
    for alias in config.model.keys() {
        rr_state.register_alias(alias.clone());
    }

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(10)
        .redirect(reqwest::redirect::Policy::none())
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
        .any(|p| p.resolved_kind() == ProviderKind::GcpAdc);
    let gcp_token_provider = if needs_gcp {
        info!("GCP ADC auth enabled");
        Some(GcpTokenProvider::new())
    } else {
        None
    };

    let exploit_k = config.routing.exploit_k;

    let (budget_bytes, budget_source) = config.routing.buffer_budget_bytes();
    let configured_max_body = config.routing.max_body_bytes;
    let budget = BufferBudget::new(budget_bytes, configured_max_body);
    info!(
        budget_mb = budget.permits() / 1024,
        source = budget_source,
        "aggregate request buffer budget"
    );
    // A per-request cap above the aggregate budget is a request that can
    // never be admitted; BufferBudget clamps it so "fits the cap" means
    // "can ever fit".
    if budget.max_body_bytes < configured_max_body {
        warn!(
            configured_mb = configured_max_body / 1024 / 1024,
            clamped_mb = budget.max_body_bytes / 1024 / 1024,
            "max_body_bytes exceeds the buffer budget; clamping the per-request cap to the budget"
        );
    }

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
        exploit_k,
        gcp_token_provider,
        budget,
        metrics,
        shutting_down: AtomicBool::new(false),
    });

    let addr: SocketAddr = config.listen.parse()?;
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "listening");

    let shutdown = async {
        use tokio::signal::unix::{signal, SignalKind};
        // failing to register SIGTERM at startup is unrecoverable.
        #[allow(clippy::expect_used)]
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
