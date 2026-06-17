use crate::model_map::ProviderKind;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default = "default_listen")]
    pub listen: String,
    pub provider: HashMap<String, ProviderConfig>,
    pub model: HashMap<String, Vec<ModelCandidate>>,
    #[serde(default)]
    pub routing: RoutingConfig,
    /// Labels merged into outbound bodies as `labels` for opt-in Vertex
    /// providers. Typically deployment identity from env vars.
    #[serde(default)]
    pub attribution: HashMap<String, String>,
}

fn default_listen() -> String {
    "127.0.0.1:4000".to_string()
}

#[derive(Debug, Default, Deserialize)]
pub struct ProviderConfig {
    /// Base URL for the provider API. Optional if a provider shorthand is set.
    pub base_url: Option<String>,
    pub api_key: Option<String>,
    /// Vertex AI shorthand — derives base_url from project/location.
    pub vertex_ai: Option<VertexAiConfig>,
    /// Azure OpenAI shorthand — derives base_url from resource name.
    pub azure_openai: Option<AzureOpenAiConfig>,
    /// Google AI Studio shorthand — derives base_url, uses API key as query param.
    pub google_ai: Option<GoogleAiConfig>,
    /// Anthropic shorthand — derives base_url, uses x-api-key header.
    pub anthropic: Option<AnthropicConfig>,
}

#[derive(Debug, Deserialize)]
pub struct VertexAiConfig {
    pub project_id: String,
    pub location: String,
    /// Merge top-level `[attribution]` into the request as Vertex `labels`.
    #[serde(default)]
    pub attribution: bool,
}

#[derive(Debug, Deserialize)]
pub struct AzureOpenAiConfig {
    pub resource_name: String,
    pub api_version: String,
}

#[derive(Debug, Deserialize)]
pub struct GoogleAiConfig {
    /// API version prefix in the URL. Defaults to `v1beta`.
    #[serde(default = "GoogleAiConfig::default_api_version")]
    pub api_version: String,
}

impl GoogleAiConfig {
    fn default_api_version() -> String {
        "v1beta".to_string()
    }
}

#[derive(Debug, Deserialize)]
pub struct AnthropicConfig {
    /// Anthropic API version sent as `anthropic-version` header.
    /// Defaults to `2023-06-01`.
    #[serde(default = "AnthropicConfig::default_version")]
    pub version: String,
}

impl AnthropicConfig {
    fn default_version() -> String {
        "2023-06-01".to_string()
    }
}

impl ProviderConfig {
    pub fn resolved_base_url(&self) -> Option<String> {
        self.base_url.clone().or_else(|| {
            if let Some(ref v) = self.vertex_ai {
                // `global` has no regional hostname prefix.
                let host = if v.location == "global" {
                    "aiplatform.googleapis.com".to_string()
                } else {
                    format!("{}-aiplatform.googleapis.com", v.location)
                };
                return Some(format!(
                    "https://{}/v1beta1/projects/{}/locations/{}/endpoints/openapi",
                    host, v.project_id, v.location
                ));
            }
            if let Some(ref az) = self.azure_openai {
                return Some(format!(
                    "https://{}.openai.azure.com/openai",
                    az.resource_name
                ));
            }
            if let Some(ref g) = self.google_ai {
                return Some(format!(
                    "https://generativelanguage.googleapis.com/{}/openai",
                    g.api_version
                ));
            }
            if self.anthropic.is_some() {
                return Some("https://api.anthropic.com/v1".to_string());
            }
            None
        })
    }

    pub fn resolved_kind(&self) -> ProviderKind {
        if self.vertex_ai.is_some() {
            if self.api_key.is_some() {
                return ProviderKind::ApiKey;
            }
            return ProviderKind::GcpAdc;
        }
        if let Some(ref az) = self.azure_openai {
            return ProviderKind::AzureOpenAi {
                api_version: az.api_version.clone(),
            };
        }
        if self.google_ai.is_some() {
            return ProviderKind::ApiKey;
        }
        if let Some(ref a) = self.anthropic {
            return ProviderKind::Anthropic {
                version: a.version.clone(),
            };
        }
        ProviderKind::ApiKey
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelCandidate {
    pub provider: String,
    pub model: String,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct RoutingConfig {
    /// Smoothing shared by the latency and success-rate EWMAs
    /// (~`1/ewma_alpha` samples of memory).
    pub ewma_alpha: f64,
    /// Exploit sharpness for proportional routing: traffic scales with
    /// `(best_effective / its_effective)^exploit_k`, where effective latency
    /// is the latency EWMA over the success-rate EWMA. Rarely needs changing.
    pub exploit_k: f64,
    /// Error-rate EWMA above which a session-affinity pin is broken.
    /// Routing weights never consult it.
    pub error_threshold: f64,
    pub connect_timeout_secs: u64,
    pub read_timeout_secs: u64,
    /// Maximum request body size in bytes; larger requests 413.
    /// The 32 MB default matches the request-size cap of the largest
    /// mainstream provider APIs.
    pub max_body_bytes: usize,
    /// Cap on the *total* bytes of request bodies buffered across all
    /// in-flight requests; beyond it new requests get 429. Defaults to
    /// half the container's cgroup memory limit when one is detected,
    /// else `4 * max_body_bytes`.
    pub max_buffered_bytes: Option<usize>,
    pub shutdown_timeout_secs: u64,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            ewma_alpha: 0.3,
            exploit_k: crate::router::EXPLOIT_K,
            error_threshold: 0.5,
            connect_timeout_secs: 10,
            read_timeout_secs: 60,
            max_body_bytes: 32 * 1024 * 1024, // 32 MB
            max_buffered_bytes: None,
            shutdown_timeout_secs: 30,
        }
    }
}

impl RoutingConfig {
    /// Resolve the aggregate buffer budget in bytes, and where it came
    /// from (for the startup log): explicit config, half the cgroup
    /// memory limit, or a multiple of the per-request cap.
    pub fn buffer_budget_bytes(&self) -> (usize, &'static str) {
        if let Some(bytes) = self.max_buffered_bytes {
            return (bytes, "max_buffered_bytes");
        }
        if let Some(limit) = cgroup_memory_limit() {
            return (usize::try_from(limit / 2).unwrap_or(usize::MAX), "cgroup");
        }
        (self.max_body_bytes.saturating_mul(4), "4x max_body_bytes")
    }
}

/// The container's memory limit from cgroups (v2, then v1), if one is set.
fn cgroup_memory_limit() -> Option<u64> {
    cgroup_memory_limit_from(
        Path::new("/sys/fs/cgroup/memory.max"),
        Path::new("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
    )
}

/// Prefer cgroup v2, fall back to v1, else `None` (unlimited or unavailable).
fn cgroup_memory_limit_from(v2: &Path, v1: &Path) -> Option<u64> {
    if let Ok(raw) = std::fs::read_to_string(v2) {
        return parse_cgroup_v2(&raw);
    }
    parse_cgroup_v1(&std::fs::read_to_string(v1).ok()?)
}

/// v2: a byte count, or "max" (unparseable → `None`) when unlimited.
fn parse_cgroup_v2(raw: &str) -> Option<u64> {
    raw.trim().parse().ok()
}

/// v1: a byte count; reports a page-rounded `i64::MAX` when unlimited.
fn parse_cgroup_v1(raw: &str) -> Option<u64> {
    const UNBOUNDED_THRESHOLD: u64 = 1u64 << 60;
    let limit: u64 = raw.trim().parse().ok()?;
    (limit < UNBOUNDED_THRESHOLD).then_some(limit)
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("failed to read config file '{}': {e}", path.display()))?;
        let interpolated =
            shellexpand::env(&raw).map_err(|e| anyhow::anyhow!("env var expansion failed: {e}"))?;
        let config: Config = toml::from_str(&interpolated)?;
        config.validate()?;
        Ok(config)
    }

    /// Load a `.env` file into process env vars (does not overwrite existing).
    pub fn load_env_file(path: &Path) -> anyhow::Result<()> {
        dotenvy::from_path(path)?;
        Ok(())
    }

    /// Load env vars from a directory of secret files (filename = key, content = value).
    /// Overwrites existing env vars so mounted secrets take precedence.
    /// Skips hidden files and subdirectories.
    pub fn load_env_dir(dir: &Path) -> anyhow::Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() && !entry.file_type()?.is_symlink() {
                continue;
            }
            let name = entry.file_name();
            let key = name
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("non-UTF-8 filename: {}", entry.path().display()))?;
            if key.starts_with('.') {
                continue;
            }
            let val = std::fs::read_to_string(entry.path())?;
            std::env::set_var(key, val.trim_end_matches('\n'));
        }
        Ok(())
    }

    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if !(0.0..=1.0).contains(&self.routing.ewma_alpha) {
            anyhow::bail!(
                "ewma_alpha must be between 0.0 and 1.0, got {}",
                self.routing.ewma_alpha
            );
        }
        if !self.routing.exploit_k.is_finite() || self.routing.exploit_k < 0.0 {
            anyhow::bail!(
                "exploit_k must be a finite value >= 0.0, got {}",
                self.routing.exploit_k
            );
        }
        if !(0.0..=1.0).contains(&self.routing.error_threshold) {
            anyhow::bail!(
                "error_threshold must be between 0.0 and 1.0, got {}",
                self.routing.error_threshold
            );
        }
        for (name, p) in &self.provider {
            let shorthand_count = [
                p.vertex_ai.is_some(),
                p.azure_openai.is_some(),
                p.google_ai.is_some(),
                p.anthropic.is_some(),
            ]
            .iter()
            .filter(|&&b| b)
            .count();
            if shorthand_count > 1 {
                anyhow::bail!(
                    "provider '{}' has multiple provider shorthands; use only one of vertex_ai, azure_openai, google_ai, or anthropic",
                    name
                );
            }
            let Some(base_url) = p.resolved_base_url() else {
                anyhow::bail!(
                    "provider '{}' must have base_url or a provider shorthand configured",
                    name
                );
            };
            if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
                anyhow::bail!(
                    "provider '{}' has invalid base_url (must start with http:// or https://): {}",
                    name,
                    base_url
                );
            }
        }
        for (alias, candidates) in &self.model {
            for c in candidates {
                if !self.provider.contains_key(&c.provider) {
                    anyhow::bail!(
                        "model alias '{alias}' references unknown provider '{}'",
                        c.provider
                    );
                }
            }
        }
        let any_attribution_opt_in = self
            .provider
            .values()
            .any(|p| p.vertex_ai.as_ref().map(|v| v.attribution).unwrap_or(false));
        if any_attribution_opt_in && self.attribution.is_empty() {
            anyhow::bail!(
                "at least one provider has vertex_ai.attribution = true but the top-level [attribution] map is empty"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
