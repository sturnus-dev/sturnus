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
    /// Deprecated and ignored as of 4.1.0 — routing now uses proportional
    /// weighting. Retained only to warn operators whose config still sets it.
    pub explore_ratio: Option<f64>,
    /// Error-rate EWMA above which a session-affinity pin is broken.
    /// Routing weights never consult it.
    pub error_threshold: f64,
    /// Deprecated and ignored as of 4.2.0 — the success-rate EWMA recovers
    /// through probe traffic rather than a time window.
    pub error_decay_secs: Option<u64>,
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
    /// Deprecated and ignored as of 4.2.0 — there is no error window to cap.
    pub max_error_window_entries: Option<usize>,
    pub shutdown_timeout_secs: u64,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            ewma_alpha: 0.3,
            exploit_k: crate::router::EXPLOIT_K,
            explore_ratio: None,
            error_threshold: 0.5,
            error_decay_secs: None,
            connect_timeout_secs: 10,
            read_timeout_secs: 60,
            max_body_bytes: 32 * 1024 * 1024, // 32 MB
            max_buffered_bytes: None,
            max_error_window_entries: None,
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
    if let Ok(raw) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        // v2: a byte count, or "max" when unlimited.
        return raw.trim().parse().ok();
    }
    let raw = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes").ok()?;
    let limit: u64 = raw.trim().parse().ok()?;
    // v1 reports a page-rounded i64::MAX when unlimited.
    const UNBOUNDED_THRESHOLD: u64 = 1u64 << 60;
    // Use cgroup limit if bounded, otherwise None and fallback should be used
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
        if config.routing.explore_ratio.is_some() {
            tracing::warn!(
                "`routing.explore_ratio` is deprecated and ignored as of 4.1.0; routing now uses proportional latency weighting"
            );
        }
        if config.routing.error_decay_secs.is_some() {
            tracing::warn!(
                "`routing.error_decay_secs` is deprecated and ignored as of 4.2.0; errors now proportionally reduce a candidate's routing weight"
            );
        }
        if config.routing.max_error_window_entries.is_some() {
            tracing::warn!(
                "`routing.max_error_window_entries` is deprecated and ignored as of 4.2.0; errors now proportionally reduce a candidate's routing weight"
            );
        }
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
mod tests {
    use super::*;

    #[test]
    fn vertex_ai_derives_base_url() {
        let provider = ProviderConfig {
            vertex_ai: Some(VertexAiConfig {
                project_id: "my-project".into(),
                location: "us-central1".into(),
                attribution: false,
            }),
            ..Default::default()
        };
        let url = provider.resolved_base_url().unwrap();
        assert!(url.contains("us-central1-aiplatform.googleapis.com"));
        assert!(url.contains("my-project"));
        assert!(url.contains("us-central1"));
    }

    #[test]
    fn vertex_ai_global_location_omits_region_prefix() {
        let provider = ProviderConfig {
            vertex_ai: Some(VertexAiConfig {
                project_id: "my-project".into(),
                location: "global".into(),
                attribution: false,
            }),
            ..Default::default()
        };
        let url = provider.resolved_base_url().unwrap();
        assert_eq!(
            url,
            "https://aiplatform.googleapis.com/v1beta1/projects/my-project/locations/global/endpoints/openapi"
        );
        assert!(!url.contains("global-aiplatform"));
    }

    #[test]
    fn vertex_ai_defaults_to_gcp_adc_auth() {
        let provider = ProviderConfig {
            vertex_ai: Some(VertexAiConfig {
                project_id: "p".into(),
                location: "l".into(),
                attribution: false,
            }),
            ..Default::default()
        };
        assert_eq!(provider.resolved_kind(), ProviderKind::GcpAdc);
    }

    #[test]
    fn vertex_ai_with_api_key_uses_api_key_auth() {
        let provider = ProviderConfig {
            api_key: Some("my-key".into()),
            vertex_ai: Some(VertexAiConfig {
                project_id: "p".into(),
                location: "l".into(),
                attribution: false,
            }),
            ..Default::default()
        };
        assert_eq!(provider.resolved_kind(), ProviderKind::ApiKey);
        // base_url should still be derived from vertex_ai config
        let url = provider.resolved_base_url().unwrap();
        assert!(url.contains("aiplatform.googleapis.com"));
    }

    #[test]
    fn api_key_provider_defaults_to_api_key_kind() {
        let provider = ProviderConfig {
            base_url: Some("https://api.openai.com/v1".into()),
            api_key: Some("sk-test".into()),
            ..Default::default()
        };
        assert_eq!(provider.resolved_kind(), ProviderKind::ApiKey);
        assert_eq!(
            provider.resolved_base_url().unwrap(),
            "https://api.openai.com/v1"
        );
    }

    #[test]
    fn timeout_defaults_when_omitted() {
        let toml_str = r#"
[provider.openai]
base_url = "https://api.openai.com/v1"
api_key = "sk-test"

[model]
fast = [{ provider = "openai", model = "gpt-4o-mini" }]
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.routing.connect_timeout_secs, 10);
        assert_eq!(config.routing.read_timeout_secs, 60);
    }

    #[test]
    fn exploit_k_rejects_negative() {
        let toml_str = r#"
[provider.openai]
base_url = "https://api.openai.com/v1"
api_key = "sk-test"

[model]
fast = [{ provider = "openai", model = "gpt-4o-mini" }]

[routing]
exploit_k = -1.0
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn deprecated_explore_ratio_is_accepted_and_ignored() {
        let toml_str = r#"
[provider.openai]
base_url = "https://api.openai.com/v1"
api_key = "sk-test"

[model]
fast = [{ provider = "openai", model = "gpt-4o-mini" }]

[routing]
explore_ratio = 0.02
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        // Parses, validates, and is captured so load() can warn — but does not
        // affect routing (which uses exploit_k).
        assert!(config.validate().is_ok());
        assert_eq!(config.routing.explore_ratio, Some(0.02));
    }

    #[test]
    fn timeout_explicit_values_override_defaults() {
        let toml_str = r#"
[provider.openai]
base_url = "https://api.openai.com/v1"
api_key = "sk-test"

[model]
fast = [{ provider = "openai", model = "gpt-4o-mini" }]

[routing]
connect_timeout_secs = 5
read_timeout_secs = 120
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.routing.connect_timeout_secs, 5);
        assert_eq!(config.routing.read_timeout_secs, 120);
    }

    #[test]
    fn azure_openai_derives_base_url() {
        let provider = ProviderConfig {
            api_key: Some("key".into()),
            azure_openai: Some(AzureOpenAiConfig {
                resource_name: "my-resource".into(),
                api_version: "2024-10-21".into(),
            }),
            ..Default::default()
        };
        let url = provider.resolved_base_url().unwrap();
        assert_eq!(url, "https://my-resource.openai.azure.com/openai");
    }

    #[test]
    fn azure_openai_resolves_kind() {
        let provider = ProviderConfig {
            api_key: Some("key".into()),
            azure_openai: Some(AzureOpenAiConfig {
                resource_name: "r".into(),
                api_version: "v".into(),
            }),
            ..Default::default()
        };
        assert!(matches!(
            provider.resolved_kind(),
            ProviderKind::AzureOpenAi { .. }
        ));
    }

    #[test]
    fn google_ai_shorthand_derives_url_and_kind() {
        let toml_str = r#"
[provider.gemini]
api_key = "key"
google_ai = {}

[model]
test = [{ provider = "gemini", model = "gemini-2.5-flash" }]
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(config.validate().is_ok());
        let p = &config.provider["gemini"];
        assert_eq!(
            p.resolved_base_url().unwrap(),
            "https://generativelanguage.googleapis.com/v1beta/openai"
        );
        assert_eq!(p.resolved_kind(), ProviderKind::ApiKey);
    }

    #[test]
    fn anthropic_shorthand_derives_url_and_kind() {
        let toml_str = r#"
[provider.anthropic]
api_key = "key"
anthropic = {}

[model]
test = [{ provider = "anthropic", model = "claude-sonnet-4-20250514" }]
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(config.validate().is_ok());
        let p = &config.provider["anthropic"];
        assert_eq!(
            p.resolved_base_url().unwrap(),
            "https://api.anthropic.com/v1"
        );
        assert!(matches!(p.resolved_kind(), ProviderKind::Anthropic { .. }));
    }

    #[test]
    fn attribution_opt_in_requires_non_empty_attribution_map() {
        let toml_str = r#"
[provider.vertex]
api_key = "k"
vertex_ai = { project_id = "p", location = "l", attribution = true }

[model]
test = [{ provider = "vertex", model = "google/gemini-2.5-flash" }]
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn attribution_opt_in_with_attribution_map_validates() {
        let toml_str = r#"
[attribution]
service = "my-service"
owner = "team-x"

[provider.vertex]
api_key = "k"
vertex_ai = { project_id = "p", location = "l", attribution = true }

[model]
test = [{ provider = "vertex", model = "google/gemini-2.5-flash" }]
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(config.validate().is_ok());
        assert_eq!(config.attribution.get("service").unwrap(), "my-service");
        assert!(
            config.provider["vertex"]
                .vertex_ai
                .as_ref()
                .unwrap()
                .attribution
        );
    }

    #[test]
    fn attribution_defaults_off_for_vertex() {
        let toml_str = r#"
[provider.vertex]
api_key = "k"
vertex_ai = { project_id = "p", location = "l" }

[model]
test = [{ provider = "vertex", model = "google/gemini-2.5-flash" }]
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(config.validate().is_ok());
        assert!(
            !config.provider["vertex"]
                .vertex_ai
                .as_ref()
                .unwrap()
                .attribution
        );
    }

    #[test]
    fn attribution_field_does_not_exist_on_non_vertex_providers() {
        // `attribution` lives inside the vertex_ai shorthand, so non-Vertex
        // providers can't set it — the type system enforces Vertex-only.
        let toml_str = r#"
[provider.openai]
base_url = "https://api.openai.com/v1"
api_key = "k"

[model]
test = [{ provider = "openai", model = "gpt-4o-mini" }]
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(config.provider["openai"].vertex_ai.is_none());
    }

    #[test]
    fn rejects_multiple_provider_shorthands() {
        let toml_str = r#"
[provider.bad]
api_key = "key"
google_ai = {}
anthropic = {}

[model]
test = [{ provider = "bad", model = "test" }]
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(config.validate().is_err());
    }

    mod env_dir {
        use super::*;
        use std::io::Write;

        fn tmp_secret_dir(entries: &[(&str, &str)]) -> tempfile::TempDir {
            let dir = tempfile::tempdir().unwrap();
            for (name, value) in entries {
                let mut f = std::fs::File::create(dir.path().join(name)).unwrap();
                f.write_all(value.as_bytes()).unwrap();
            }
            dir
        }

        #[test]
        fn reads_files_as_env_vars() {
            let dir = tmp_secret_dir(&[("TEST_DIR_A", "secret-a")]);
            Config::load_env_dir(dir.path()).unwrap();
            assert_eq!(std::env::var("TEST_DIR_A").unwrap(), "secret-a");
            std::env::remove_var("TEST_DIR_A");
        }

        #[test]
        fn trims_trailing_newline() {
            let dir = tmp_secret_dir(&[("TEST_DIR_TRIM", "secret\n")]);
            Config::load_env_dir(dir.path()).unwrap();
            assert_eq!(std::env::var("TEST_DIR_TRIM").unwrap(), "secret");
            std::env::remove_var("TEST_DIR_TRIM");
        }

        #[test]
        fn overwrites_existing() {
            std::env::set_var("TEST_DIR_OVER", "original");
            let dir = tmp_secret_dir(&[("TEST_DIR_OVER", "rotated")]);
            Config::load_env_dir(dir.path()).unwrap();
            assert_eq!(std::env::var("TEST_DIR_OVER").unwrap(), "rotated");
            std::env::remove_var("TEST_DIR_OVER");
        }

        #[test]
        fn skips_hidden_files_and_subdirs() {
            let dir = tmp_secret_dir(&[(".hidden", "nope"), ("TEST_DIR_VIS", "yes")]);
            std::fs::create_dir(dir.path().join("subdir")).unwrap();
            Config::load_env_dir(dir.path()).unwrap();
            assert!(std::env::var(".hidden").is_err());
            assert_eq!(std::env::var("TEST_DIR_VIS").unwrap(), "yes");
            std::env::remove_var("TEST_DIR_VIS");
        }
    }
}
