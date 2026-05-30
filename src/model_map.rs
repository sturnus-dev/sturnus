use crate::config::{Config, ModelCandidate};
use crate::tracker::Tracker;
use hyper::header::HeaderValue;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum ProviderKind {
    /// Bearer token auth. Also used by Google AI Studio.
    ApiKey,
    /// Access token via GCP Application Default Credentials.
    GcpAdc,
    /// api-key header + deployment URL rewriting.
    AzureOpenAi { api_version: String },
    /// x-api-key header + anthropic-version header.
    Anthropic { version: String },
}

#[derive(Debug, Clone)]
pub struct ResolvedCandidate {
    pub provider_name: String,
    pub model: String,
    pub base_url: String,
    pub api_key: Option<String>,
    pub kind: ProviderKind,
    /// Index into `Tracker`'s stats vector. Candidates with the same
    /// (provider, model) pair share a single stats slot across aliases.
    pub stats_index: usize,
    pub provider_header: HeaderValue,
    pub affinity_header: HeaderValue,
    /// Labels to merge into the outbound body as Vertex `labels`.
    /// `None` unless this provider opted in. Shared across candidates
    /// since the contents are immutable after config load.
    pub attribution_labels: Option<Arc<BTreeMap<String, String>>>,
}

#[derive(Debug)]
pub struct ModelMap {
    aliases: HashMap<String, Vec<ResolvedCandidate>>,
}

impl ModelMap {
    /// Build the alias → candidates map and register each unique
    /// (provider, model) pair with the tracker. The returned candidates
    /// each carry a `stats_index` that points into the tracker.
    pub fn from_config(config: &Config, tracker: &mut Tracker) -> anyhow::Result<Self> {
        let mut aliases = HashMap::new();
        let mut dedup: HashMap<(String, String), usize> = HashMap::new();

        let attribution_template: Arc<BTreeMap<String, String>> = Arc::new(
            config
                .attribution
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        );

        for (alias, candidates) in &config.model {
            let resolved = candidates
                .iter()
                .map(|c: &ModelCandidate| {
                    let prov = config.provider.get(&c.provider).ok_or_else(|| {
                        anyhow::anyhow!(
                            "alias '{}' references undefined provider '{}'",
                            alias,
                            c.provider
                        )
                    })?;
                    let base_url = prov.resolved_base_url().ok_or_else(|| {
                        anyhow::anyhow!("provider '{}' has no base_url configured", c.provider)
                    })?;
                    let provider_header = HeaderValue::try_from(c.provider.as_str())
                        .map_err(|_| anyhow::anyhow!("invalid provider name '{}'", c.provider))?;
                    let affinity_header =
                        HeaderValue::try_from(format!("{}/{}", c.provider, c.model)).map_err(
                            |_| {
                                anyhow::anyhow!(
                                    "invalid model name '{}' for provider '{}'",
                                    c.model,
                                    c.provider
                                )
                            },
                        )?;
                    let key = (c.provider.clone(), c.model.clone());
                    let stats_index = *dedup.entry(key).or_insert_with(|| tracker.register());
                    let attribution_labels = prov
                        .vertex_ai
                        .as_ref()
                        .map(|v| v.attribution)
                        .unwrap_or(false)
                        .then(|| Arc::clone(&attribution_template));
                    Ok(ResolvedCandidate {
                        provider_name: c.provider.clone(),
                        model: c.model.clone(),
                        base_url,
                        api_key: prov.api_key.clone(),
                        kind: prov.resolved_kind(),
                        stats_index,
                        provider_header,
                        affinity_header,
                        attribution_labels,
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            aliases.insert(alias.clone(), resolved);
        }

        Ok(Self { aliases })
    }

    pub fn get(&self, alias: &str) -> Option<&[ResolvedCandidate]> {
        self.aliases.get(alias).map(|v| v.as_slice())
    }

    pub fn alias_names(&self) -> Vec<&str> {
        self.aliases.keys().map(|s| s.as_str()).collect()
    }

    /// Iterate every (alias, candidate) pair, for status reporting and
    /// metric initialisation.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &ResolvedCandidate)> {
        self.aliases.iter().flat_map(|(alias, candidates)| {
            candidates
                .iter()
                .map(move |candidate| (alias.as_str(), candidate))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_provider_model_across_aliases_dedupes_to_one_stats_index() {
        let toml_str = r#"
[provider.openai]
base_url = "https://api.openai.com/v1"
api_key = "sk-test"

[model]
fast = [{ provider = "openai", model = "gpt-4o-mini" }]
cheap = [{ provider = "openai", model = "gpt-4o-mini" }]
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        let mut tracker = Tracker::new(0.3, 30, 0.5, 10_000);
        let map = ModelMap::from_config(&config, &mut tracker).unwrap();

        let fast_idx = map.get("fast").unwrap()[0].stats_index;
        let cheap_idx = map.get("cheap").unwrap()[0].stats_index;
        assert_eq!(fast_idx, cheap_idx);
    }

    #[test]
    fn attribution_propagates_only_to_opt_in_candidates() {
        let toml_str = r#"
[attribution]
service = "my-service"
owner = "team-x"

[provider.vertex-attr]
api_key = "k"
vertex_ai = { project_id = "p", location = "l", attribution = true }

[provider.vertex-no-attr]
api_key = "k"
vertex_ai = { project_id = "p", location = "l" }

[model]
attr   = [{ provider = "vertex-attr",    model = "google/gemini-2.5-flash" }]
noattr = [{ provider = "vertex-no-attr", model = "google/gemini-2.5-flash" }]
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        config.validate().unwrap();
        let mut tracker = Tracker::new(0.3, 30, 0.5, 10_000);
        let map = ModelMap::from_config(&config, &mut tracker).unwrap();

        let attr = &map.get("attr").unwrap()[0];
        let labels = attr
            .attribution_labels
            .as_ref()
            .expect("opt-in candidate should have labels");
        assert_eq!(labels.get("service").unwrap(), "my-service");
        assert_eq!(labels.get("owner").unwrap(), "team-x");
        assert_eq!(labels.len(), 2);

        let noattr = &map.get("noattr").unwrap()[0];
        assert!(noattr.attribution_labels.is_none());
    }

    #[test]
    fn attribution_labels_none_when_attribution_map_empty() {
        let toml_str = r#"
[provider.vertex]
api_key = "k"
vertex_ai = { project_id = "p", location = "l" }

[model]
test = [{ provider = "vertex", model = "google/gemini-2.5-flash" }]
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        let mut tracker = Tracker::new(0.3, 30, 0.5, 10_000);
        let map = ModelMap::from_config(&config, &mut tracker).unwrap();
        assert!(map.get("test").unwrap()[0].attribution_labels.is_none());
    }

    #[test]
    fn distinct_provider_model_pairs_get_distinct_indices() {
        let toml_str = r#"
[provider.openai]
base_url = "https://api.openai.com/v1"
api_key = "sk-test"

[model]
fast = [
    { provider = "openai", model = "gpt-4o-mini" },
    { provider = "openai", model = "gpt-4o" },
]
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        let mut tracker = Tracker::new(0.3, 30, 0.5, 10_000);
        let map = ModelMap::from_config(&config, &mut tracker).unwrap();

        let candidates = map.get("fast").unwrap();
        assert_ne!(candidates[0].stats_index, candidates[1].stats_index);
    }
}
