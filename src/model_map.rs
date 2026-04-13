use crate::config::{Config, ModelCandidate};
use crate::tracker::Tracker;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum ProviderKind {
    /// Bearer token auth. Also used by Google AI Studio.
    ApiKey,
    /// Access token from GCP metadata server.
    GcpMetadata,
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
}

#[derive(Debug)]
pub struct ModelMap {
    aliases: HashMap<String, Vec<ResolvedCandidate>>,
}

impl ModelMap {
    /// Build the alias → candidates map and register each unique
    /// (provider, model) pair with the tracker. The returned candidates
    /// each carry a `stats_index` that points into the tracker.
    pub fn from_config(config: &Config, tracker: &mut Tracker) -> Self {
        let mut aliases = HashMap::new();
        let mut dedup: HashMap<(String, String), usize> = HashMap::new();

        for (alias, candidates) in &config.model {
            let resolved: Vec<ResolvedCandidate> = candidates
                .iter()
                .filter_map(|c: &ModelCandidate| {
                    let prov = config.provider.get(&c.provider)?;
                    let kind = prov.resolved_kind();
                    let base_url = prov.resolved_base_url()?;
                    let key = (c.provider.clone(), c.model.clone());
                    let stats_index = *dedup.entry(key).or_insert_with(|| tracker.register());
                    Some(ResolvedCandidate {
                        provider_name: c.provider.clone(),
                        model: c.model.clone(),
                        base_url,
                        api_key: prov.api_key.clone(),
                        kind,
                        stats_index,
                    })
                })
                .collect();
            aliases.insert(alias.clone(), resolved);
        }

        Self { aliases }
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
        let map = ModelMap::from_config(&config, &mut tracker);

        let fast_idx = map.get("fast").unwrap()[0].stats_index;
        let cheap_idx = map.get("cheap").unwrap()[0].stats_index;
        assert_eq!(fast_idx, cheap_idx);
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
        let map = ModelMap::from_config(&config, &mut tracker);

        let candidates = map.get("fast").unwrap();
        assert_ne!(candidates[0].stats_index, candidates[1].stats_index);
    }
}
