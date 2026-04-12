use crate::config::{Config, ModelCandidate};
use std::collections::HashMap;
use std::sync::Arc;

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
    pub provider_name: Arc<str>,
    pub model: Arc<str>,
    pub base_url: String,
    pub api_key: Option<String>,
    pub kind: ProviderKind,
}

#[derive(Debug)]
pub struct ModelMap {
    aliases: HashMap<String, Vec<ResolvedCandidate>>,
}

impl ModelMap {
    pub fn from_config(config: &Config) -> Self {
        let mut aliases = HashMap::new();
        for (alias, candidates) in &config.model {
            let resolved: Vec<ResolvedCandidate> = candidates
                .iter()
                .filter_map(|c: &ModelCandidate| {
                    let prov = config.provider.get(&c.provider)?;
                    let kind = prov.resolved_kind();
                    Some(ResolvedCandidate {
                        provider_name: Arc::from(c.provider.as_str()),
                        model: Arc::from(c.model.as_str()),
                        base_url: prov.resolved_base_url()?,
                        api_key: prov.api_key.clone(),
                        kind,
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
}
