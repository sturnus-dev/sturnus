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

#[test]
fn cgroup_parsing_handles_limits_and_unlimited_sentinels() {
    assert_eq!(parse_cgroup_v2("268435456\n"), Some(268435456));
    assert_eq!(parse_cgroup_v2("max\n"), None); // v2 unlimited
    assert_eq!(parse_cgroup_v1("268435456\n"), Some(268435456));
    assert_eq!(parse_cgroup_v1("9223372036854771712"), None); // v1 page-rounded i64::MAX
}

#[test]
fn cgroup_prefers_v2_over_v1() {
    let dir = tempfile::tempdir().unwrap();
    let v2 = dir.path().join("memory.max");
    let v1 = dir.path().join("limit_in_bytes");
    std::fs::write(&v2, "111\n").unwrap();
    std::fs::write(&v1, "999\n").unwrap();
    assert_eq!(cgroup_memory_limit_from(&v2, &v1), Some(111));
    // v2 absent → fall back to v1.
    std::fs::remove_file(&v2).unwrap();
    assert_eq!(cgroup_memory_limit_from(&v2, &v1), Some(999));
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
