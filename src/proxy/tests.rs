use super::*;
use futures_util::stream;
use futures_util::StreamExt;

#[tokio::test]
async fn stream_error_emits_sse_error_event() {
    // Simulate a stream: good chunk, then error, then another good chunk
    let source: Vec<Result<Bytes, String>> = vec![
        Ok(Bytes::from("data: {\"chunk\":1}\n\n")),
        Err("connection reset".to_string()),
        Ok(Bytes::from("data: {\"chunk\":2}\n\n")),
    ];

    let mock_stream = stream::iter(source);
    let relay = build_relay_stream(mock_stream);
    let chunks: Vec<_> = relay.collect().await;

    // Should have: good chunk, error event — stream terminates after error
    assert!(
        chunks.len() >= 2,
        "expected at least 2 frames, got {}",
        chunks.len()
    );

    // First frame is the good data
    let first = chunks[0].as_ref().unwrap().data_ref().unwrap();
    assert_eq!(first.as_ref(), b"data: {\"chunk\":1}\n\n");

    // Second frame should be an SSE error event
    let second = chunks[1].as_ref().unwrap().data_ref().unwrap();
    let second_str = std::str::from_utf8(second.as_ref()).unwrap();
    assert!(
        second_str.contains("error"),
        "expected error event, got: {second_str}"
    );
}

fn outbound_bytes(out: &OutboundBody) -> Vec<u8> {
    out.data.as_deref().unwrap_or_default().to_vec()
}

fn prepared(json: &str, real_model: &str, labels: Option<&BTreeMap<String, String>>) -> Value {
    let body: RawBody = serde_json::from_str(json).unwrap();
    let out = prepare_outbound(body, real_model, labels, None).unwrap();
    serde_json::from_slice(&outbound_bytes(&out)).unwrap()
}

fn labels_of(items: &[(&str, &str)]) -> BTreeMap<String, String> {
    items
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn prepare_body_replaces_model_alias() {
    let body = prepared(r#"{"model":"fast","temperature":0.7}"#, "gpt-4o-mini", None);
    assert_eq!(body["model"], Value::String("gpt-4o-mini".into()));
    assert_eq!(body["temperature"], Value::from(0.7));
}

#[test]
fn prepare_body_without_attribution_leaves_labels_untouched() {
    let body = prepared(
        r#"{"model":"x","labels":{"client":"v"}}"#,
        "real-model",
        None,
    );
    assert_eq!(body["labels"]["client"], "v");
}

#[test]
fn prepare_body_injects_labels_into_empty_body() {
    let labels = labels_of(&[("service", "foo")]);
    let body = prepared(r#"{"model":"x"}"#, "real-model", Some(&labels));
    assert_eq!(body["labels"]["service"], "foo");
}

#[test]
fn prepare_body_merges_with_existing_labels_sidecar_wins() {
    let labels = labels_of(&[("service", "sidecar-set"), ("owner", "team-a")]);
    let body = prepared(
        r#"{"model":"x","labels":{"tenant":"abc","service":"client-set"}}"#,
        "real-model",
        Some(&labels),
    );
    assert_eq!(body["labels"]["tenant"], "abc");
    assert_eq!(body["labels"]["service"], "sidecar-set");
    assert_eq!(body["labels"]["owner"], "team-a");
}

#[test]
fn prepare_body_overwrites_when_existing_labels_wrong_shape() {
    let labels = labels_of(&[("service", "foo")]);
    let body = prepared(
        r#"{"model":"x","labels":"not-an-object"}"#,
        "real-model",
        Some(&labels),
    );
    assert_eq!(body["labels"]["service"], "foo");
    assert!(body["labels"].is_object());
}

#[test]
fn prepare_outbound_passes_other_fields_through_verbatim() {
    let body: RawBody = serde_json::from_str(
        r#"{"temperature":0.30,"model":"fast","big":18446744073709551616,"messages":[{"role":"user","content":"hi"}]}"#,
    )
    .unwrap();
    let bytes = outbound_bytes(&prepare_outbound(body, "gpt-4o-mini", None, None).unwrap());
    // Key order, number formatting, and u64-overflowing precision survive.
    assert_eq!(
        std::str::from_utf8(&bytes).unwrap(),
        r#"{"temperature":0.30,"model":"gpt-4o-mini","big":18446744073709551616,"messages":[{"role":"user","content":"hi"}]}"#
    );
}

#[test]
fn prepare_outbound_serializes_large_bodies_losslessly() {
    let blob = "x".repeat(1024 * 1024);
    let input = format!(r#"{{"model":"fast","content":"{blob}"}}"#);
    let body: RawBody = serde_json::from_str(&input).unwrap();
    let out = prepare_outbound(body, "gpt-4o-mini", None, None).unwrap();

    // The serialized length is reported exactly, so hyper sets Content-Length.
    let expected = format!(r#"{{"model":"gpt-4o-mini","content":"{blob}"}}"#);
    assert_eq!(out.size_hint().exact(), Some(expected.len() as u64));
    assert_eq!(String::from_utf8(outbound_bytes(&out)).unwrap(), expected);
}

fn make_candidate(kind: ProviderKind, base_url: &str) -> ResolvedCandidate {
    ResolvedCandidate {
        provider_name: "test".into(),
        model: "gpt-4o".into(),
        base_url: base_url.into(),
        api_key: Some("test-key".into()),
        kind,
        stats_index: 0,
        provider_header: hyper::header::HeaderValue::from_static("test"),
        affinity_header: hyper::header::HeaderValue::from_static("test/gpt-4o"),
        attribution_labels: None,
    }
}

#[test]
fn url_default_deduplicates_v1() {
    let c = make_candidate(ProviderKind::ApiKey, "https://api.openai.com/v1");
    let url = build_upstream_url(&c, "/v1/chat/completions");
    assert_eq!(url, "https://api.openai.com/v1/chat/completions");
}

#[test]
fn url_vertex_strips_v1() {
    let c = make_candidate(
        ProviderKind::GcpAdc,
        "https://us-central1-aiplatform.googleapis.com/v1beta1/projects/p/locations/l/endpoints/openapi",
    );
    let url = build_upstream_url(&c, "/v1/chat/completions");
    assert_eq!(
        url,
        "https://us-central1-aiplatform.googleapis.com/v1beta1/projects/p/locations/l/endpoints/openapi/chat/completions"
    );
}

#[test]
fn url_azure_includes_deployment_and_version() {
    let c = make_candidate(
        ProviderKind::AzureOpenAi {
            api_version: "2024-10-21".into(),
        },
        "https://my-resource.openai.azure.com/openai",
    );
    let url = build_upstream_url(&c, "/v1/chat/completions");
    assert_eq!(
        url,
        "https://my-resource.openai.azure.com/openai/deployments/gpt-4o/chat/completions?api-version=2024-10-21"
    );
}

#[test]
fn sanitize_headers_strips_unsafe_and_preserves_rest() {
    let mut upstream = reqwest::header::HeaderMap::new();
    upstream.insert("content-type", "application/json".parse().unwrap());
    upstream.insert(
        reqwest::header::TRANSFER_ENCODING,
        "chunked".parse().unwrap(),
    );
    upstream.insert(reqwest::header::CONNECTION, "keep-alive".parse().unwrap());
    upstream.insert(reqwest::header::ALT_SVC, "h3=\":443\"".parse().unwrap());
    upstream.append(reqwest::header::SET_COOKIE, "__cf_bm=abc".parse().unwrap());
    upstream.append(reqwest::header::SET_COOKIE, "__cfduid=def".parse().unwrap());
    upstream.append(reqwest::header::VARY, "accept".parse().unwrap());
    upstream.append(reqwest::header::VARY, "origin".parse().unwrap());

    let sanitized = sanitize_response_headers(&upstream);

    for stripped in [
        reqwest::header::SET_COOKIE,
        reqwest::header::TRANSFER_ENCODING,
        reqwest::header::CONNECTION,
        reqwest::header::ALT_SVC,
    ] {
        assert!(
            !sanitized.contains_key(&stripped),
            "{stripped} should be stripped"
        );
    }
    assert_eq!(sanitized.get("content-type").unwrap(), "application/json");
    assert_eq!(sanitized.get_all(reqwest::header::VARY).iter().count(), 2);
}
