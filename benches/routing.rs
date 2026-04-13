use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::time::Duration;

use llmrouter::model_map::{ProviderKind, ResolvedCandidate};
use llmrouter::proxy;
use llmrouter::router::{self, RoundRobinState};
use llmrouter::tracker::Tracker;

fn make_candidate(provider: &str, model: &str, stats_index: usize) -> ResolvedCandidate {
    ResolvedCandidate {
        provider_name: provider.to_string(),
        model: model.to_string(),
        base_url: "http://localhost".to_string(),
        api_key: None,
        kind: ProviderKind::ApiKey,
        stats_index,
    }
}

/// Small request body (~100 bytes) — typical chat completion.
fn small_body() -> Vec<u8> {
    br#"{"model":"fast","messages":[{"role":"user","content":"Hello"}],"stream":false}"#.to_vec()
}

/// Large request body (~1MB) — multimodal with base64 image.
fn large_body() -> Vec<u8> {
    let prefix = br#"{"model":"fast","messages":[{"role":"user","content":[{"type":"text","text":"describe"},{"type":"image_url","image_url":{"url":"data:image/png;base64,"#;
    let image_data = "A".repeat(1_000_000);
    let suffix = br#""}}]}],"stream":true}"#;
    let mut body = Vec::with_capacity(prefix.len() + image_data.len() + suffix.len());
    body.extend_from_slice(prefix);
    body.extend_from_slice(image_data.as_bytes());
    body.extend_from_slice(suffix);
    body
}

fn build_candidates(specs: &[(&str, &str)]) -> (Vec<ResolvedCandidate>, Tracker, RoundRobinState) {
    let mut tracker = Tracker::new(0.3, 30, 0.5, 10_000);
    let mut rr = RoundRobinState::new();
    rr.register_alias("fast".to_string());
    let candidates: Vec<_> = specs
        .iter()
        .map(|(provider, model)| {
            let stats_index = tracker.register();
            // Warm up with some TTFC data
            tracker.record_ttfc(stats_index, Duration::from_millis(100));
            tracker.record_success(stats_index);
            make_candidate(provider, model, stats_index)
        })
        .collect();
    (candidates, tracker, rr)
}

fn bench_extract_model_small(c: &mut Criterion) {
    let body = small_body();
    c.bench_function("extract_model/small_body", |b| {
        b.iter(|| proxy::extract_json_field(black_box(&body), "model"))
    });
}

fn bench_extract_model_large(c: &mut Criterion) {
    let body = large_body();
    c.bench_function("extract_model/large_body_1mb", |b| {
        b.iter(|| proxy::extract_json_field(black_box(&body), "model"))
    });
}

fn bench_extract_stream(c: &mut Criterion) {
    let body = small_body();
    c.bench_function("extract_stream_bool", |b| {
        b.iter(|| proxy::extract_json_bool(black_box(&body), "stream"))
    });
}

fn bench_rewrite_model_small(c: &mut Criterion) {
    let body = small_body();
    c.bench_function("rewrite_model/small_body", |b| {
        b.iter(|| proxy::rewrite_model(black_box(&body), "gpt-4o-mini"))
    });
}

fn bench_rewrite_model_large(c: &mut Criterion) {
    let body = large_body();
    c.bench_function("rewrite_model/large_body_1mb", |b| {
        b.iter(|| proxy::rewrite_model(black_box(&body), "gpt-4o-mini"))
    });
}

fn bench_select_candidate(c: &mut Criterion) {
    let (candidates, tracker, rr) = build_candidates(&[
        ("openai", "gpt-4o-mini"),
        ("groq", "llama-3.3-70b"),
        ("vertex", "gemini-2.5-flash"),
    ]);

    c.bench_function("select_candidate/3_warm", |b| {
        b.iter(|| {
            router::select_candidate(
                black_box("fast"),
                black_box(&candidates),
                black_box(&tracker),
                black_box(&rr),
                black_box(0.2),
            )
        })
    });
}

fn bench_full_pipeline_small(c: &mut Criterion) {
    let body = small_body();
    let (candidates, tracker, rr) =
        build_candidates(&[("openai", "gpt-4o-mini"), ("groq", "llama-3.3-70b")]);

    c.bench_function("full_pipeline/small_body", |b| {
        b.iter(|| {
            let (_, alias) = proxy::extract_json_field(black_box(&body), "model").unwrap();
            let _stream = proxy::extract_json_bool(black_box(&body), "stream");
            let candidate =
                router::select_candidate(alias, &candidates, &tracker, &rr, 0.2).unwrap();
            proxy::rewrite_model(black_box(&body), &candidate.model).unwrap()
        })
    });
}

fn bench_full_pipeline_large(c: &mut Criterion) {
    let body = large_body();
    let (candidates, tracker, rr) =
        build_candidates(&[("openai", "gpt-4o-mini"), ("groq", "llama-3.3-70b")]);

    c.bench_function("full_pipeline/large_body_1mb", |b| {
        b.iter(|| {
            let (_, alias) = proxy::extract_json_field(black_box(&body), "model").unwrap();
            let _stream = proxy::extract_json_bool(black_box(&body), "stream");
            let candidate =
                router::select_candidate(alias, &candidates, &tracker, &rr, 0.2).unwrap();
            proxy::rewrite_model(black_box(&body), &candidate.model).unwrap()
        })
    });
}

criterion_group!(
    benches,
    bench_extract_model_small,
    bench_extract_model_large,
    bench_extract_stream,
    bench_rewrite_model_small,
    bench_rewrite_model_large,
    bench_select_candidate,
    bench_full_pipeline_small,
    bench_full_pipeline_large,
);
criterion_main!(benches);
