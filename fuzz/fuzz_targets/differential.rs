#![no_main]

use libfuzzer_sys::fuzz_target;
use llmrouter::proxy::{extract_json_bool, extract_json_field};
use serde_json::Value;

// The scanner must agree with serde_json on top-level "model" and "stream",
// or fail closed. A disagreement would route to the wrong upstream.
fuzz_target!(|data: &[u8]| {
    let parsed: Value = match serde_json::from_slice(data) {
        Ok(v) => v,
        Err(_) => return,
    };
    let map = match parsed {
        Value::Object(m) => m,
        _ => return,
    };

    if let Some(Value::String(serde_model)) = map.get("model") {
        if let Some((_, scanner_model)) = extract_json_field(data, "model") {
            assert_eq!(
                scanner_model,
                serde_model.as_str(),
                "scanner disagrees with serde_json on top-level model"
            );
        }
    }

    if let Some(Value::Bool(serde_stream)) = map.get("stream") {
        if let Some(scanner_stream) = extract_json_bool(data, "stream") {
            assert_eq!(
                scanner_stream, *serde_stream,
                "scanner disagrees with serde_json on top-level stream"
            );
        }
    }
});
