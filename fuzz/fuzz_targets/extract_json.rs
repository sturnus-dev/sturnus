#![no_main]

use libfuzzer_sys::fuzz_target;
use llmrouter::proxy::{extract_json_bool, extract_json_field, rewrite_model};

fuzz_target!(|data: &[u8]| {
    let _ = extract_json_field(data, "model");
    let _ = extract_json_field(data, "stream");
    let _ = extract_json_field(data, "");
    let _ = extract_json_bool(data, "stream");
    let _ = extract_json_bool(data, "model");
    let _ = rewrite_model(data, "replacement-model");
});
