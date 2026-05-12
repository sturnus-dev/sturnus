#![no_main]

use libfuzzer_sys::fuzz_target;
use llmrouter::config::Config;
use llmrouter::model_map::ModelMap;
use llmrouter::tracker::Tracker;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let Ok(config) = toml::from_str::<Config>(text) else {
        return;
    };
    let mut tracker = Tracker::new(0.3, 30, 0.5, 10_000);
    let _ = ModelMap::from_config(&config, &mut tracker);
});
