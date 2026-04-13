use reqwest::Client;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::debug;

const METADATA_URL: &str =
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

struct CachedToken {
    token: String,
    expires_at: Instant,
}

/// Fetches and caches GCP access tokens from the metadata server.
/// The lock is held through the fetch to coalesce concurrent refreshes.
pub struct GcpTokenProvider {
    client: Client,
    cache: Mutex<Option<CachedToken>>,
}

impl GcpTokenProvider {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            cache: Mutex::new(None),
        }
    }

    pub async fn get_token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut cache = self.cache.lock().await;

        if let Some(ref cached) = *cache {
            if cached.expires_at > Instant::now() + Duration::from_secs(60) {
                return Ok(cached.token.clone());
            }
        }

        debug!("refreshing GCP access token from metadata server");
        let resp = self
            .client
            .get(METADATA_URL)
            .header("Metadata-Flavor", "Google")
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("GCP metadata server returned {status}: {body}").into());
        }

        let body: serde_json::Value = resp.json().await?;
        let token = body["access_token"]
            .as_str()
            .ok_or("missing access_token in metadata response")?
            .to_string();
        let expires_in = body["expires_in"].as_u64().unwrap_or(3600);

        let expires_at = Instant::now() + Duration::from_secs(expires_in);
        debug!(expires_in, "GCP token refreshed");

        *cache = Some(CachedToken {
            token: token.clone(),
            expires_at,
        });

        Ok(token)
    }
}
