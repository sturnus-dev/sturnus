use gcp_auth::TokenProvider;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;
use tracing::debug;

/// Vertex AI invocation requires the broad cloud-platform scope.
const VERTEX_SCOPES: &[&str] = &["https://www.googleapis.com/auth/cloud-platform"];

/// Cap on each network step so a stuck endpoint can't hang a request.
const AUTH_TIMEOUT: Duration = Duration::from_secs(5);

/// Resolves GCP access tokens via Application Default Credentials.
/// Detection runs once on first use; the provider caches and refreshes tokens.
pub struct GcpTokenProvider {
    provider: OnceCell<Arc<dyn TokenProvider>>,
}

impl GcpTokenProvider {
    pub fn new() -> Self {
        Self {
            provider: OnceCell::new(),
        }
    }

    async fn provider(
        &self,
    ) -> Result<&dyn TokenProvider, Box<dyn std::error::Error + Send + Sync>> {
        self.provider
            .get_or_try_init(|| async {
                debug!("resolving GCP application default credentials");
                // detection performs network I/O, so it can hang too.
                match tokio::time::timeout(AUTH_TIMEOUT, gcp_auth::provider()).await {
                    Ok(result) => Ok(result?),
                    Err(_) => Err(format!(
                        "GCP credential detection did not complete within {}s",
                        AUTH_TIMEOUT.as_secs()
                    )
                    .into()),
                }
            })
            .await
            .map(|p| p.as_ref())
    }

    pub async fn get_token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let provider = self.provider().await?;
        let fetch = provider.token(VERTEX_SCOPES);
        let token = match tokio::time::timeout(AUTH_TIMEOUT, fetch).await {
            Ok(result) => result?,
            Err(_) => {
                return Err(format!(
                    "GCP token request did not complete within {}s",
                    AUTH_TIMEOUT.as_secs()
                )
                .into());
            }
        };
        Ok(token.as_str().to_string())
    }
}

impl Default for GcpTokenProvider {
    fn default() -> Self {
        Self::new()
    }
}
