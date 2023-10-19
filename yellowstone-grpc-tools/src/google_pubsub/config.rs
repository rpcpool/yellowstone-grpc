use {
    crate::config::{deserialize_usize_str, ConfigGrpcRequest},
    google_cloud_pubsub::{
        client::{google_cloud_auth::credentials::CredentialsFile, Client, ClientConfig},
        publisher::PublisherConfig,
    },
    serde::Deserialize,
    std::{net::SocketAddr, time::Duration},
};

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    pub prometheus: Option<SocketAddr>,
    pub auth: Option<String>,
    pub grpc2pubsub: Option<ConfigGrpc2PubSub>,
}

impl Config {
    pub async fn create_client(&self) -> anyhow::Result<Client> {
        let mut config = ClientConfig::default();
        if let Some(creds) = match self.auth.clone() {
            Some(filepath) => CredentialsFile::new_from_file(filepath).await.map(Some),
            None => {
                if std::env::var("GOOGLE_APPLICATION_CREDENTIALS_JSON").is_ok()
                    || std::env::var("GOOGLE_APPLICATION_CREDENTIALS").is_ok()
                {
                    CredentialsFile::new().await.map(Some)
                } else {
                    Ok(None)
                }
            }
        }? {
            config = config.with_credentials(creds).await?;
        }
        Client::new(config).await.map_err(Into::into)
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ConfigGrpc2PubSub {
    pub endpoint: String,
    pub x_token: Option<String>,
    pub request: ConfigGrpcRequest,

    pub topic: String,
    // Create `topic` with default config if not exists
    pub create_if_not_exists: bool,

    // Publisher config
    #[serde(default = "ConfigGrpc2PubSub::default_workers")]
    pub workers: usize,
    #[serde(
        default = "ConfigGrpc2PubSub::default_flush_interval_ms",
        deserialize_with = "deserialize_usize_str"
    )]
    pub flush_interval_ms: usize,
    #[serde(default = "ConfigGrpc2PubSub::default_bundle_size")]
    pub bundle_size: usize,

    // Publisher bulk config
    #[serde(
        default = "ConfigGrpc2PubSub::default_bulk_max_size",
        deserialize_with = "deserialize_usize_str"
    )]
    pub bulk_max_size: usize,
    #[serde(
        default = "ConfigGrpc2PubSub::default_bulk_max_wait_ms",
        deserialize_with = "deserialize_usize_str"
    )]
    pub bulk_max_wait_ms: usize,
}

impl ConfigGrpc2PubSub {
    fn default_workers() -> usize {
        PublisherConfig::default().workers
    }

    fn default_flush_interval_ms() -> usize {
        PublisherConfig::default().flush_interval.as_millis() as usize
    }

    fn default_bundle_size() -> usize {
        PublisherConfig::default().bundle_size
    }

    const fn default_bulk_max_size() -> usize {
        10
    }

    const fn default_bulk_max_wait_ms() -> usize {
        100
    }

    pub const fn get_publisher_config(&self) -> PublisherConfig {
        PublisherConfig {
            workers: self.workers,
            flush_interval: Duration::from_millis(self.flush_interval_ms as u64),
            bundle_size: self.bundle_size,
            retry_setting: None,
        }
    }
}
