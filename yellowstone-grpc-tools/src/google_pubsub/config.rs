use {
    crate::config::{deserialize_duration_ms_str, deserialize_usize_str, ConfigGrpcRequest},
    google_cloud_pubsub::{
        client::{
            google_cloud_auth::credentials::CredentialsFile, Client,
            ClientConfig as PubsubClientConfig,
        },
        publisher::PublisherConfig,
    },
    serde::Deserialize,
    std::{net::SocketAddr, time::Duration},
};

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    pub prometheus: Option<SocketAddr>,
    pub client: ConfigClient,
    pub grpc2pubsub: Option<ConfigGrpc2PubSub>,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct ConfigClient {
    pub with_auth: Option<bool>,
    pub with_credentials: Option<String>,
    pub pool_size: Option<usize>,
}

impl Default for ConfigClient {
    fn default() -> Self {
        Self {
            with_auth: None,
            with_credentials: None,
            pool_size: PubsubClientConfig::default().pool_size,
        }
    }
}

impl ConfigClient {
    pub async fn create_client(&self) -> anyhow::Result<Client> {
        let mut config = PubsubClientConfig::default();

        match (self.with_auth, self.with_credentials.as_ref()) {
            (Some(true), Some(_creds)) => {
                anyhow::bail!("Only `with_auth` or `with_credentials` can be enabled")
            }
            (Some(true), _) => {
                config = config.with_auth().await?;
            }
            (_, Some(filepath)) => {
                let creds = CredentialsFile::new_from_file(filepath.clone()).await?;
                config = config.with_credentials(creds).await?;
            }
            (_, None) => {
                if std::env::var("GOOGLE_APPLICATION_CREDENTIALS_JSON").is_ok()
                    || std::env::var("GOOGLE_APPLICATION_CREDENTIALS").is_ok()
                {
                    let creds = CredentialsFile::new().await?;
                    config = config.with_credentials(creds).await?;
                }
            }
        }

        Client::new(config).await.map_err(Into::into)
    }
}

#[derive(Debug, Deserialize)]
pub struct ConfigGrpc2PubSub {
    pub endpoint: String,
    pub x_token: Option<String>,
    pub request: ConfigGrpcRequest,
    #[serde(
        default = "ConfigGrpc2PubSub::default_max_message_size",
        deserialize_with = "deserialize_usize_str"
    )]
    pub max_message_size: usize,

    pub topic: String,
    // Create `topic` with default config if not exists
    #[serde(default)]
    pub create_if_not_exists: bool,

    // Publisher config
    pub publisher: ConfigGrpc2PubSubPublisher,

    // Publisher bulk/batch config
    pub batch: ConfigGrpc2PubSubBatch,
}

impl ConfigGrpc2PubSub {
    const fn default_max_message_size() -> usize {
        512 * 1024 * 1024
    }
}

#[derive(Debug, Deserialize)]
pub struct ConfigGrpc2PubSubPublisher {
    #[serde(default = "ConfigGrpc2PubSubPublisher::default_workers")]
    pub workers: usize,
    #[serde(
        default = "ConfigGrpc2PubSubPublisher::default_flush_interval_ms",
        deserialize_with = "deserialize_usize_str"
    )]
    pub flush_interval_ms: usize,
    #[serde(default = "ConfigGrpc2PubSubPublisher::default_bundle_size")]
    pub bundle_size: usize,
}

impl ConfigGrpc2PubSubPublisher {
    fn default_workers() -> usize {
        PublisherConfig::default().workers
    }

    fn default_flush_interval_ms() -> usize {
        PublisherConfig::default().flush_interval.as_millis() as usize
    }

    fn default_bundle_size() -> usize {
        PublisherConfig::default().bundle_size
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

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct ConfigGrpc2PubSubBatch {
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub max_messages: usize,

    #[serde(deserialize_with = "deserialize_usize_str")]
    pub max_size_bytes: usize,

    #[serde(
        deserialize_with = "deserialize_duration_ms_str",
        rename = "max_wait_ms"
    )]
    pub max_wait: Duration,

    #[serde(deserialize_with = "deserialize_usize_str")]
    pub max_in_progress: usize,
}

impl Default for ConfigGrpc2PubSubBatch {
    fn default() -> Self {
        Self {
            max_messages: 10,
            max_size_bytes: 9_500_000,
            max_wait: Duration::from_millis(100),
            max_in_progress: 100,
        }
    }
}
