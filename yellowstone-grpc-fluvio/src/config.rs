use std::{net::SocketAddr, time::Duration};

use bytesize::ByteSize;
use serde::Deserialize;


#[derive(Deserialize, Debug)]
pub struct ProducerConfig {
    pub fluvio_address: SocketAddr,
    
    pub topic: String,
    
    #[serde(default = "ProducerConfig::default_batch_size", with = "bytesize_serde")]
    pub batch_size: ByteSize,

    #[serde(default = "ProducerConfig::default_linger", with = "humantime_serde")]
    pub linger: Duration,

    pub geyser: GeyserConfig, 
}

#[derive(Clone, Deserialize, Debug)]
pub struct GeyserConfig {
    pub endpoint: String,
    pub x_token: Option<String>,
}

impl ProducerConfig {
    pub(crate) fn default_batch_size() -> ByteSize {
        ByteSize::mb(1)
    }
    
    pub(crate) fn default_linger() -> Duration {
        Duration::from_millis(10)
    }
}