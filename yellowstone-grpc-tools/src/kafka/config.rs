use {
    super::dedup::{KafkaDedup, KafkaDedupMemory},
    crate::config::{deserialize_usize_str, ConfigGrpcRequest},
    serde::Deserialize,
    std::{collections::HashMap, net::SocketAddr},
};

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    pub prometheus: Option<SocketAddr>,
    pub kafka: HashMap<String, String>,
    pub dedup: Option<ConfigDedup>,
    pub grpc2kafka: Option<ConfigGrpc2Kafka>,
    pub kafka2grpc: Option<ConfigKafka2Grpc>,
}

#[derive(Debug, Deserialize)]
pub struct ConfigDedup {
    #[serde(default)]
    pub kafka: HashMap<String, String>,
    pub kafka_input: String,
    pub kafka_output: String,
    #[serde(
        default = "ConfigGrpc2Kafka::default_kafka_queue_size",
        deserialize_with = "deserialize_usize_str"
    )]
    pub kafka_queue_size: usize,
    pub backend: ConfigDedupBackend,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ConfigDedupBackend {
    Memory,
}

impl ConfigDedupBackend {
    pub async fn create(&self) -> anyhow::Result<Box<impl KafkaDedup>> {
        Ok(match self {
            Self::Memory => Box::<KafkaDedupMemory>::default(),
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct ConfigGrpc2Kafka {
    pub endpoint: String,
    pub x_token: Option<String>,
    pub request: ConfigGrpcRequest,
    #[serde(default)]
    pub kafka: HashMap<String, String>,
    pub kafka_topic: String,
    #[serde(
        default = "ConfigGrpc2Kafka::default_kafka_queue_size",
        deserialize_with = "deserialize_usize_str"
    )]
    pub kafka_queue_size: usize,
}

impl ConfigGrpc2Kafka {
    const fn default_kafka_queue_size() -> usize {
        10_000
    }
}

#[derive(Debug, Deserialize)]
pub struct ConfigKafka2Grpc {
    #[serde(default)]
    pub kafka: HashMap<String, String>,
    pub kafka_topic: String,
    pub listen: SocketAddr,
    #[serde(default = "ConfigKafka2Grpc::channel_capacity_default")]
    pub channel_capacity: usize,
}

impl ConfigKafka2Grpc {
    const fn channel_capacity_default() -> usize {
        250_000
    }
}
