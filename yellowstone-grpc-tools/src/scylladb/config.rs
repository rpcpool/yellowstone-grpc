use {
    super::{sink::ScyllaSinkConfig, types::CommitmentLevel},
    crate::config::ConfigGrpcRequest,
    serde::Deserialize,
    serde_with::{serde_as, DurationMilliSeconds},
    std::{net::SocketAddr, time::Duration},
};

const fn default_batch_len_limit() -> usize {
    10
}

const fn default_batch_size_kb() -> usize {
    131585
}

const fn default_linger() -> Duration {
    Duration::from_millis(10)
}

fn default_scylla_username() -> String {
    "cassandra".into()
}

fn default_scylla_password() -> String {
    "cassandra".into()
}

fn default_keyspace() -> String {
    "default".into()
}

fn default_hostname() -> String {
    String::from("127.0.0.1:9144")
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    pub prometheus: Option<SocketAddr>,
    pub scylladb: ScyllaDbConnectionInfo,
    pub grpc2scylladb: Option<ConfigGrpc2ScyllaDB>,
    pub yellowstone_log_server: Option<ConfigYellowstoneLogServer>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ScyllaDbConnectionInfo {
    #[serde(default = "default_hostname")]
    pub hostname: String,
    #[serde(default = "default_scylla_username")]
    pub username: String,
    #[serde(default = "default_scylla_password")]
    pub password: String,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct ConfigYellowstoneLogServer {
    pub listen: String,
    #[serde(default = "default_keyspace")]
    pub keyspace: String,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct ConfigGrpc2ScyllaDB {
    pub endpoint: String,
    pub x_token: Option<String>,
    pub request: ConfigGrpcRequest,

    pub producer_id: u8,

    // Optional network interface name used to write in the producer lock table.
    pub ifname: Option<String>,

    #[serde(default = "default_batch_len_limit")]
    pub batch_len_limit: usize,

    #[serde(default = "default_batch_size_kb")]
    pub batch_size_kb_limit: usize,

    #[serde(default = "default_linger")]
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    pub linger: Duration,

    #[serde(default = "default_keyspace")]
    pub keyspace: String,
}

impl ConfigGrpc2ScyllaDB {
    pub fn get_scylladb_sink_config(&self) -> ScyllaSinkConfig {
        ScyllaSinkConfig {
            producer_id: self.producer_id,
            batch_len_limit: self.batch_len_limit,
            batch_size_kb_limit: self.batch_size_kb_limit,
            linger: self.linger,
            keyspace: self.keyspace.clone(),
            ifname: self.ifname.to_owned(),
            commitment_level: match self.request.commitment.expect("Missing commitment level") {
                crate::config::ConfigGrpcRequestCommitment::Processed => CommitmentLevel::Processed,
                crate::config::ConfigGrpcRequestCommitment::Confirmed => CommitmentLevel::Confirmed,
                crate::config::ConfigGrpcRequestCommitment::Finalized => CommitmentLevel::Finalized,
            },
        }
    }
}
