use std::{fs, path::PathBuf};

use bytesize::ByteSize;
use clap::Parser;
use fluvio::{Compression, Fluvio, FluvioConfig};
use log::set_logger;
use yellowstone_grpc_client::GeyserGrpcBuilder;
use yellowstone_grpc_fluvio::{config::ProducerConfig, fluvio_geyser_producer, setup_logger, spawn_geyser_source};


#[derive(Debug, Clone, Parser)]
#[clap(author, version, about = "Yellowstone gRPC ScyllaDB Tool")]
struct Args {
    /// Path to static config file
    #[clap(long)]
    config: PathBuf,
}


#[tokio::main]
async fn main() {
    setup_logger();
    let args = Args::parse();
    let config_path = args.config;
    let config_reader = fs::File::open(config_path).expect("config read"); 

    let config: ProducerConfig = serde_yaml::from_reader(config_reader).expect("config parse");
    let fluvio_addr = config.fluvio_address;
    let fluvio_config = FluvioConfig::new(fluvio_addr.to_string());
    let fluvio = Fluvio::connect_with_config(&fluvio_config).await.unwrap();
    let topic_producer_config = fluvio::TopicProducerConfigBuilder::default()
        .batch_size(config.batch_size.as_u64() as usize)
        .max_request_size(ByteSize::mb(100).as_u64() as usize)
        .compression(Compression::Lz4)
        .linger(config.linger)
        .isolation(fluvio::Isolation::ReadUncommitted)
        .build()
        .expect("producer config");

    
    log::info!("spawning geyser source...");
    let geyser_stream = spawn_geyser_source(&config.geyser).await;
    
    let producer = fluvio.topic_producer_with_config(
        config.topic, 
        topic_producer_config
    ).await.expect("producer");

    log::info!("Starting Fluvio producer");
    fluvio_geyser_producer(
        producer,
        geyser_stream
    ).await;
    
}
