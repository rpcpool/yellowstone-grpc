pub(crate) mod consumer_group_store;
pub(crate) mod consumer_source;
pub(crate) mod consumer_supervisor;
pub(crate) mod error;
pub(crate) mod etcd_path;
pub(crate) mod leader;
pub(crate) mod lock;
pub(crate) mod producer_queries;
pub(crate) mod consumer_group_service {
    tonic::include_proto!("yellowstone.log.consumer_group");
}
pub(crate) mod coordinator;
pub(crate) mod server;
pub(crate) mod shard_iterator;
