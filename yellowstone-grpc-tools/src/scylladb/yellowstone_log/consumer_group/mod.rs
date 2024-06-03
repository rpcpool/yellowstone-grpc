pub(crate) mod consumer_source;
pub(crate) mod etcd_path;
pub(crate) mod leader;
pub(crate) mod lock;
pub(crate) mod producer_queries;
pub(crate) mod repo;
pub(crate) mod consumer_group_service {
    tonic::include_proto!("yellowstone.log.consumer_group");
}
pub(crate) mod local_coordinator;
pub(crate) mod server;
