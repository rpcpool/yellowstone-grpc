use tokio::sync::mpsc;

use crate::scylladb::yellowstone_log::consumer_group::consumer_group_service::consumer_group_service_server::ConsumerGroupService;

use super::consumer_group_service::{StopConsumptionRequest, StopConsumptionResponse};

// struct ConsumerGroupServer {
//     tx_stop_consumption: mpsc::Sender<()>,
// }

// #[tonic::async_trait]
// impl ConsumerGroupService for ConsumerGroupServer {

//     async fn stop_consumption(
//         &self,
//         request: tonic::Request<StopConsumptionRequest>
//     ) -> Result<tonic::Response<StopConsumptionResponse>, tonic::Status> {

//         todo!()
//     }

// }
