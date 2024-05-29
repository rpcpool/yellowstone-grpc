
use super::types::{ConsumerGroupId, InstanceId};




struct LeaderHandle;


struct StaticMemberSpawner {
    leader_handle: LeaderHandle,
}


impl StaticMemberSpawner {

    async fn spawn_member(&self, consumer_group_id: ConsumerGroupId, instance_id: Option<InstanceId>) {

        todo!();
        // leader_handle.get_producer_id(consumer_group_id);
    }
}

