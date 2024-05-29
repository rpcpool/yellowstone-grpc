use std::{collections::BTreeMap, net::IpAddr};

use scylla::{cql_to_rust::{FromCqlVal, FromCqlValError}, frame::response::result::CqlValue, serialize::value::SerializeCql, FromRow};

use crate::scylladb::types::{ProducerId, ShardId};

use super::types::InstanceId;

#[derive(Clone, Debug, PartialEq, Eq, Copy)]
pub(crate) enum ConsumerGroupType {
    Static = 0,
}

impl TryFrom<i16> for ConsumerGroupType {
    type Error = anyhow::Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ConsumerGroupType::Static),
            x => Err(anyhow::anyhow!(
                "Unknown ConsumerGroupType equivalent for {:?}",
                x
            )),
        }
    }
}

impl From<ConsumerGroupType> for i16 {
    fn from(val: ConsumerGroupType) -> Self {
        match val {
            ConsumerGroupType::Static => 0,
        }
    }
}

impl SerializeCql for ConsumerGroupType {
    fn serialize<'b>(
        &self,
        typ: &scylla::frame::response::result::ColumnType,
        writer: scylla::serialize::CellWriter<'b>,
    ) -> Result<
        scylla::serialize::writers::WrittenCellProof<'b>,
        scylla::serialize::SerializationError,
    > {
        let x: i16 = (*self).into();
        SerializeCql::serialize(&x, typ, writer)
    }
}

impl FromCqlVal<CqlValue> for ConsumerGroupType {
    fn from_cql(cql_val: CqlValue) -> Result<Self, scylla::cql_to_rust::FromCqlValError> {
        match cql_val {
            CqlValue::SmallInt(x) => x.try_into().map_err(|_| FromCqlValError::BadVal),
            _ => Err(FromCqlValError::BadCqlType),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct ConsumerGroupInfo {
    consumer_group_id: Vec<u8>,
    group_type: ConsumerGroupType,
    producer_id: Option<ProducerId>,
    fencing_token: Option<Vec<u8>>,
    instance_id_shard_assignments: BTreeMap<InstanceId, Vec<ShardId>>,
    last_access_ip_address: Option<IpAddr>,
}