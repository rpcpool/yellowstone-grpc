use {
    crate::scylladb::types::{CommitmentLevel, ConsumerGroupId, Slot},
    core::fmt,
    thiserror::Error,
};

///
/// This error is raised when no lock is held by any producer.
///
#[derive(Error, PartialEq, Eq, Debug)]
pub struct NoActiveProducer;

impl fmt::Display for NoActiveProducer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NoActiveProducer")
    }
}

///
/// This error is raised when there is no active producer for the desired commitment level.
///
#[derive(Copy, Error, PartialEq, Eq, Debug, Clone)]
pub struct ImpossibleCommitmentLevel(pub CommitmentLevel);

impl fmt::Display for ImpossibleCommitmentLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cl = self.0;
        f.write_fmt(format_args!("ImpossibleCommitmentLevel({})", cl))
    }
}

///
/// This error is raised when the combination of consumer critera result in an empty set of elligible producer timeline.
///
#[derive(Error, PartialEq, Eq, Debug)]
pub struct ImpossibleTimelineSelection;

impl fmt::Display for ImpossibleTimelineSelection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ImpossibleTimelineSelection")
    }
}
///
/// This error is raised when no producer as seen the desired `slot`.
///
#[derive(Clone, Debug, Error, PartialEq, Eq, Copy)]
pub struct ImpossibleSlotOffset(pub Slot);

impl fmt::Display for ImpossibleSlotOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let slot = self.0;
        f.write_fmt(format_args!("ImpossbielInititalOffset({})", slot))
    }
}

///
/// This error is raised when a query to a remote store return data that is more up to date
/// then what the current process timeline is expecting.
///
#[derive(Clone, Debug, Error, PartialEq, Eq, Copy)]
pub struct StaleRevision(pub i64);

impl fmt::Display for StaleRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let stale_revision = self.0;
        f.write_fmt(format_args!("StaleRevision({})", stale_revision))
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub struct StaleConsumerGroup(pub ConsumerGroupId);

impl fmt::Display for StaleConsumerGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cg_id = String::from_utf8_lossy(self.0.as_slice());
        f.write_fmt(format_args!("StaleConsumerGroup({})", cg_id))
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub struct LeaderStateLogNotFound(pub ConsumerGroupId);

impl fmt::Display for LeaderStateLogNotFound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cg_id = String::from_utf8_lossy(self.0.as_slice());
        f.write_fmt(format_args!("LeaderStateLogNotFound({})", cg_id))
    }
}
