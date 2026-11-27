//! Test support: tests read a client's Subscribe queue as the updates a client decodes from
//! the bytes of its items, so they do not depend on the queue's item type.

use {
    crate::{
        plugin::filter::message::FilteredUpdate, subscribe::entry::SubscribeQueueEntry,
        util::stream::LoadAwareReceiver,
    },
    prost::Message as _,
    yellowstone_grpc_proto::geyser::SubscribeUpdate,
};

/// An item of a client's Subscribe queue: an update, or the error status that ends the stream.
pub type SubscribeQueueItem = tonic::Result<SubscribeQueueEntry>;

/// The receiving half of a client's Subscribe queue.
pub type SubscribeReceiver = LoadAwareReceiver<SubscribeQueueItem>;

/// One read of a client: an update with its filter names, or the message of the error status
/// that ends its stream.
pub type ClientRead = Result<SubscribeUpdate, String>;

/// A Subscribe queue item, read as a client reads it: decoded from the bytes it encodes to.
pub trait DecodeQueueItem {
    /// Appends what a client reads from this item, in order.
    fn decode_reads_into(&self, client_reads: &mut Vec<ClientRead>);

    /// What a client reads from this item, in order.
    fn decode_reads(&self) -> Vec<ClientRead> {
        let mut client_reads = Vec::new();
        self.decode_reads_into(&mut client_reads);
        client_reads
    }

    /// The one read a client makes of this item.
    /// Panics on an item that holds more updates or none.
    fn decode_read(&self) -> ClientRead {
        match <[ClientRead; 1]>::try_from(self.decode_reads()) {
            Ok([client_read]) => client_read,
            Err(client_reads) => panic!("{} reads from one queue item", client_reads.len()),
        }
    }

    /// The one update a client reads from this item.
    /// Panics on an error status, and as [`DecodeQueueItem::decode_read`] does.
    fn decode_update(&self) -> SubscribeUpdate {
        self.decode_read().unwrap_or_else(|status_message| {
            panic!("an error status, not an update: {status_message}")
        })
    }
}

impl DecodeQueueItem for FilteredUpdate {
    fn decode_reads_into(&self, client_reads: &mut Vec<ClientRead>) {
        let update_bytes = self.encode_to_vec();
        let decoded_update =
            SubscribeUpdate::decode(update_bytes.as_slice()).expect("a queued update decodes");
        client_reads.push(Ok(decoded_update));
    }
}

impl DecodeQueueItem for SubscribeQueueEntry {
    fn decode_reads_into(&self, client_reads: &mut Vec<ClientRead>) {
        let update_bytes = self.encode_to_vec();
        let decoded_update =
            SubscribeUpdate::decode(update_bytes.as_slice()).expect("a queued entry decodes");
        client_reads.push(Ok(decoded_update));
    }
}

impl<T: DecodeQueueItem> DecodeQueueItem for tonic::Result<T> {
    fn decode_reads_into(&self, client_reads: &mut Vec<ClientRead>) {
        match self {
            Ok(queued_update) => queued_update.decode_reads_into(client_reads),
            Err(error_status) => client_reads.push(Err(error_status.message().to_owned())),
        }
    }
}

mod tests {
    use {
        super::*,
        crate::plugin::{
            filter::{fixtures, message::FilteredUpdateOneof, name::FilterName},
            message::SlotStatus,
        },
        std::sync::Arc,
        yellowstone_grpc_proto::geyser::{
            subscribe_update::UpdateOneof, SlotStatus as SlotStatusProto, SubscribeUpdateSlot,
        },
    };

    /// A queued update reads as the `SubscribeUpdate` its bytes decode to, names included, and
    /// an error status as its message.
    #[test]
    fn a_queue_item_reads_as_its_decoded_update_or_its_status_message() {
        let slot_message = fixtures::message_slot(7, SlotStatus::Confirmed);
        let mut slot_update =
            FilteredUpdate::new_empty(FilteredUpdateOneof::slot(Arc::new(slot_message)));
        slot_update.filters = ["first", "second"]
            .map(FilterName::new)
            .into_iter()
            .collect();
        let expected_update = SubscribeUpdate {
            filters: vec!["first".to_owned(), "second".to_owned()],
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 7,
                parent: Some(6),
                status: SlotStatusProto::SlotConfirmed as i32,
                dead_error: None,
                bank_id: Some(7),
            })),
            created_at: Some(slot_update.created_at),
        };
        assert_eq!(slot_update.decode_update(), expected_update);
        let queued_items: [SubscribeQueueItem; 2] = [
            Ok(SubscribeQueueEntry::Filtered(slot_update)),
            Err(tonic::Status::internal("lagged to send an update")),
        ];
        let client_reads: Vec<ClientRead> = queued_items
            .iter()
            .flat_map(DecodeQueueItem::decode_reads)
            .collect();
        let expected_reads = [
            Ok(expected_update),
            Err("lagged to send an update".to_owned()),
        ];
        assert_eq!(client_reads, expected_reads);
    }
}
