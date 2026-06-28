use agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoV3;
use crate::generator::GeyserEvent;

/// Zero-alloc borrowed view. Borrows directly from GeyserEvent fields.
pub fn account_view(event: &GeyserEvent) -> ReplicaAccountInfoV3<'_> {
    let GeyserEvent::Account {
        pubkey,
        lamports,
        owner,
        executable,
        rent_epoch,
        data,
        write_version,
        ..
    } = event
    else {
        unreachable!("account_view called on non-Account event")
    };

    ReplicaAccountInfoV3 {
        pubkey,
        lamports: *lamports,
        owner,
        executable: *executable,
        rent_epoch: *rent_epoch,
        data,
        write_version: *write_version,
        txn: None,
    }
}