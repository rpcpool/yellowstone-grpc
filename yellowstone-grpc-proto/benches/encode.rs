use {
    criterion::{criterion_group, criterion_main, BenchmarkId, Criterion},
    prost::Message as _,
    std::time::Duration,
    yellowstone_grpc_proto::{
        geyser::{subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateAccount},
        plugin::{
            filter::FilterAccountsDataSlice,
            message::MessageAccount,
            message_ref::{
                tests::{create_accounts, create_message_filters},
                Message, MessageFilters, MessageRef,
            },
        },
    },
};

fn build_subscribe_update(filters: &MessageFilters, update: UpdateOneof) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: filters.iter().map(|f| f.as_ref().to_owned()).collect(),
        update_oneof: Some(update),
    }
}

fn build_subscribe_update_account(
    message: &MessageAccount,
    data_slice: &FilterAccountsDataSlice,
) -> UpdateOneof {
    UpdateOneof::Account(SubscribeUpdateAccount {
        account: Some((message.account.as_ref(), data_slice).into()),
        slot: message.slot,
        is_startup: message.is_startup,
    })
}

fn bench_account(c: &mut Criterion) {
    let accounts = create_accounts();
    let filters = create_message_filters(&["my special filter"]);

    c.bench_with_input(
        BenchmarkId::new("accounts", "ref"),
        &(&accounts, &filters),
        |b, (accounts, filters)| {
            b.iter(|| {
                for (account, data_slice) in accounts.iter() {
                    let msg = Message {
                        filters: (*filters).clone(),
                        message: MessageRef::account(account, data_slice.clone()),
                    };
                    msg.encode_to_vec().len();
                }
            })
        },
    );
    c.bench_with_input(
        BenchmarkId::new("accounts", "prost"),
        &(&accounts, &filters),
        |b, (accounts, filters)| {
            b.iter(|| {
                for (account, data_slice) in accounts.iter() {
                    let msg = build_subscribe_update(
                        filters,
                        build_subscribe_update_account(account, data_slice),
                    );
                    msg.encode_to_vec().len();
                }
            })
        },
    );

    let accounts = accounts
        .iter()
        .map(|(account, data_slice)| build_subscribe_update_account(account, data_slice))
        .collect::<Vec<_>>();
    c.bench_with_input(
        BenchmarkId::new("accounts", "prost clone"),
        &(&accounts, &filters),
        |b, (accounts, filters)| {
            b.iter(|| {
                for account in accounts.iter() {
                    let msg = build_subscribe_update(filters, account.clone());
                    msg.encode_to_vec().len();
                }
            })
        },
    );
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(3)) // default 3
        .measurement_time(Duration::from_secs(5)); // default 5
    targets = bench_account
);
criterion_main!(benches);
