use {
    criterion::{criterion_group, criterion_main, BenchmarkId, Criterion},
    prost::Message as _,
    std::time::Duration,
    yellowstone_grpc_proto::plugin::message_ref::{
        tests::{
            build_subscribe_update, build_subscribe_update_account, create_accounts,
            create_message_filters,
        },
        Message, MessageRef,
    },
};

fn bench_account(c: &mut Criterion) {
    let accounts = create_accounts();
    let filters = create_message_filters(&["my special filter"]);

    c.bench_with_input(
        BenchmarkId::new("accounts", "weak"),
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
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(3)) // default 3
        .measurement_time(Duration::from_secs(5)); // default 5
    targets = bench_account
);
criterion_main!(benches);
