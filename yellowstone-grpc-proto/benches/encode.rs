use {
    criterion::{criterion_group, criterion_main, BenchmarkId, Criterion},
    prost::Message as _,
    std::time::Duration,
    yellowstone_grpc_proto::plugin::{
        filter::message::{
            tests::{
                create_accounts, create_message_filters, load_predefined_blocks,
                load_predefined_transactions,
            },
            FilteredUpdate, FilteredUpdateOneof,
        },
        message::MessageTransaction,
    },
};

fn bench_account(c: &mut Criterion) {
    let filters = create_message_filters(&["my special filter"]);

    macro_rules! bench {
        ($updates:expr, $kind:expr) => {
            c.bench_with_input(BenchmarkId::new($kind, "ref"), $updates, |b, updates| {
                b.iter(|| {
                    for update in updates.iter() {
                        update.encode_to_vec().len();
                    }
                })
            });
            c.bench_with_input(BenchmarkId::new($kind, "prost"), $updates, |b, updates| {
                b.iter(|| {
                    for update in updates.iter() {
                        update.as_subscribe_update().encode_to_vec().len();
                    }
                })
            });
        };
    }

    let updates = create_accounts()
        .into_iter()
        .map(|(msg, data_slice)| FilteredUpdate {
            filters: filters.clone(),
            message: FilteredUpdateOneof::account(&msg, data_slice),
        })
        .collect::<Vec<_>>();
    bench!(&updates, "accounts");

    let updates = load_predefined_transactions()
        .into_iter()
        .map(|transaction| FilteredUpdate {
            filters: filters.clone(),
            message: FilteredUpdateOneof::transaction(&MessageTransaction {
                transaction,
                slot: 42,
            }),
        })
        .collect::<Vec<_>>();
    bench!(&updates, "transactions");

    let updates = load_predefined_blocks()
        .into_iter()
        .map(|block| FilteredUpdate {
            filters: filters.clone(),
            message: FilteredUpdateOneof::block(Box::new(block)),
        })
        .collect::<Vec<_>>();
    bench!(&updates, "blocks");
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(3)) // default 3
        .measurement_time(Duration::from_secs(5)); // default 5
    targets = bench_account
);
criterion_main!(benches);
