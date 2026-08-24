use {
    criterion::{criterion_group, criterion_main, BenchmarkId, Criterion},
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{collections::HashMap, hint::black_box, time::Duration},
    yellowstone_grpc_geyser::plugin::{
        filter::{fixtures, limits::FilterLimits, Filter},
        message::Message,
    },
    yellowstone_grpc_proto::{
        cuckoo::CuckooFilter,
        geyser::{
            CuckooFilter as ProtoCuckooFilter, SubscribeRequest, SubscribeRequestFilterAccounts,
            SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
        },
    },
};

const FILTER_COUNTS: [usize; 6] = [1, 2, 8, 64, 65, 256];
const PUBKEYS_PER_FILTER: [usize; 3] = [1, 100, 10_000];

fn unlimited() -> FilterLimits {
    FilterLimits::default()
}

fn build_filter(request: SubscribeRequest) -> Filter {
    Filter::new(&request, &unlimited(), &mut fixtures::filter_names()).expect("filter builds")
}

fn account_request(
    n_filters: usize,
    pubkeys_per_filter: usize,
    pool: &[Pubkey],
    owner: Pubkey,
) -> SubscribeRequest {
    let mut accounts = HashMap::with_capacity(n_filters);
    for i in 0..n_filters {
        let start = (i * pubkeys_per_filter) % pool.len();
        let keys = (0..pubkeys_per_filter)
            .map(|k| pool[(start + k) % pool.len()].to_string())
            .collect::<Vec<_>>();
        accounts.insert(
            format!("filter-{i}"),
            SubscribeRequestFilterAccounts {
                account: keys,
                owner: vec![owner.to_string()],
                ..Default::default()
            },
        );
    }
    SubscribeRequest {
        accounts,
        ..Default::default()
    }
}

fn accounts_by_filter_count(c: &mut Criterion) {
    let pool = fixtures::deterministic_pubkeys(1, 20_000);
    let owner = Pubkey::new_from_array([9; 32]);
    let stranger = Pubkey::new_from_array([8; 32]);

    for pubkeys_per_filter in PUBKEYS_PER_FILTER {
        let mut group = c.benchmark_group(format!("accounts/scan_{pubkeys_per_filter}"));
        for n_filters in FILTER_COUNTS {
            let filter = build_filter(account_request(n_filters, pubkeys_per_filter, &pool, owner));

            let hit = Message::Account(fixtures::simple_message_account(pool[0], owner));
            let miss = Message::Account(fixtures::simple_message_account(stranger, stranger));
            let near_miss = Message::Account(fixtures::simple_message_account(stranger, owner));

            for (label, message) in [("hit", &hit), ("miss", &miss), ("near_miss", &near_miss)] {
                group.bench_with_input(BenchmarkId::new(label, n_filters), &n_filters, |b, _| {
                    b.iter(|| black_box(filter.get_updates(black_box(message), None)))
                });
            }
        }
        group.finish();
    }
}

fn accounts_with_cuckoo(c: &mut Criterion) {
    let pool = fixtures::deterministic_pubkeys(2, 10_000);
    let owner = Pubkey::new_from_array([9; 32]);
    let stranger = Pubkey::new_from_array([8; 32]);

    let mut cuckoo = CuckooFilter::<[u8; 32]>::with_capacity(pool.len()).unwrap();
    for key in &pool {
        cuckoo.insert(&key.to_bytes()).unwrap();
    }
    let proto = ProtoCuckooFilter::from(&cuckoo);

    let mut group = c.benchmark_group("accounts/cuckoo");
    for n_filters in [1usize, 8, 64] {
        let mut accounts = HashMap::new();
        for i in 0..n_filters {
            accounts.insert(
                format!("filter-{i}"),
                SubscribeRequestFilterAccounts {
                    cuckoo_accounts_filter: Some(proto.clone()),
                    ..Default::default()
                },
            );
        }
        let filter = build_filter(SubscribeRequest {
            accounts,
            ..Default::default()
        });

        let hit = Message::Account(fixtures::simple_message_account(pool[0], owner));
        let miss = Message::Account(fixtures::simple_message_account(stranger, stranger));
        for (label, message) in [("hit", &hit), ("miss", &miss)] {
            group.bench_with_input(BenchmarkId::new(label, n_filters), &n_filters, |b, _| {
                b.iter(|| black_box(filter.get_updates(black_box(message), None)))
            });
        }
    }
    group.finish();
}

fn subscribe_filter_new(c: &mut Criterion) {
    let pool = fixtures::deterministic_pubkeys(3, 100_000);
    let owner = Pubkey::new_from_array([9; 32]);

    let mut group = c.benchmark_group("subscribe/filter_new");
    group.sample_size(20);
    for (n_filters, pubkeys_per_filter) in [(1usize, 1_000usize), (1, 10_000), (10, 10_000)] {
        let request = account_request(n_filters, pubkeys_per_filter, &pool, owner);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{n_filters}x{pubkeys_per_filter}")),
            &request,
            |b, request| {
                b.iter(|| {
                    black_box(
                        Filter::new(
                            black_box(request),
                            &unlimited(),
                            &mut fixtures::filter_names(),
                        )
                        .unwrap(),
                    )
                })
            },
        );
    }
    group.finish();
}

fn slots_only_client(c: &mut Criterion) {
    let filter = build_filter(SubscribeRequest {
        slots: HashMap::from([("s".to_owned(), SubscribeRequestFilterSlots::default())]),
        ..Default::default()
    });
    let message = Message::Account(fixtures::simple_message_account(
        Pubkey::new_from_array([1; 32]),
        Pubkey::new_from_array([2; 32]),
    ));

    c.bench_function("slots_only/account", |b| {
        b.iter(|| black_box(filter.get_updates(black_box(&message), None)))
    });
}

fn transaction_keys() -> Vec<Pubkey> {
    fixtures::deterministic_pubkeys(4, 30)
}

fn transaction_account_include(c: &mut Criterion) {
    let keys = transaction_keys();
    let message = Message::Transaction(fixtures::message_transaction(
        Signature::default(),
        keys.clone(),
        false,
        Default::default(),
    ));
    let pool = fixtures::deterministic_pubkeys(5, 10_000);

    let mut group = c.benchmark_group("transactions/account_include");
    for n in [1usize, 10, 100, 1_000, 10_000] {
        for (label, include) in [
            (
                "miss",
                pool[..n].iter().map(|k| k.to_string()).collect::<Vec<_>>(),
            ),
            (
                "hit",
                pool[..n.saturating_sub(1)]
                    .iter()
                    .map(|k| k.to_string())
                    .chain(std::iter::once(keys[15].to_string()))
                    .collect::<Vec<_>>(),
            ),
        ] {
            let filter = build_filter(SubscribeRequest {
                transactions: HashMap::from([(
                    "t".to_owned(),
                    SubscribeRequestFilterTransactions {
                        account_include: include,
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            });
            group.bench_with_input(BenchmarkId::new(label, n), &n, |b, _| {
                b.iter(|| black_box(filter.get_updates(black_box(&message), None)))
            });
        }
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets =
        accounts_by_filter_count,
        accounts_with_cuckoo,
        subscribe_filter_new,
        slots_only_client,
        transaction_account_include,
}
criterion_main!(benches);
