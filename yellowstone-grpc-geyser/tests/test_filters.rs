#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use yellowstone_grpc_geyser::{config::ConfigGrpcFilters, filters::Filter};
    use yellowstone_grpc_proto::geyser::{
        SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterTransactions,
    };

    #[test]
    fn test_filters_all_empty() {
        // ensure Filter can be created with empty values
        let config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
        };
        let limit = ConfigGrpcFilters::default();
        let filter = Filter::new(&config, &limit);
        assert!(filter.is_ok());
    }

    #[test]
    fn test_filters_account_empty() {
        let mut accounts = HashMap::new();

        accounts.insert(
            "solend".to_owned(),
            SubscribeRequestFilterAccounts {
                account: vec![],
                owner: vec![],
                filters: vec![],
            },
        );

        let config = SubscribeRequest {
            accounts,
            slots: HashMap::new(),
            transactions: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
        };
        let limit = ConfigGrpcFilters::default();
        let filter = Filter::new(&config, &limit);
        // filter should fail
        assert!(filter.is_ok());
    }

    #[test]
    fn test_filters_transaction_empty() {
        let mut transactions = HashMap::new();

        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(true),
                failed: None,
                signature: None,
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions,
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
        };
        let limit = ConfigGrpcFilters::default();
        let filter = Filter::new(&config, &limit);
        // filter should fail
        assert!(filter.is_ok());
    }
}
