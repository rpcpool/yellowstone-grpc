#[cfg(test)]
mod tests {
    use solana_sdk::{
        hash::Hash,
        message::Message as SolMessage,
        message::{v0::LoadedAddresses, MessageHeader},
        pubkey::Pubkey,
        signer::{keypair::Keypair, Signer},
        // signature::Signature,
        transaction::{SanitizedTransaction, Transaction},
    };
    use solana_transaction_status::TransactionStatusMeta;
    use std::collections::HashMap;
    use yellowstone_grpc_geyser::{
        config::ConfigGrpcFilters,
        filters::Filter,
        grpc::{Message, MessageTransaction, MessageTransactionInfo},
    };
    use yellowstone_grpc_proto::geyser::{
        SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterTransactions,
    };

    fn create_message_transaction(
        keypair: &Keypair,
        account_keys: Vec<Pubkey>,
    ) -> MessageTransaction {
        let message = SolMessage {
            header: MessageHeader {
                num_required_signatures: 1,
                ..MessageHeader::default()
            },
            account_keys,
            ..SolMessage::default()
        };
        let recent_blockhash = Hash::default();
        let sanitized_transaction = SanitizedTransaction::from_transaction_for_tests(
            Transaction::new(&[keypair], message, recent_blockhash),
        );
        let meta = TransactionStatusMeta {
            status: Ok(()),
            fee: 0,
            pre_balances: vec![],
            post_balances: vec![],
            inner_instructions: None,
            log_messages: None,
            pre_token_balances: None,
            post_token_balances: None,
            rewards: None,
            loaded_addresses: LoadedAddresses::default(),
            return_data: None,
            compute_units_consumed: None,
        };
        let sig = sanitized_transaction.signature();
        MessageTransaction {
            transaction: MessageTransactionInfo {
                signature: *sig,
                is_vote: true,
                transaction: sanitized_transaction,
                meta,
                index: 1,
            },
            slot: 100,
        }
    }

    #[test]
    fn test_filters_all_empty() {
        // ensure Filter can be created with empty values
        let config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            commitment: None,
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
            commitment: None,
        };
        let mut limit = ConfigGrpcFilters::default();
        limit.accounts.any = false;
        let filter = Filter::new(&config, &limit);
        // filter should fail
        assert!(filter.is_err());
    }

    #[test]
    fn test_filters_transaction_empty() {
        let mut transactions = HashMap::new();

        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,
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
            commitment: None,
        };
        let mut limit = ConfigGrpcFilters::default();
        limit.transactions.any = false;
        let filter = Filter::new(&config, &limit);
        // filter should fail
        assert!(filter.is_err());
    }

    #[test]
    fn test_filters_transaction_not_null() {
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
            commitment: None,
        };
        let mut limit = ConfigGrpcFilters::default();
        limit.transactions.any = false;
        let filter_res = Filter::new(&config, &limit);
        // filter should succeed
        assert!(filter_res.is_ok());
    }

    #[test]
    fn test_transaction_include_a() {
        let mut transactions = HashMap::new();

        let keypair_a = Keypair::new();
        let account_key_a = keypair_a.pubkey();
        let keypair_b = Keypair::new();
        let account_key_b = keypair_b.pubkey();
        let account_include = vec![account_key_a].iter().map(|k| k.to_string()).collect();
        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,
                failed: None,
                signature: None,
                account_include,
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
            commitment: None,
        };
        let limit = ConfigGrpcFilters::default();
        let filter = Filter::new(&config, &limit).unwrap();

        let message_transaction =
            create_message_transaction(&keypair_b, vec![account_key_b, account_key_a]);
        let message = Message::Transaction(message_transaction);
        let filters = filter.get_filters(&message);
        assert!(!filters.is_empty());
    }

    #[test]
    fn test_transaction_include_b() {
        let mut transactions = HashMap::new();

        let keypair_a = Keypair::new();
        let account_key_a = keypair_a.pubkey();
        let keypair_b = Keypair::new();
        let account_key_b = keypair_b.pubkey();
        let account_include = vec![account_key_b].iter().map(|k| k.to_string()).collect();
        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,
                failed: None,
                signature: None,
                account_include,
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
            commitment: None,
        };
        let limit = ConfigGrpcFilters::default();
        let filter = Filter::new(&config, &limit).unwrap();

        let message_transaction =
            create_message_transaction(&keypair_b, vec![account_key_b, account_key_a]);
        let message = Message::Transaction(message_transaction);
        let filters = filter.get_filters(&message);
        assert!(!filters.is_empty());
    }

    #[test]
    fn test_transaction_exclude() {
        let mut transactions = HashMap::new();

        let keypair_a = Keypair::new();
        let account_key_a = keypair_a.pubkey();
        let keypair_b = Keypair::new();
        let account_key_b = keypair_b.pubkey();
        let account_exclude = vec![account_key_b].iter().map(|k| k.to_string()).collect();
        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,
                failed: None,
                signature: None,
                account_include: vec![],
                account_exclude,
                account_required: vec![],
            },
        );

        let config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions,
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            commitment: None,
        };
        let limit = ConfigGrpcFilters::default();
        let filter = Filter::new(&config, &limit).unwrap();

        let message_transaction =
            create_message_transaction(&keypair_b, vec![account_key_b, account_key_a]);
        let message = Message::Transaction(message_transaction);
        let filters = filter.get_filters(&message);
        assert!(filters.is_empty());
    }

    #[test]
    fn test_transaction_required_x_include_y_z_case001() {
        let mut transactions = HashMap::new();

        let keypair_x = Keypair::new();
        let account_key_x = keypair_x.pubkey();
        let account_key_y = Pubkey::new_unique();
        let account_key_z = Pubkey::new_unique();

        // require x, include y, z
        let account_include = vec![account_key_y, account_key_z]
            .iter()
            .map(|k| k.to_string())
            .collect();
        let account_required = vec![account_key_x].iter().map(|k| k.to_string()).collect();
        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,
                failed: None,
                signature: None,
                account_include,
                account_exclude: vec![],
                account_required,
            },
        );

        let config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions,
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            commitment: None,
        };
        let limit = ConfigGrpcFilters::default();
        let filter = Filter::new(&config, &limit).unwrap();

        let message_transaction = create_message_transaction(
            &keypair_x,
            vec![account_key_x, account_key_y, account_key_z],
        );
        let message = Message::Transaction(message_transaction);
        let filters = filter.get_filters(&message);
        assert!(!filters.is_empty());
    }

    #[test]
    fn test_transaction_required_y_z_include_x() {
        let mut transactions = HashMap::new();

        let keypair_x = Keypair::new();
        let account_key_x = keypair_x.pubkey();
        let account_key_y = Pubkey::new_unique();
        let account_key_z = Pubkey::new_unique();

        // require x, include y, z
        let account_include = vec![account_key_x].iter().map(|k| k.to_string()).collect();
        let account_required = vec![account_key_y, account_key_z]
            .iter()
            .map(|k| k.to_string())
            .collect();
        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,
                failed: None,
                signature: None,
                account_include,
                account_exclude: vec![],
                account_required,
            },
        );

        let config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions,
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            commitment: None,
        };
        let limit = ConfigGrpcFilters::default();
        let filter = Filter::new(&config, &limit).unwrap();

        let message_transaction =
            create_message_transaction(&keypair_x, vec![account_key_x, account_key_z]);
        let message = Message::Transaction(message_transaction);
        let filters = filter.get_filters(&message);
        assert!(filters.is_empty());
    }
}
