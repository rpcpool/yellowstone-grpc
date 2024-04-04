use {std::iter::repeat, yellowstone_grpc_proto::geyser::SubscribeUpdateAccount};

type Pubkey = [u8; 32];

pub struct AccountUpdate {
    pub slot: i64,
    pub pubkey: Pubkey,
    pub lamports: i64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: i64,
    pub write_version: i64,
    pub data: Vec<u8>,
    pub txn_signature: Option<Vec<u8>>,
}

impl From<AccountUpdate>
    for (
        i64,
        Pubkey,
        i64,
        Pubkey,
        bool,
        i64,
        i64,
        Vec<u8>,
        Option<Vec<u8>>,
    )
{
    fn from(acc: AccountUpdate) -> Self {
        (
            acc.slot,
            acc.pubkey,
            acc.lamports,
            acc.owner,
            acc.executable,
            acc.rent_epoch,
            acc.write_version,
            acc.data,
            acc.txn_signature,
        )
    }
}

impl AccountUpdate {
    #[allow(clippy::type_complexity)]
    pub fn as_row(
        self,
    ) -> (
        i64,
        Pubkey,
        i64,
        Pubkey,
        bool,
        i64,
        i64,
        Vec<u8>,
        Option<Vec<u8>>,
    ) {
        self.into()
    }

    pub fn zero_account() -> Self {
        let bytes_vec: Vec<u8> = repeat(0).take(32).collect();
        let bytes_arr: [u8; 32] = bytes_vec.try_into().unwrap();
        AccountUpdate {
            slot: 0,
            pubkey: bytes_arr,
            lamports: 0,
            owner: bytes_arr,
            executable: false,
            rent_epoch: 0,
            write_version: 0,
            data: vec![],
            txn_signature: None,
        }
    }
}

impl TryFrom<SubscribeUpdateAccount> for AccountUpdate {
    type Error = ();
    fn try_from(value: SubscribeUpdateAccount) -> Result<Self, Self::Error> {
        let slot = value.slot;
        if value.account.is_none() {
            Err(())
        } else {
            let acc: yellowstone_grpc_proto::prelude::SubscribeUpdateAccountInfo =
                value.account.unwrap();
            let pubkey: Pubkey = acc.pubkey.try_into().map_err(|_| ())?;
            let owner: Pubkey = acc.owner.try_into().map_err(|_| ())?;
            let ret = AccountUpdate {
                slot: slot as i64,
                pubkey,
                lamports: acc.lamports as i64,
                owner,
                executable: acc.executable,
                rent_epoch: acc.rent_epoch as i64,
                write_version: acc.write_version as i64,
                data: acc.data,
                txn_signature: acc.txn_signature,
            };
            Ok(ret)
        }
    }
}
