pub mod config;
pub mod prom;
pub mod ser;

use {
    plerkle_messenger::{ACCOUNT_STREAM, BLOCK_STREAM, SLOT_STREAM, TRANSACTION_STREAM},
    std::fmt,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PlerkleStream {
    Account,
    Transaction,
    Block,
    Slot,
}

impl PlerkleStream {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Account => ACCOUNT_STREAM,
            Self::Transaction => TRANSACTION_STREAM,
            Self::Block => BLOCK_STREAM,
            Self::Slot => SLOT_STREAM,
        }
    }
}

impl fmt::Debug for PlerkleStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
