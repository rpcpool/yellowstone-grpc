#![allow(clippy::large_enum_variant)]

pub mod geyser {
    tonic::include_proto!("geyser");
}

pub mod solana {
    pub mod storage {
        pub mod confirmed_block {
            tonic::include_proto!("solana.storage.confirmed_block");
        }
    }
}

pub mod prelude {
    pub use super::geyser::*;
    pub use super::solana::storage::confirmed_block::*;
}
