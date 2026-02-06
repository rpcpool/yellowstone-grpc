#![allow(clippy::large_enum_variant)]

pub mod geyser {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]

    #[cfg(feature = "tonic")]
    include!(concat!(env!("OUT_DIR"), "/geyser.rs"));
    #[cfg(not(feature = "tonic"))]
    include!(concat!(env!("OUT_DIR"), "/no-tonic/geyser.rs"));
}

pub mod solana {
    #![allow(clippy::missing_const_for_fn)]

    pub mod storage {
        pub mod confirmed_block {
            #[cfg(feature = "tonic")]
            include!(concat!(
                env!("OUT_DIR"),
                "/solana.storage.confirmed_block.rs"
            ));
            #[cfg(not(feature = "tonic"))]
            include!(concat!(
                env!("OUT_DIR"),
                "/no-tonic/solana.storage.confirmed_block.rs"
            ));
        }
    }
}

pub mod prelude {
    pub use super::{geyser::*, solana::storage::confirmed_block::*};
}

#[cfg(feature = "tonic")]
pub use tonic;
pub use {prost, prost_types};
