pub use {prost, prost_types};

pub mod geyser {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]

    include!(concat!(env!("OUT_DIR"), "/geyser.rs"));
}

pub mod solana {
    #![allow(clippy::missing_const_for_fn)]

    pub mod storage {
        pub mod confirmed_block {
            include!(concat!(
                env!("OUT_DIR"),
                "/solana.storage.confirmed_block.rs"
            ));
        }
    }
}

pub mod gen {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]

    include!(concat!(env!("OUT_DIR"), "/geyser.Geyser.rs"));
}

pub mod prelude {
    pub use super::{geyser::*, solana::storage::confirmed_block::*};
}
