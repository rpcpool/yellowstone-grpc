use yellowstone_grpc_proto::geyser;

mod x1 {
    pub mod x2 {
        tonic::include_proto!("yellowstone.log");
    }
}

pub mod log {
    pub use crate::x1::x2::*;
}

