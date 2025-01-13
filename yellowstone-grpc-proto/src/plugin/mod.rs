pub mod filter;
pub mod message;

pub mod proto {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]
    tonic::include_proto!("geyser.Geyser");
}
