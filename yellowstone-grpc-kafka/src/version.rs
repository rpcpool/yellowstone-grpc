use {serde::Serialize, std::env};

#[derive(Debug, Serialize)]
pub struct Version {
    pub package: &'static str,
    pub version: &'static str,
    pub proto: &'static str,
    pub solana: &'static str,
    pub git: &'static str,
    pub rustc: &'static str,
    pub buildts: &'static str,
}

pub const VERSION: Version = Version {
    package: env!("CARGO_PKG_NAME"),
    version: env!("CARGO_PKG_VERSION"),
    proto: env!("YELLOWSTONE_GRPC_PROTO_VERSION"),
    solana: env!("SOLANA_SDK_VERSION"),
    git: env!("GIT_VERSION"),
    rustc: env!("VERGEN_RUSTC_SEMVER"),
    buildts: env!("VERGEN_BUILD_TIMESTAMP"),
};
