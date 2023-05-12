use {serde::Serialize, std::env};

#[derive(Debug, Serialize)]
pub struct Version {
    pub version: &'static str,
    pub solana: &'static str,
    pub git: &'static str,
    pub rustc: &'static str,
    pub buildts: &'static str,
}

pub const VERSION: Version = Version {
    version: env!("VERGEN_BUILD_SEMVER"),
    solana: env!("SOLANA_SDK_VERSION"),
    git: env!("GIT_VERSION"),
    rustc: env!("VERGEN_RUSTC_SEMVER"),
    buildts: env!("VERGEN_BUILD_TIMESTAMP"),
};
