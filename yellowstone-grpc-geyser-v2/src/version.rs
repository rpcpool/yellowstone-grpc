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

#[derive(Debug, Serialize)]
pub struct GrpcVersionInfoExtra {
    hostname: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct GrpcVersionInfo {
    version: Version,
    extra: GrpcVersionInfoExtra,
}

impl Default for GrpcVersionInfo {
    fn default() -> Self {
        Self {
            version: VERSION,
            extra: GrpcVersionInfoExtra {
                hostname: hostname::get()
                    .ok()
                    .and_then(|name| name.into_string().ok()),
            },
        }
    }
}
