use {
    std::collections::HashMap, triton_grpc_tools::server::tonic::auth::interceptor::XTokenResolver,
};

/// A mock, for testing purposes only.
#[derive(Clone)]
pub(crate) struct PassthroughXTokenResolver;

impl XTokenResolver for PassthroughXTokenResolver {
    type Extension = SubscriptionInfo;

    fn resolve(&self, _x_token: &str) -> Option<Self::Extension> {
        SubscriptionInfo {
            subscription_id: "mock_subscription_id".to_string(),
            x_token: "mock_x_token".to_string(),
            rps_ratelimit_table: HashMap::new(),
        }
        .into()
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SubscriptionInfo {
    pub subscription_id: String,
    x_token: String,
    pub rps_ratelimit_table: HashMap<String /* method uri */, u64>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct StaticXTokenResolver {
    // In-memory store for simplicity; can be replaced with DB or external service calls as needed.
    pub x_token_mappings: Vec<SubscriptionInfo>,
}

impl XTokenResolver for StaticXTokenResolver {
    type Extension = SubscriptionInfo;

    fn resolve(&self, x_token: &str) -> Option<Self::Extension> {
        self.x_token_mappings
            .iter()
            .find(|mapping| mapping.x_token == x_token)
            .cloned()
    }
}

impl Default for StaticXTokenResolver {
    fn default() -> Self {
        Self {
            x_token_mappings: Vec::new(),
        }
    }
}

///
/// Loads x-token to subscription mapping from a JSON file.
/// The JSON file should contain an array of objects with the following structure:
/// ```json
/// {
///     "x_token_mappings": [
///         {
///             "subscription_id": "sub_123",
///             "x_token": "token_123",
///             "tier": "tier_123"
///         },
///         {
///             "subscription_id": "sub_456",
///             "x_token": "token_456",
///             "tier": "tier_456"
///         },
///         ...
///     ]
/// }
/// ```
pub fn load_x_token_subscription_mappings_from_file<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<StaticXTokenResolver, Box<dyn std::error::Error>> {
    let json_str = std::fs::read_to_string(path)?;
    let mappings = serde_json::from_str(&json_str)?;
    Ok(mappings)
}
