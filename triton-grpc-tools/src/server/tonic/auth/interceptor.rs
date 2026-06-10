use {
    crate::server::ratelimit::tonic::method_ratelimit::{
        RateLimitKeyExtractor, TieredRateLimitKey,
    },
    arc_swap::ArcSwap,
    http::Request,
    std::sync::Arc,
    tonic::{metadata::AsciiMetadataValue, service::interceptor, Status},
};

#[derive(Clone)]
pub struct RequireXToken;

impl interceptor::Interceptor for RequireXToken {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        match request.metadata().get("x-token") {
            Some(_) => Ok(request),
            None => Err(Status::unauthenticated("missing x-token provided")),
        }
    }
}

#[derive(Clone)]
pub struct XTokenResolverInterceptor {
    x_token_mapping: Arc<ArcSwap<XTokenSubscriptionMappings>>,
}

impl XTokenResolverInterceptor {
    pub fn new(initial_mappings: XTokenSubscriptionMappings) -> Self {
        Self {
            x_token_mapping: Arc::new(ArcSwap::new(Arc::new(initial_mappings))),
        }
    }

    /// Updates the in-memory x-token to subscription mapping.
    /// Can be called at runtime to reflect changes in token subscriptions without restarting the server.
    pub fn update_mappings(&self, new_mappings: XTokenSubscriptionMappings) {
        self.x_token_mapping.store(Arc::new(new_mappings));
    }
}

impl interceptor::Interceptor for XTokenResolverInterceptor {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        if let Some(token) = request.metadata().get("x-token") {
            let mappings = self.x_token_mapping.load();
            if let Some(mapping) = mappings
                .x_token_mappings
                .iter()
                .find(|mapping| mapping.token.as_str() == token)
            {
                request.metadata_mut().insert(
                    "x-subscription-id",
                    AsciiMetadataValue::try_from(mapping.subscription_id.as_str()).unwrap(),
                );
                request.metadata_mut().insert(
                    "x-subscription-tier",
                    AsciiMetadataValue::try_from(mapping.tier.as_str()).unwrap(),
                );
                request.extensions_mut().insert(mapping.clone());
                Ok(request)
            } else {
                Err(Status::unauthenticated("Invalid x-token"))
            }
        } else {
            Err(Status::unauthenticated("No x-token provided"))
        }
    }
}

pub struct XTokenRateLimitKeyExtractor;

impl RateLimitKeyExtractor for XTokenRateLimitKeyExtractor {
    type Key = String;

    fn extract_key_tier_pair<ReqBody>(
        &self,
        request: &Request<ReqBody>,
    ) -> Option<TieredRateLimitKey<Self::Key>> {
        request
            .extensions()
            .get::<XTokenSubscriptionMapping>()
            .map(|mapping| TieredRateLimitKey {
                key: mapping.subscription_id.clone(),
                tier: mapping.tier.clone(),
            })
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct XTokenSubscriptionMapping {
    pub subscription_id: String,
    pub token: String,
    pub tier: String,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct XTokenSubscriptionMappings {
    // In-memory store for simplicity; can be replaced with DB or external service calls as needed.
    pub x_token_mappings: Vec<XTokenSubscriptionMapping>,
}

///
/// Loads x-token to subscription mapping from a JSON file.
/// The JSON file should contain an array of objects with the following structure:
/// ```json
/// {
///     "x_token_mappings": [
///         {
///             "subscription_id": "sub_123",
///             "token": "token_123",
///             "tier": "tier_123"
///         },
///         {
///             "subscription_id": "sub_456",
///             "token": "token_456",
///             "tier": "tier_456"
///         },
///         ...
///     ]
/// }
/// ```
pub fn load_x_token_subscription_mappings_from_file<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<XTokenSubscriptionMappings, Box<dyn std::error::Error>> {
    let json_str = std::fs::read_to_string(path)?;
    let mappings = serde_json::from_str(&json_str)?;
    Ok(mappings)
}
