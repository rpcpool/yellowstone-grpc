use tonic::{service::interceptor, Status};

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

pub trait XTokenResolver {
    type Extension: Clone + Send + Sync + 'static;

    fn resolve(&self, x_token: &str) -> Option<Self::Extension>;
}

#[derive(Clone)]
pub struct XTokenResolverInterceptor<R> {
    resolver: R,

    /// When false, requests without x-token will be allowed to proceed with a default subscription tier (e.g. "free"). When true, requests without x-token will be rejected.
    must_authenticate: bool,
}

impl<R> XTokenResolverInterceptor<R> {
    pub fn new(resolver: R, must_authenticate: bool) -> Self {
        Self {
            resolver,
            must_authenticate,
        }
    }
}

impl<R> interceptor::Interceptor for XTokenResolverInterceptor<R>
where
    R: XTokenResolver,
{
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        if let Some(x_token_ascii) = request.metadata().get("x-token") {
            let x_token_str = x_token_ascii
                .to_str()
                .map_err(|_| Status::unauthenticated("invalid x-token format"))?;
            if let Some(ext) = self.resolver.resolve(x_token_str) {
                request.extensions_mut().insert(ext);
                Ok(request)
            } else {
                Err(Status::unauthenticated("Invalid x-token provided"))
            }
        } else if !self.must_authenticate {
            Ok(request)
        } else {
            Err(Status::unauthenticated("Missing x-token"))
        }
    }
}
