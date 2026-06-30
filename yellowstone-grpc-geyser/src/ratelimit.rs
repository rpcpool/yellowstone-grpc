use {
    crate::metrics, std::net::IpAddr,
    yellowstone_grpc_tools::server::tonic::ratelimit::transport::RatelimitedCallbacks,
};

pub struct PrometheusRatelimitCallbacks;

impl RatelimitedCallbacks for PrometheusRatelimitCallbacks {
    fn on_rate_limit_exceeded(&self, remote_peer_ip: Option<IpAddr>) {
        metrics::incr_ip_conncur_rate_limit_exceeded(remote_peer_ip.map(|ip| ip.to_string()));
    }
}
