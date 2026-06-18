use {
    crate::server::tcp::TcpIncoming,
    arc_swap::ArcSwap,
    futures::Stream,
    std::{
        collections::{HashMap, HashSet},
        fmt,
        fs::{self, File},
        io::{self, BufReader, ErrorKind},
        net::SocketAddr,
        path::Path,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    },
    tokio::{
        io::{AsyncRead, AsyncWrite, ReadBuf},
        net::TcpStream,
        task::JoinSet,
    },
    tokio_rustls::{
        rustls::{
            crypto::aws_lc_rs::sign::any_supported_type,
            server::{ClientHello, ResolvesServerCert},
            sign::CertifiedKey,
        },
        server::TlsStream,
        TlsAcceptor,
    },
    tonic::transport::{server::Connected, CertificateDer},
    x509_parser::{extensions::GeneralName, parse_x509_certificate},
};

///
/// A certificate resolver that supports both exact SNI names and wildcard patterns.
#[derive(Default)]
pub struct SniResolver {
    exact: HashMap<String, Arc<CertifiedKey>>,
    wildcard: Vec<(String, Arc<CertifiedKey>)>,
}

impl SniResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, name: &str, certified_key: CertifiedKey) -> io::Result<()> {
        let name = name.to_ascii_lowercase();
        let cert = Arc::new(certified_key);

        if let Some(suffix) = name.strip_prefix("*.") {
            let suffix = format!(".{suffix}");
            if self
                .wildcard
                .iter()
                .any(|(existing, _)| existing == &suffix)
            {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    format!("duplicate wildcard SNI name: {name}"),
                ));
            }
            self.wildcard.push((suffix, cert));
            return Ok(());
        }

        if self.exact.insert(name.clone(), cert).is_some() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!("duplicate SNI name: {name}"),
            ));
        }

        Ok(())
    }

    fn resolve_by_name(&self, server_name: &str) -> Option<Arc<CertifiedKey>> {
        let server_name = server_name.to_ascii_lowercase();
        if let Some(cert) = self.exact.get(&server_name) {
            return Some(Arc::clone(cert));
        }

        let mut best_match: Option<(&str, Arc<CertifiedKey>)> = None;
        for (suffix, cert) in &self.wildcard {
            if !server_name.ends_with(suffix) {
                continue;
            }

            let prefix = &server_name[..server_name.len().saturating_sub(suffix.len())];
            if prefix.is_empty() || prefix.contains('.') {
                continue;
            }

            match best_match {
                Some((best_suffix, _)) if best_suffix.len() >= suffix.len() => {}
                _ => best_match = Some((suffix.as_str(), Arc::clone(cert))),
            }
        }

        best_match.map(|(_, cert)| cert)
    }
}

impl fmt::Debug for SniResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SniResolver")
            .field("exact_names", &self.exact.len())
            .field("wildcard_names", &self.wildcard.len())
            .finish()
    }
}

impl ResolvesServerCert for SniResolver {
    fn resolve(&self, client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let server_name = client_hello.server_name()?;
        self.resolve_by_name(server_name)
    }
}

/// A wrapper around [`SniResolver`] that allows hot-swapping the underlying cert resolver at runtime.
///
pub struct HotResolvesServerCertUsingSni {
    inner: Arc<ArcSwap<SniResolver>>,
}

impl Default for HotResolvesServerCertUsingSni {
    fn default() -> Self {
        Self::new()
    }
}

impl HotResolvesServerCertUsingSni {
    ///
    /// Creates a new [`HotResolvesServerCertUsingSni`] with an empty [`SniResolver`] as the initial resolver.
    ///
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(SniResolver::new())),
        }
    }

    ///
    /// Swaps the underlying [`SniResolver`] with a new one at runtime.
    ///
    pub fn swap(&self, new_resolver: SniResolver) {
        self.inner.store(Arc::new(new_resolver));
    }
}

impl From<SniResolver> for HotResolvesServerCertUsingSni {
    fn from(resolver: SniResolver) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(resolver)),
        }
    }
}

impl fmt::Debug for HotResolvesServerCertUsingSni {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl ResolvesServerCert for HotResolvesServerCertUsingSni {
    fn resolve(&self, client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        self.inner.load().resolve(client_hello)
    }
}

///
/// Builds an [`SniResolver`] by loading all PEM files in the given directory.
///
/// Each PEM file must contain at least one certificate and a private key.
/// The same cert/key pair will be registered for each DNS name found in the cert's SAN and CN fields.
///
pub fn build_sni_resolver_from_cert_dir<D: AsRef<Path>>(dir: D) -> Result<SniResolver, io::Error> {
    let mut resolver = SniResolver::new();
    let mut loaded_bundle_count = 0usize;
    let dir_path = dir.as_ref();

    if !dir_path.is_dir() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!("--cert-dir path is not a directory: {dir_path:?}"),
        )
        .into());
    }

    for entry in fs::read_dir(dir_path)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() || !is_pem_like_file(&path) {
            continue;
        }

        let mut reader = BufReader::new(File::open(&path)?);
        let certs = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
        if certs.is_empty() {
            continue;
        }

        let mut key_reader = BufReader::new(File::open(&path)?);
        let Some(key) = rustls_pemfile::private_key(&mut key_reader)? else {
            continue;
        };

        let names = extract_cert_names(&certs[0])?;
        if names.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!("no DNS names found in certificate file {}", path.display()),
            )
            .into());
        }

        let signing_key = any_supported_type(&key).map_err(|err| {
            io::Error::new(
                ErrorKind::InvalidInput,
                format!("failed to parse key in {}: {err}", path.display()),
            )
        })?;

        for name in names {
            let certified_key = CertifiedKey::new(certs.clone(), signing_key.clone());
            resolver.add(&name, certified_key).map_err(|err| {
                io::Error::new(
                    ErrorKind::InvalidInput,
                    format!(
                        "failed to register SNI cert from {} for host '{}': {err}",
                        path.display(),
                        name
                    ),
                )
            })?;
        }

        loaded_bundle_count += 1;
    }

    if loaded_bundle_count == 0 {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!(
                "no usable PEM bundles found in {dir_path:?} (each file needs cert(s) and private key)"
            ),
        )
        .into());
    }

    Ok(resolver)
}

fn is_pem_like_file(path: &Path) -> bool {
    matches!(path.extension().and_then(|ext| ext.to_str()), Some("pem"))
}

fn extract_cert_names(cert: &CertificateDer<'_>) -> Result<Vec<String>, io::Error> {
    let (_, parsed) = parse_x509_certificate(cert.as_ref()).map_err(|err| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("failed to parse certificate for SAN/CN extraction: {err}"),
        )
    })?;

    let mut names = HashSet::new();

    if let Ok(Some(san)) = parsed.subject_alternative_name() {
        for general_name in &san.value.general_names {
            if let GeneralName::DNSName(dns) = general_name {
                names.insert(dns.to_ascii_lowercase());
            }
        }
    }

    for cn_attr in parsed.subject().iter_common_name() {
        if let Ok(cn) = cn_attr.as_str() {
            names.insert(cn.to_ascii_lowercase());
        }
    }

    let mut out: Vec<String> = names.into_iter().collect();
    out.sort();
    Ok(out)
}

#[derive(Clone, Debug)]
pub struct TlsConnectInfo {
    /// Returns the local address of this connection.
    pub local_addr: Option<SocketAddr>,
    /// Returns the remote (peer) address of this connection.
    pub remote_addr: Option<SocketAddr>,
    /// Returns the SNI hostname of the server certificate used for this connection, if any.
    pub sni: Option<String>,
}

pin_project_lite::pin_project! {
    pub struct TlsIO {
        #[pin]
        inner: TlsStream<TcpStream>,
        connect_info: TlsConnectInfo,
    }
}

impl Connected for TlsIO {
    type ConnectInfo = TlsConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.connect_info.clone()
    }
}

impl AsyncRead for TlsIO {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.project().inner.poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsIO {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        self.project().inner.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.project().inner.poll_shutdown(cx)
    }
}

pub struct TlsIncoming {
    incoming: TcpIncoming,
    acceptor: TlsAcceptor,
    tasks: JoinSet<Result<TlsIO, io::Error>>,
}

impl TlsIncoming {
    pub fn new(incoming: TcpIncoming, acceptor: TlsAcceptor) -> Self {
        Self {
            incoming,
            acceptor,
            tasks: JoinSet::new(),
        }
    }
}

impl Stream for TlsIncoming {
    type Item = Result<TlsIO, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match Pin::new(&mut this.tasks).poll_join_next(cx) {
                Poll::Ready(Some(Ok(Ok(stream)))) => return Poll::Ready(Some(Ok(stream))),
                Poll::Ready(Some(Ok(Err(err)))) => {
                    eprintln!("TLS handshake error: {err}");
                    continue;
                }
                Poll::Ready(Some(Err(err))) => {
                    return Poll::Ready(Some(Err(io::Error::other(format!(
                        "TLS handshake task failed: {err}"
                    )))));
                }
                Poll::Ready(None) | Poll::Pending => {}
            }

            match Pin::new(&mut this.incoming).poll_next(cx) {
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Ok(tcp_stream))) => {
                    let remote_addr = tcp_stream.peer_addr().ok();
                    let acceptor = this.acceptor.clone();
                    let local_addr = tcp_stream.local_addr().ok();
                    this.tasks.spawn(async move {
                        let tls_stream = acceptor.accept(tcp_stream).await.map_err(|err| {
                            io::Error::new(
                                ErrorKind::InvalidData,
                                format!("({remote_addr:?}) {err}"),
                            )
                        })?;

                        let sni = tls_stream
                            .get_ref()
                            .1
                            .server_name()
                            .map(std::string::ToString::to_string);

                        Ok(TlsIO {
                            inner: tls_stream,
                            connect_info: TlsConnectInfo {
                                local_addr,
                                remote_addr,
                                sni,
                            },
                        })
                    });
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Pending => {
                    if this.tasks.is_empty() {
                        return Poll::Pending;
                    }

                    match Pin::new(&mut this.tasks).poll_join_next(cx) {
                        Poll::Ready(Some(Ok(Ok(stream)))) => return Poll::Ready(Some(Ok(stream))),
                        Poll::Ready(Some(Ok(Err(err)))) => {
                            eprintln!("TLS handshake error: {err}");
                            continue;
                        }
                        Poll::Ready(Some(Err(err))) => {
                            return Poll::Ready(Some(Err(io::Error::other(format!(
                                "TLS handshake task failed: {err}"
                            )))));
                        }
                        Poll::Ready(None) | Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        std::{
            fs,
            io::{Cursor, ErrorKind, Write},
            path::PathBuf,
            sync::atomic::{AtomicU64, Ordering},
            time::{Duration, SystemTime, UNIX_EPOCH},
        },
        tokio::io::AsyncWriteExt,
        tokio_rustls::{
            rustls::{
                self,
                pki_types::{PrivateKeyDer, ServerName},
            },
            TlsConnector,
        },
        tokio_stream::StreamExt,
    };

    const TEST_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIDQzCCAiugAwIBAgIUP+5U3MPBWVZe2aUrw4AoHZnWf7swDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDYxMTE3NTkzMVoXDTI3MDYx
MTE3NTkzMVowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAuGyyR/6nFgBoxF0UUEaXQcYnrYA0GxKTgV4ZeTIBzlEX
fBYplTucli39dl/BwSEUaW/9QiwL/lKjlpUhki3QCQQvuXWy935xuiIZPcmEpvd1
BA7y9qPBizDZm7amBrnLMU0zluePfQ71dFbIp73a4fo2SpibDtofZtqTWKcfOoIn
37OfsgMd+NmAC+yB39aFozLgV1RGlhaGBmYh0Pjy1IPzuTe+O3A7JaJc9rTvqqMe
/WWU1cokQN1mJnPZzRgTWR7Q+WgF4FXXxmx8c3Cgwk5Wfzo1ht0bvV+yd9LdSAol
nd5/Gzv2VCOJOC7IIRXl8u2HWICunX0ZLskCOvDMUQIDAQABo4GMMIGJMB0GA1Ud
DgQWBBSir783lBGxEjmbV0EnZwrj/DE+0jAfBgNVHSMEGDAWgBSir783lBGxEjmb
V0EnZwrj/DE+0jAUBgNVHREEDTALgglsb2NhbGhvc3QwDAYDVR0TAQH/BAIwADAO
BgNVHQ8BAf8EBAMCBaAwEwYDVR0lBAwwCgYIKwYBBQUHAwEwDQYJKoZIhvcNAQEL
BQADggEBAFs3lXmt+QbWpmsS60FssKAZtjgVh/ZhNBBzTrKsVIncHQlp0hj6VJCH
cLY15/ZDWKJguf3bc9kLNu9ESOQRR/1dI9RL0EZhjZFYhY2XJFo/mEpf0eZNSZVd
+TRz4wrQkzheTNqtXkHZrAjZP2sx7HUSaPNlEHs0mj/9Ji6JWkmzn7v+QcsoYZF0
NeIzn1zMo7p9tZu8e5J86GKWy9w/apoqx8PaSMq3MuDpoIH3EJYSpG9fPidqjwPS
MV3NjIpQ4ZbFjlGGSTahcXNyucDyTMIN0rPFeBMsAbeXzxVt72PngdWWuKfQVXLt
tLwey6svT+9G/HwV9WmoFYPQjNOcRoI=
-----END CERTIFICATE-----
";

    const TEST_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC4bLJH/qcWAGjE
XRRQRpdBxietgDQbEpOBXhl5MgHOURd8FimVO5yWLf12X8HBIRRpb/1CLAv+UqOW
lSGSLdAJBC+5dbL3fnG6Ihk9yYSm93UEDvL2o8GLMNmbtqYGucsxTTOW5499DvV0
Vsinvdrh+jZKmJsO2h9m2pNYpx86giffs5+yAx342YAL7IHf1oWjMuBXVEaWFoYG
ZiHQ+PLUg/O5N747cDslolz2tO+qox79ZZTVyiRA3WYmc9nNGBNZHtD5aAXgVdfG
bHxzcKDCTlZ/OjWG3Ru9X7J30t1ICiWd3n8bO/ZUI4k4LsghFeXy7YdYgK6dfRku
yQI68MxRAgMBAAECggEAS20tNxO322A8eP8GhVRxnVWBOc0CwoXE7TaCnZYttedl
fvsDc8TnJGbX0HeWYzn3wq2qO0uPdirvO/FvQv1Ypa9gI243TVCaC8HRZ/tItQ7k
/U1t4iCUUiye+zfmzD5lk5ra/B9liIS7L6MkurID2MNAPB8Q37CnAiZn9+yV8ZPD
JFMZbzeCSaTApBFTvspiDVWJ1mL1REMPkrMXAwGiRuB8MYUVZCOKMtvNbfRsgVg7
KcMV5/OjAKQlsWMopjkTaCMOf0OohEyxjO5FCE97y7Ewl6hf/O0bOzh1e47yoRw+
zG2QLgV2q3fhJMPwFXB1v9Sr1nYRk8cyKMHaDqy5dwKBgQDbOaTG+TMf1xKAhAvn
digTbBcS4USwU7IIpp7tfTG+0rEnIsQnviCc3WFPk/Z/2GK3zmPQFtKJGr1HbAGf
UGFmnjn76YNywQQI1aik2v+SNDZGCb/JApsTO/uHHbEq9447TFSiot35BcHqWGri
jwxd9bQYjnQn9xnvJng+Dk0gWwKBgQDXXJWuIsRV7sjB8pwoZFdY7oUsGQ9gF/RQ
ntd/EoivNv3F73WW8iE9f4ywSPPtcWNP1hbukv1ASaYhrxsW8vUc88GMtJpez9zk
LkLCyHkxeznlXcSoV1a24mJyvt3ZamRuJ2pKFNIQzOcgyQh4NAbCdlsFoa68IJ5L
g5DvuMwlwwKBgQCkyaTKCGJcqb93qUqFd3TSfKqvf3Oxk4g9JnpKjJQLG7cccu69
7RX4tBREzDU7jn1OKy8uKSmi892ZxV9G0RYWHBP7/2DWrq4IsgptuUzpKqQta4Cl
aXcGM010GGanpKRegJcSFZkDakeEj2fw25RxQJNa7iH0NLNi6Cj0hK2HBwKBgFz2
yF4M//egReUC10nQVqw6+h2ZC7wNWxdaGefuljYcZNuGjJoGFzc20gJe230Jzzbt
UaTWqp+PqzkrH2R+qDRBPLGCXIjE7bNKDOOMKlSjvtA18+g/G12Cx8CEh7uMY6Hx
Pb6Q0kUSTksmvJM20hwrfwslSgpHgk1Sk8QHX4iFAoGAQZuzUAw1hbPVq3Tp806E
ckQJqLtNW/scI6/+HqERbClf0O/aC2TAd0wMcDZA3w2oi6tnhz5jlOg4QFJNK4QK
sF+HCDt5QXFY9Up3hhtWqKee6Sfd+kGC2cUKNhTZ2Q5VAc4uzJ7TpBU/DX6W+DU0
+fvPCcdQG5i45xzBEPLOHf8=
-----END PRIVATE KEY-----
";

    static TEST_TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDirGuard {
        path: PathBuf,
    }

    impl TempDirGuard {
        fn new(prefix: &str) -> Self {
            let unique = TEST_TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time should be monotonic after epoch")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "triton-grpc-tools-{prefix}-{}-{nanos}-{unique}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("temporary test directory should be created");
            Self { path }
        }
    }

    impl Drop for TempDirGuard {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn is_pem_like_file_matches_expected_extensions() {
        assert!(is_pem_like_file(Path::new("cert.pem")));
        assert!(!is_pem_like_file(Path::new("cert.crt")));
        assert!(!is_pem_like_file(Path::new("cert.key")));
        assert!(!is_pem_like_file(Path::new("cert.pem.key")));
        assert!(!is_pem_like_file(Path::new("cert")));
    }

    #[test]
    fn extract_cert_names_returns_error_for_invalid_der() {
        let invalid = CertificateDer::from(vec![1, 2, 3, 4]);
        let error = extract_cert_names(&invalid).expect_err("invalid DER should fail parsing");

        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn build_sni_resolver_fails_for_non_directory() {
        let dir = TempDirGuard::new("non-dir");
        let file_path = dir.path.join("bundle.pem");
        fs::write(&file_path, b"not a directory").expect("test file should be written");

        let error = build_sni_resolver_from_cert_dir(&file_path)
            .expect_err("passing a file path should fail");
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn build_sni_resolver_fails_when_no_usable_pem_bundle_exists() {
        let dir = TempDirGuard::new("empty-bundles");

        // Non-PEM files are ignored by the loader.
        let mut txt = File::create(dir.path.join("ignore.txt")).expect("text file should exist");
        txt.write_all(b"ignored")
            .expect("text file should be writable");

        let error = build_sni_resolver_from_cert_dir(&dir.path)
            .expect_err("directory without PEM bundles should fail");

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(error.to_string().contains("no usable PEM bundles found"));
    }

    #[test]
    fn hot_resolver_swap_preserves_debug_and_no_panic() {
        let resolver = HotResolvesServerCertUsingSni::new();
        resolver.swap(SniResolver::new());

        let debug = format!("{resolver:?}");
        assert!(!debug.is_empty());
    }

    fn make_test_certified_key() -> CertifiedKey {
        let (certs, key) = load_test_cert_and_key();
        let signing_key = any_supported_type(&key).expect("test key should parse");
        CertifiedKey::new(certs, signing_key)
    }

    #[test]
    fn sni_resolver_wildcard_matches_exactly_one_label() {
        let mut resolver = SniResolver::new();
        resolver
            .add("*.rpcpool.com", make_test_certified_key())
            .expect("wildcard should register");

        assert!(resolver.resolve_by_name("api.rpcpool.com").is_some());
        assert!(resolver.resolve_by_name("rpcpool.com").is_none());
        assert!(resolver.resolve_by_name("foo.api.rpcpool.com").is_none());
    }

    #[test]
    fn sni_resolver_prefers_more_specific_wildcard() {
        let mut resolver = SniResolver::new();
        resolver
            .add("*.rpcpool.com", make_test_certified_key())
            .expect("broad wildcard should register");
        resolver
            .add("*.mainnet.rpcpool.com", make_test_certified_key())
            .expect("specific wildcard should register");

        let selected = resolver
            .resolve_by_name("api.mainnet.rpcpool.com")
            .expect("matching wildcard should resolve");
        let broad = resolver
            .resolve_by_name("api.rpcpool.com")
            .expect("broad wildcard should resolve");

        assert!(
            !Arc::ptr_eq(&selected, &broad),
            "more specific wildcard should select a different certificate"
        );
    }

    fn load_test_cert_and_key() -> (
        Vec<rustls::pki_types::CertificateDer<'static>>,
        PrivateKeyDer<'static>,
    ) {
        let mut cert_reader = Cursor::new(TEST_CERT_PEM.as_bytes());
        let certs = rustls_pemfile::certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .expect("test cert PEM should parse into certificates");

        let mut key_reader = Cursor::new(TEST_KEY_PEM.as_bytes());
        let key = rustls_pemfile::private_key(&mut key_reader)
            .expect("test key PEM should be parseable")
            .expect("test key PEM should contain one private key");

        (certs, key)
    }

    fn build_test_tls_acceptor() -> TlsAcceptor {
        let (certs, key) = load_test_cert_and_key();

        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .expect("server cert and key should build server TLS config");

        TlsAcceptor::from(Arc::new(server_config))
    }

    fn build_test_tls_connector() -> TlsConnector {
        let (certs, _key) = load_test_cert_and_key();
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(certs[0].clone())
            .expect("root store should accept test certificate");

        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();

        TlsConnector::from(Arc::new(client_config))
    }

    #[tokio::test]
    async fn tls_incoming_skips_failed_handshake_and_yields_successful_tls_stream() {
        let tcp_incoming = TcpIncoming::bind("127.0.0.1:0")
            .await
            .expect("tcp listener should bind");
        let addr = tcp_incoming
            .local_addr()
            .expect("tcp listener should have local addr");
        let mut tls_incoming = TlsIncoming::new(tcp_incoming, build_test_tls_acceptor());

        let bad_client = tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr)
                .await
                .expect("bad client should connect");
            stream
                .write_all(b"not-tls")
                .await
                .expect("bad client should write plaintext bytes");
        });

        let good_connector = build_test_tls_connector();
        let good_client = tokio::spawn(async move {
            let stream = TcpStream::connect(addr)
                .await
                .expect("good client should connect");
            let server_name = ServerName::try_from("localhost")
                .expect("localhost should be a valid TLS server name");

            good_connector
                .connect(server_name, stream)
                .await
                .expect("TLS handshake should succeed");
        });

        let accepted = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match tls_incoming.next().await {
                    Some(Ok(io)) => return io,
                    Some(Err(err)) => panic!("unexpected stream error: {err}"),
                    None => panic!("tls incoming ended unexpectedly"),
                }
            }
        })
        .await
        .expect("TlsIncoming should yield a successful stream in time");

        bad_client
            .await
            .expect("bad client task should complete without panic");
        good_client
            .await
            .expect("good client task should complete without panic");

        let connect_info = accepted.connect_info();
        assert!(connect_info.local_addr.is_some());
        assert!(connect_info.remote_addr.is_some());
        assert_eq!(connect_info.sni.as_deref(), Some("localhost"));
    }
}
