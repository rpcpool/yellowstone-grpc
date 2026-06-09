use {
    arc_swap::ArcSwap,
    std::{
        collections::HashSet,
        fmt,
        fs::{self, File},
        io::{self, BufReader, ErrorKind},
        path::Path,
        sync::Arc,
    },
    tokio_rustls::rustls::{
        crypto::aws_lc_rs::sign::any_supported_type,
        server::{ClientHello, ResolvesServerCert, ResolvesServerCertUsingSni},
        sign::CertifiedKey,
    },
    tonic::transport::CertificateDer,
    x509_parser::{extensions::GeneralName, parse_x509_certificate},
};

///
/// A wrapper around [`ResolvesServerCertUsingSni`] that allows hot-swapping the underlying cert resolver at runtime.
///
pub struct HotResolvesServerCertUsingSni {
    inner: Arc<ArcSwap<ResolvesServerCertUsingSni>>,
}

impl Default for HotResolvesServerCertUsingSni {
    fn default() -> Self {
        Self::new()
    }
}

impl HotResolvesServerCertUsingSni {
    ///
    /// Creates a new [`HotResolvesServerCertUsingSni`] with an empty [`ResolvesServerCertUsingSni`] as the initial resolver.
    ///
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(ResolvesServerCertUsingSni::new())),
        }
    }

    ///
    /// Swaps the underlying [`ResolvesServerCertUsingSni`] with a new one at runtime.
    ///
    pub fn swap(&self, new_resolver: ResolvesServerCertUsingSni) {
        self.inner.store(Arc::new(new_resolver));
    }
}

impl From<ResolvesServerCertUsingSni> for HotResolvesServerCertUsingSni {
    fn from(resolver: ResolvesServerCertUsingSni) -> Self {
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
/// Builds a `ResolvesServerCertUsingSni` by loading all PEM files in the given directory.
///
/// Each PEM file must contain at least one certificate and a private key.
/// The same cert/key pair will be registered for each DNS name found in the cert's SAN and CN fields.
///
pub fn build_sni_resolver_from_cert_dir<D: AsRef<Path>>(
    dir: D,
) -> Result<ResolvesServerCertUsingSni, io::Error> {
    let mut resolver = ResolvesServerCertUsingSni::new();
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
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("pem") | Some("crt")
    )
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
