//! `TlsProvider` over `futures-rustls` with the pure-Rust `rustls-rustcrypto`
//! crypto provider. The certificate verifier deliberately accepts any cert:
//! Tor does not use web-PKI; the relay cert is only a container for the link
//! key, and real authentication happens later via the in-protocol CERTS cell
//! (see the `TlsConnector` rustdoc in tor-rtcompat). This is correct for Tor.

use std::borrow::Cow;
use std::io::{self, Result as IoResult};
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use futures::io::{AsyncRead, AsyncWrite};
use futures_rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use futures_rustls::rustls::client::danger;
use futures_rustls::rustls::crypto::{
    verify_tls12_signature, verify_tls13_signature, CryptoProvider, WebPkiSupportedAlgorithms,
};
use futures_rustls::rustls::{
    self, DigitallySignedStruct, DistinguishedName, Error as TlsError, SignatureScheme,
};

use tor_rtcompat::tls::{TlsAcceptorSettings, TlsConnector};
use tor_rtcompat::{CertifiedConn, StreamOps, TlsProvider};

/// A [`TlsProvider`] using rustls with the rustcrypto crypto backend.
#[derive(Clone)]
pub struct TorTls {
    config: Arc<rustls::ClientConfig>,
}

impl TorTls {
    pub fn new() -> Self {
        // Install the pure-Rust crypto provider process-wide (idempotent).
        let _ = CryptoProvider::install_default(rustls_rustcrypto::provider());
        let provider = CryptoProvider::get_default().expect("crypto provider installed");

        let mut config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(Verifier(
                provider.signature_verification_algorithms,
            )))
            .with_no_client_auth();

        // tor-spec forbids TLS session resumption.
        config.resumption = rustls::client::Resumption::disabled();

        TorTls {
            config: Arc::new(config),
        }
    }
}

impl Default for TorTls {
    fn default() -> Self {
        Self::new()
    }
}

// --- CertifiedConn / StreamOps for the client TLS stream -------------------

/// Newtype so we can implement the tor-rtcompat traits on the TLS stream.
pub struct TorTlsStream<S>(futures_rustls::client::TlsStream<S>);

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for TorTlsStream<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
    }
}
impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for TorTlsStream<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_close(cx)
    }
}

impl<S: StreamOps> StreamOps for TorTlsStream<S> {
    fn set_tcp_notsent_lowat(&self, notsent_lowat: u32) -> IoResult<()> {
        self.0.get_ref().0.set_tcp_notsent_lowat(notsent_lowat)
    }
    fn new_handle(&self) -> Box<dyn StreamOps + Send + Unpin> {
        self.0.get_ref().0.new_handle()
    }
}

impl<S> CertifiedConn for TorTlsStream<S> {
    fn peer_certificate(&self) -> IoResult<Option<Cow<'_, [u8]>>> {
        let (_, session) = self.0.get_ref();
        Ok(session
            .peer_certificates()
            .and_then(|certs| certs.first().map(|c| Cow::from(c.as_ref()))))
    }

    fn export_keying_material(
        &self,
        len: usize,
        label: &[u8],
        context: Option<&[u8]>,
    ) -> IoResult<Vec<u8>> {
        let (_, session) = self.0.get_ref();
        session
            .export_keying_material(vec![0_u8; len], label, context)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    fn own_certificate(&self) -> IoResult<Option<Cow<'_, [u8]>>> {
        Ok(None)
    }
}

// --- connector -------------------------------------------------------------

pub struct TorConnector<S> {
    connector: futures_rustls::TlsConnector,
    _phantom: PhantomData<fn(S) -> S>,
}

#[async_trait]
impl<S> TlsConnector<S> for TorConnector<S>
where
    S: AsyncRead + AsyncWrite + StreamOps + Unpin + Send + 'static,
{
    type Conn = TorTlsStream<S>;

    async fn negotiate_unvalidated(&self, stream: S, sni_hostname: &str) -> IoResult<Self::Conn> {
        let name: ServerName<'_> = sni_hostname
            .try_into()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let s = self.connector.connect(name.to_owned(), stream).await?;
        Ok(TorTlsStream(s))
    }
}

impl<S> TlsProvider<S> for TorTls
where
    S: AsyncRead + AsyncWrite + StreamOps + Unpin + Send + 'static,
{
    type Connector = TorConnector<S>;
    type TlsStream = TorTlsStream<S>;
    type Acceptor = NoAcceptor<S>;
    type TlsServerStream = UnsupportedTls;

    fn tls_connector(&self) -> TorConnector<S> {
        TorConnector {
            connector: futures_rustls::TlsConnector::from(Arc::clone(&self.config)),
            _phantom: PhantomData,
        }
    }

    fn tls_acceptor(&self, _settings: TlsAcceptorSettings) -> IoResult<NoAcceptor<S>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "tls server (onion hosting) not yet supported",
        ))
    }

    fn supports_keying_material_export(&self) -> bool {
        true
    }
}

// --- unsupported server side ----------------------------------------------

pub struct NoAcceptor<S>(PhantomData<fn() -> S>);

#[async_trait]
impl<S> TlsConnector<S> for NoAcceptor<S>
where
    S: AsyncRead + AsyncWrite + StreamOps + Unpin + Send + 'static,
{
    type Conn = UnsupportedTls;
    async fn negotiate_unvalidated(&self, _stream: S, _sni: &str) -> IoResult<Self::Conn> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "tls server not supported",
        ))
    }
}

/// Uninhabited server-stream type (never constructed).
pub enum UnsupportedTls {}
impl AsyncRead for UnsupportedTls {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut [u8],
    ) -> std::task::Poll<io::Result<usize>> {
        match *self {}
    }
}
impl AsyncWrite for UnsupportedTls {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        match *self {}
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match *self {}
    }
    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match *self {}
    }
}
impl StreamOps for UnsupportedTls {}
impl CertifiedConn for UnsupportedTls {
    fn peer_certificate(&self) -> IoResult<Option<Cow<'_, [u8]>>> {
        match *self {}
    }
    fn export_keying_material(
        &self,
        _len: usize,
        _label: &[u8],
        _context: Option<&[u8]>,
    ) -> IoResult<Vec<u8>> {
        match *self {}
    }
    fn own_certificate(&self) -> IoResult<Option<Cow<'_, [u8]>>> {
        match *self {}
    }
}

// --- Tor's accept-any certificate verifier ---------------------------------

#[derive(Debug)]
struct Verifier(WebPkiSupportedAlgorithms);

impl danger::ServerCertVerifier for Verifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer,
        _roots: &[CertificateDer],
        _server_name: &ServerName,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<danger::ServerCertVerified, TlsError> {
        Ok(danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer,
        dss: &DigitallySignedStruct,
    ) -> Result<danger::HandshakeSignatureValid, TlsError> {
        verify_tls12_signature(message, cert, dss, &self.0)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer,
        dss: &DigitallySignedStruct,
    ) -> Result<danger::HandshakeSignatureValid, TlsError> {
        verify_tls13_signature(message, cert, dss, &self.0)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_schemes()
    }

    fn root_hint_subjects(&self) -> Option<&[DistinguishedName]> {
        None
    }
}
