//! The acceptor of the --domain mode: axum-server serves the certificates
//! rustls-acme gets from Let's Encrypt, and answers its TLS-ALPN-01
//! challenges. rustls-acme has this acceptor behind its `axum` feature, but
//! for axum-server 0.8's `Accept` trait, so it is ported here to the
//! axum-server this crate uses. It works as rustls-acme's does: a challenge
//! handshake is finished with the challenge certificate and then closed,
//! and every other handshake is finished with the given config.

use rustls_acme::futures_rustls::rustls::ServerConfig;
use rustls_acme::futures_rustls::server::TlsStream;
use rustls_acme::futures_rustls::Accept;
use rustls_acme::{AcmeAccept, AcmeAcceptor, AcmeState};
use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

#[derive(Clone)]
pub struct AxumAcceptor {
    acme_acceptor: AcmeAcceptor,
    config: Arc<ServerConfig>,
}

impl AxumAcceptor {
    /// Answers the challenges of `state` and finishes every other handshake
    /// with `config`, as `state.axum_acceptor(config)` does.
    pub fn new<EC: Debug + 'static, EA: Debug + 'static>(state: &AcmeState<EC, EA>, config: Arc<ServerConfig>) -> Self {
        // rustls-acme deprecates acceptor() for its stream of connections,
        // which axum-server can't take. Its own axum acceptor calls it too.
        #[allow(deprecated)]
        let acme_acceptor = state.acceptor();
        AxumAcceptor { acme_acceptor, config }
    }
}

impl<I: AsyncRead + AsyncWrite + Unpin + Send + 'static, S: Send + 'static> axum_server::accept::Accept<I, S> for AxumAcceptor {
    type Stream = Compat<TlsStream<Compat<I>>>;
    type Service = S;
    type Future = AxumAccept<I, S>;

    fn accept(&self, stream: I, service: S) -> Self::Future {
        AxumAccept {
            config: self.config.clone(),
            acme_accept: self.acme_acceptor.accept(stream.compat()),
            tls_accept: None,
            service: Some(service),
        }
    }
}

/// Reads the ClientHello, then finishes the handshake: a challenge's with
/// the challenge certificate, which ends in an error so that the server
/// drops the connection, and any other with the acceptor's config.
pub struct AxumAccept<I: AsyncRead + AsyncWrite + Unpin + Send + 'static, S: Send + 'static> {
    config: Arc<ServerConfig>,
    acme_accept: AcmeAccept<Compat<I>>,
    tls_accept: Option<Accept<Compat<I>>>,
    service: Option<S>,
}

// The service is only moved out, never pinned.
impl<I: AsyncRead + AsyncWrite + Unpin + Send + 'static, S: Send + 'static> Unpin for AxumAccept<I, S> {}

impl<I: AsyncRead + AsyncWrite + Unpin + Send + 'static, S: Send + 'static> Future for AxumAccept<I, S> {
    type Output = io::Result<(Compat<TlsStream<Compat<I>>>, S)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            if let Some(tls_accept) = &mut self.tls_accept {
                return match Pin::new(&mut *tls_accept).poll(cx) {
                    Poll::Ready(Ok(tls)) => Poll::Ready(Ok((tls.compat(), self.service.take().unwrap()))),
                    Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                    Poll::Pending => Poll::Pending,
                };
            }
            return match Pin::new(&mut self.acme_accept).poll(cx) {
                Poll::Ready(Ok(Some(start_handshake))) => {
                    let config = self.config.clone();
                    self.tls_accept = Some(start_handshake.into_stream(config));
                    continue;
                }
                Poll::Ready(Ok(None)) => Poll::Ready(Err(io::Error::other("TLS-ALPN-01 validation request"))),
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                Poll::Pending => Poll::Pending,
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AxumAcceptor;
    use axum::http::header::LOCATION;
    use axum::http::StatusCode;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use rustls_acme::futures_rustls::rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls_acme::futures_rustls::rustls::crypto::{aws_lc_rs, CryptoProvider};
    use rustls_acme::futures_rustls::rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
    use rustls_acme::futures_rustls::rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme};
    use rustls_acme::futures_rustls::TlsConnector;
    use rustls_acme::AcmeConfig;
    use serde_json::json;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Notify;
    use tokio_stream::StreamExt;
    use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

    /// The DER of the OID of the acmeIdentifier extension (RFC 8737), which
    /// only a TLS-ALPN-01 challenge certificate has.
    const ACME_IDENTIFIER_OID: &[u8] = &[0x06, 0x08, 0x2b, 0x06, 0x01, 0x05, 0x05, 0x07, 0x01, 0x1f];

    fn provider() -> Arc<CryptoProvider> {
        Arc::new(aws_lc_rs::default_provider())
    }

    /// A self-signed certificate for localhost, in place of the one from
    /// Let's Encrypt, and the config that serves it.
    fn localhost_config() -> (Arc<ServerConfig>, CertificateDer<'static>) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der = cert.cert.der().clone();
        let key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
        let config = ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key.into())
            .unwrap();
        (Arc::new(config), cert_der)
    }

    /// Serves "hello" on 127.0.0.1 through `acceptor`, as main() serves the
    /// app in the --domain mode, and returns the address.
    async fn serve(acceptor: AxumAcceptor) -> SocketAddr {
        let app = Router::new().route("/hello", get(|| async { "hello" }));
        let handle = axum_server::Handle::new();
        let server = axum_server::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .handle(handle.clone())
            .acceptor(acceptor)
            .serve(app.into_make_service_with_connect_info::<SocketAddr>());
        tokio::spawn(server);
        handle.listening().await.unwrap()
    }

    /// A stand-in for Let's Encrypt on 127.0.0.1, over plain HTTP. It takes
    /// any account and any order, for localhost, and keeps the order's one
    /// TLS-ALPN-01 challenge pending, so rustls-acme keeps serving the
    /// challenge certificate. Nonces and signatures aren't checked. Returns
    /// the directory's URL and what is notified when rustls-acme asks for
    /// the challenge to be validated, which it does once it serves the
    /// challenge certificate.
    async fn acme_directory() -> (String, Arc<Notify>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let validate = Arc::new(Notify::new());
        let url = |path: &str| format!("{base}{path}");
        let directory = json!({"newNonce": url("/nonce"), "newAccount": url("/account"), "newOrder": url("/order")});
        let account = [(LOCATION, url("/account/1"))];
        let order_location = [(LOCATION, url("/order/1"))];
        let order = json!({
            "status": "pending",
            "identifiers": [{"type": "dns", "value": "localhost"}],
            "authorizations": [url("/authz/1")],
            "finalize": url("/order/1/finalize"),
        });
        let challenge = json!({"type": "tls-alpn-01", "url": url("/challenge/1"), "token": "token", "status": "pending"});
        let authz = json!({"status": "pending", "identifier": {"type": "dns", "value": "localhost"}, "challenges": [challenge]});
        let notify = validate.clone();
        let app = Router::new()
            .route("/directory", get(move || async move { Json(directory) }))
            .route("/nonce", get(|| async { [("replay-nonce", "nonce")] }))
            .route("/account", post(move || async move { (StatusCode::CREATED, account, Json(json!({"status": "valid"}))) }))
            .route("/order", post(move || async move { (StatusCode::CREATED, order_location, Json(order)) }))
            .route("/authz/1", post(move || async move { Json(authz) }))
            .route(
                "/challenge/1",
                post(move || async move {
                    notify.notify_one();
                    Json(challenge)
                }),
            );
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url("/directory"), validate)
    }

    /// Accepts any certificate and any handshake signature: only which
    /// certificate the server presents matters here. An ACME validator
    /// checks the challenge certificate's acmeIdentifier rather than its
    /// chain, and webpki, which rustls's signature checks parse the
    /// certificate with, refuses that extension, as it is critical.
    #[derive(Debug)]
    struct AnyCertificate;

    impl ServerCertVerifier for AnyCertificate {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, rustls_acme::futures_rustls::rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls_acme::futures_rustls::rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls_acme::futures_rustls::rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            provider().signature_verification_algorithms.supported_schemes()
        }
    }

    /// A client that offers the ALPN protocols `alpn` and trusts `cert`, or
    /// any certificate when `cert` is None.
    fn connector(cert: Option<&CertificateDer<'static>>, alpn: &[&str]) -> TlsConnector {
        let builder = ClientConfig::builder_with_provider(provider()).with_safe_default_protocol_versions().unwrap();
        let mut config = match cert {
            Some(cert) => {
                let mut roots = RootCertStore::empty();
                roots.add(cert.clone()).unwrap();
                builder.with_root_certificates(roots).with_no_client_auth()
            }
            None => builder.dangerous().with_custom_certificate_verifier(Arc::new(AnyCertificate)).with_no_client_auth(),
        };
        config.alpn_protocols = alpn.iter().map(|p| p.as_bytes().to_vec()).collect();
        TlsConnector::from(Arc::new(config))
    }

    fn localhost() -> ServerName<'static> {
        ServerName::try_from("localhost").unwrap()
    }

    /// Sends a GET of /hello and returns all that comes back, whether or not
    /// the server closes with a close_notify, or at all cleanly.
    async fn get_hello<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(mut tls: T) -> String {
        let _ = tls.write_all(b"GET /hello HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").await;
        let mut response = Vec::new();
        let _ = tls.read_to_end(&mut response).await;
        String::from_utf8(response).unwrap()
    }

    // The state is never polled, so it holds no certificate of its own and
    // no challenge: the given config's certificate is the only one there is.
    #[tokio::test]
    async fn serves_a_tls_connection_with_the_given_config() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let (config, cert) = localhost_config();
            let addr = serve(AxumAcceptor::new(&AcmeConfig::new(["localhost"]).state(), config)).await;
            let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            let tls = connector(Some(&cert), &[]).connect(localhost(), tcp.compat()).await.unwrap();
            let response = get_hello(tls.compat()).await;
            assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
            assert!(response.ends_with("\r\n\r\nhello"), "{response}");
        })
        .await
        .unwrap();
    }

    // A challenge handshake goes to the ACME state's challenge config, never
    // to the given one: with no challenge pending the handshake fails,
    // though the given config's certificate would have passed. The server
    // keeps serving.
    #[tokio::test]
    async fn sends_a_challenge_to_the_acme_state() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let (config, cert) = localhost_config();
            let addr = serve(AxumAcceptor::new(&AcmeConfig::new(["localhost"]).state(), config)).await;
            let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            let challenge = connector(Some(&cert), &["acme-tls/1"]).connect(localhost(), tcp.compat()).await;
            assert!(challenge.is_err());

            let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            connector(Some(&cert), &[]).connect(localhost(), tcp.compat()).await.unwrap();
        })
        .await
        .unwrap();
    }

    // With a challenge pending, as while Let's Encrypt validates an order,
    // the challenge handshake completes with the challenge certificate, and
    // then the connection is dropped without serving HTTP. Other handshakes
    // still get the given config's certificate.
    #[tokio::test]
    async fn answers_a_pending_challenge_and_drops_the_connection() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let (directory, validate) = acme_directory().await;
            let mut state = AcmeConfig::new(["localhost"]).directory(directory).state();
            let (config, cert) = localhost_config();
            let addr = serve(AxumAcceptor::new(&state, config)).await;
            tokio::spawn(async move {
                while let Some(event) = state.next().await {
                    eprintln!("ACME event: {event:?}");
                }
            });
            validate.notified().await;

            let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            let tls = connector(None, &["acme-tls/1"]).connect(localhost(), tcp.compat()).await.unwrap();
            let (_, connection) = tls.get_ref();
            assert_eq!(connection.alpn_protocol(), Some(&b"acme-tls/1"[..]));
            let presented = &connection.peer_certificates().unwrap()[0];
            assert_ne!(presented, &cert);
            assert!(presented.windows(ACME_IDENTIFIER_OID.len()).any(|w| w == ACME_IDENTIFIER_OID));
            assert_eq!(get_hello(tls.compat()).await, "");

            let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            let tls = connector(Some(&cert), &[]).connect(localhost(), tcp.compat()).await.unwrap();
            assert!(get_hello(tls.compat()).await.ends_with("\r\n\r\nhello"));
        })
        .await
        .unwrap();
    }
}
