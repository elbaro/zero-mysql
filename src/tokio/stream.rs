use core::mem::MaybeUninit;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};

#[cfg(feature = "tokio-tls")]
use tokio_native_tls::TlsStream;

pub enum Stream {
    Tcp(BufReader<TcpStream>),
    #[cfg(feature = "tokio-tls")]
    Tls(BufReader<TlsStream<TcpStream>>),
    Unix(BufReader<UnixStream>),
}

impl Stream {
    pub fn tcp(stream: TcpStream) -> Self {
        Self::Tcp(BufReader::new(stream))
    }

    pub fn unix(stream: UnixStream) -> Self {
        Self::Unix(BufReader::new(stream))
    }

    #[cfg(feature = "tokio-tls")]
    pub async fn upgrade_to_tls(self, host: &str) -> std::io::Result<Self> {
        let tcp = match self {
            Self::Tcp(buf_reader) => buf_reader.into_inner(),
            #[cfg(feature = "tokio-tls")]
            Self::Tls(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Already using TLS",
                ));
            }
            Self::Unix(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "TLS not supported for Unix sockets",
                ));
            }
        };

        let connector = native_tls::TlsConnector::new()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let connector = tokio_native_tls::TlsConnector::from(connector);
        let tls_stream = connector
            .connect(host, tcp)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(Self::Tls(BufReader::new(tls_stream)))
    }

    pub async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        match self {
            Self::Tcp(reader) => reader.read_exact(buf).await.map(|_| ()),
            #[cfg(feature = "tokio-tls")]
            Self::Tls(reader) => reader.read_exact(buf).await.map(|_| ()),
            Self::Unix(reader) => reader.read_exact(buf).await.map(|_| ()),
        }
    }

    pub async fn read_buf_exact(&mut self, buf: &mut [MaybeUninit<u8>]) -> std::io::Result<()> {
        match self {
            Self::Tcp(reader) => read_buf_exact_impl(reader, buf).await,
            #[cfg(feature = "tokio-tls")]
            Self::Tls(reader) => read_buf_exact_impl(reader, buf).await,
            Self::Unix(reader) => read_buf_exact_impl(reader, buf).await,
        }
    }

    pub async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Tcp(reader) => reader.get_mut().write_all(buf).await,
            #[cfg(feature = "tokio-tls")]
            Self::Tls(reader) => reader.get_mut().write_all(buf).await,
            Self::Unix(reader) => reader.get_mut().write_all(buf).await,
        }
    }

    pub async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Tcp(reader) => reader.get_mut().flush().await,
            #[cfg(feature = "tokio-tls")]
            Self::Tls(reader) => reader.get_mut().flush().await,
            Self::Unix(reader) => reader.get_mut().flush().await,
        }
    }

    /// Returns true if this is a TCP connection to a loopback address
    pub fn is_tcp_loopback(&self) -> bool {
        match self {
            Self::Tcp(r) => r
                .get_ref()
                .peer_addr()
                .map(|addr| addr.ip().is_loopback())
                .unwrap_or(false),
            #[cfg(feature = "tokio-tls")]
            Self::Tls(r) => r
                .get_ref()
                .get_ref()
                .get_ref()
                .get_ref()
                .peer_addr()
                .map(|addr| addr.ip().is_loopback())
                .unwrap_or(false),
            Self::Unix(_) => false,
        }
    }
}

async fn read_buf_exact_impl<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    mut buf: &mut [MaybeUninit<u8>],
) -> std::io::Result<()> {
    while !buf.is_empty() {
        let n = reader.read_buf(&mut buf).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ));
        }
    }
    Ok(())
}
