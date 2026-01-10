use std::io::{BufReader, Read, Write};
use std::mem::MaybeUninit;
use std::net::TcpStream;
#[cfg(unix)]
use std::os::unix::net::UnixStream;

use crate::nightly::read_uninit_exact;

#[cfg(feature = "sync-tls")]
use native_tls::TlsStream;

pub enum Stream {
    Tcp(BufReader<TcpStream>),
    #[cfg(feature = "sync-tls")]
    Tls(BufReader<TlsStream<TcpStream>>),
    #[cfg(unix)]
    Unix(BufReader<UnixStream>),
}

impl Stream {
    pub fn tcp(stream: TcpStream) -> Self {
        Self::Tcp(BufReader::new(stream))
    }

    #[cfg(unix)]
    pub fn unix(stream: UnixStream) -> Self {
        Self::Unix(BufReader::new(stream))
    }

    #[cfg(feature = "sync-tls")]
    pub fn upgrade_to_tls(self, host: &str) -> std::io::Result<Self> {
        let tcp = match self {
            Self::Tcp(buf_reader) => buf_reader.into_inner(),
            #[cfg(feature = "sync-tls")]
            Self::Tls(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Already using TLS",
                ));
            }
            #[cfg(unix)]
            Self::Unix(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "TLS not supported for Unix sockets",
                ));
            }
        };

        let connector = native_tls::TlsConnector::new()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let tls_stream = connector
            .connect(host, tcp)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(Self::Tls(BufReader::new(tls_stream)))
    }

    pub fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        match self {
            Self::Tcp(r) => r.read_exact(buf),
            #[cfg(feature = "sync-tls")]
            Self::Tls(r) => r.read_exact(buf),
            #[cfg(unix)]
            Self::Unix(r) => r.read_exact(buf),
        }
    }

    pub fn read_buf_exact(&mut self, buf: &mut [MaybeUninit<u8>]) -> std::io::Result<()> {
        match self {
            Self::Tcp(r) => read_uninit_exact(r, buf),
            #[cfg(feature = "sync-tls")]
            Self::Tls(r) => read_uninit_exact(r, buf),
            #[cfg(unix)]
            Self::Unix(r) => read_uninit_exact(r, buf),
        }
    }

    pub fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Tcp(r) => r.get_mut().write_all(buf),
            #[cfg(feature = "sync-tls")]
            Self::Tls(r) => r.get_mut().write_all(buf),
            #[cfg(unix)]
            Self::Unix(r) => r.get_mut().write_all(buf),
        }
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Tcp(r) => r.get_mut().flush(),
            #[cfg(feature = "sync-tls")]
            Self::Tls(r) => r.get_mut().flush(),
            #[cfg(unix)]
            Self::Unix(r) => r.get_mut().flush(),
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
            #[cfg(feature = "sync-tls")]
            Self::Tls(r) => r
                .get_ref()
                .get_ref()
                .peer_addr()
                .map(|addr| addr.ip().is_loopback())
                .unwrap_or(false),
            #[cfg(unix)]
            Self::Unix(_) => false,
        }
    }
}
