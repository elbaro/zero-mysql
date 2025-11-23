use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::os::unix::net::UnixStream;

#[cfg(feature = "tls")]
use native_tls::TlsStream;

pub enum Stream {
    Tcp(BufReader<TcpStream>),
    #[cfg(feature = "tls")]
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

    #[cfg(feature = "tls")]
    pub fn upgrade_to_tls(self, host: &str) -> std::io::Result<Self> {
        let tcp = match self {
            Self::Tcp(buf_reader) => buf_reader.into_inner(),
            #[cfg(feature = "tls")]
            Self::Tls(_) => return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Already using TLS",
            )),
            Self::Unix(_) => return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "TLS not supported for Unix sockets",
            )),
        };

        let connector = native_tls::TlsConnector::new()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let tls_stream = connector.connect(host, tcp)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(Self::Tls(BufReader::new(tls_stream)))
    }

    pub fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        match self {
            Self::Tcp(r) => r.read_exact(buf),
            #[cfg(feature = "tls")]
            Self::Tls(r) => r.read_exact(buf),
            Self::Unix(r) => r.read_exact(buf),
        }
    }

    pub fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Tcp(r) => r.get_mut().write_all(buf),
            #[cfg(feature = "tls")]
            Self::Tls(r) => r.get_mut().write_all(buf),
            Self::Unix(r) => r.get_mut().write_all(buf),
        }
    }

    pub fn write_all_vectored(&mut self, bufs: &mut [std::io::IoSlice<'_>]) -> std::io::Result<()> {
        match self {
            Self::Tcp(r) => r.get_mut().write_all_vectored(bufs),
            #[cfg(feature = "tls")]
            Self::Tls(r) => r.get_mut().write_all_vectored(bufs),
            Self::Unix(r) => r.get_mut().write_all_vectored(bufs),
        }
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Tcp(r) => r.get_mut().flush(),
            #[cfg(feature = "tls")]
            Self::Tls(r) => r.get_mut().flush(),
            Self::Unix(r) => r.get_mut().flush(),
        }
    }
}
