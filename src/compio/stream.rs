//! Async stream abstraction for compio with read buffering.
//!
//! Wraps the raw socket with a userspace read buffer to amortize io_uring
//! submissions. Without buffering, every `read_exact(4)` (header) +
//! `read_exact(payload)` would each be a separate io_uring submission.
//! With buffering, a single read fills the buffer and subsequent message
//! parses are served from memory.

use std::mem::MaybeUninit;

use compio::buf::BufResult;
use compio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use compio::net::TcpStream;
#[cfg(unix)]
use compio::net::UnixStream;

#[cfg(feature = "compio-tls")]
use compio::tls::TlsStream;

const READ_BUF_CAPACITY: usize = 8192;

enum StreamInner {
    Tcp(TcpStream),
    #[cfg(feature = "compio-tls")]
    Tls(TlsStream<TcpStream>),
    #[cfg(unix)]
    Unix(UnixStream),
}

/// Buffered async stream for compio.
///
/// compio's `BufReader` does not implement `AsyncWrite`, so we use a custom
/// wrapper that provides read buffering while passing writes through directly.
pub struct Stream {
    inner: StreamInner,
    read_buf: Vec<u8>,
    read_pos: usize,
}

impl Stream {
    pub fn tcp(stream: TcpStream) -> Self {
        Self {
            inner: StreamInner::Tcp(stream),
            read_buf: Vec::with_capacity(READ_BUF_CAPACITY),
            read_pos: 0,
        }
    }

    #[cfg(unix)]
    pub fn unix(stream: UnixStream) -> Self {
        Self {
            inner: StreamInner::Unix(stream),
            read_buf: Vec::with_capacity(READ_BUF_CAPACITY),
            read_pos: 0,
        }
    }

    #[cfg(feature = "compio-tls")]
    pub async fn upgrade_to_tls(self, host: &str) -> std::io::Result<Self> {
        match self.inner {
            StreamInner::Tcp(tcp_stream) => {
                let native_connector = compio::native_tls::TlsConnector::new()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                let connector = compio::tls::TlsConnector::from(native_connector);
                let tls_stream = connector.connect(host, tcp_stream).await?;
                Ok(Self {
                    inner: StreamInner::Tls(tls_stream),
                    read_buf: Vec::with_capacity(READ_BUF_CAPACITY),
                    read_pos: 0,
                })
            }
            #[cfg(feature = "compio-tls")]
            StreamInner::Tls(_) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Already using TLS",
            )),
            #[cfg(unix)]
            StreamInner::Unix(_) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "TLS not supported for Unix sockets",
            )),
        }
    }

    // --- Buffered read internals ---

    fn available(&self) -> usize {
        self.read_buf.len() - self.read_pos
    }

    /// Compact the buffer and read more data from the socket.
    async fn fill_buf(&mut self) -> std::io::Result<()> {
        if self.read_pos > 0 {
            let valid = self.available();
            self.read_buf
                .copy_within(self.read_pos..self.read_pos + valid, 0);
            self.read_buf.truncate(valid);
            self.read_pos = 0;
        }

        let buf = std::mem::take(&mut self.read_buf);
        let BufResult(result, buf) = self.read_raw(buf).await;
        self.read_buf = buf;
        let n = result?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed",
            ));
        }
        Ok(())
    }

    // --- Public read API ---

    /// Read exactly `buf.len()` bytes into the provided buffer.
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        let mut filled = 0;
        while filled < buf.len() {
            let avail = self.available();
            if avail > 0 {
                let to_copy = avail.min(buf.len() - filled);
                buf[filled..filled + to_copy]
                    .copy_from_slice(&self.read_buf[self.read_pos..self.read_pos + to_copy]);
                self.read_pos += to_copy;
                filled += to_copy;
            } else {
                self.fill_buf().await?;
            }
        }
        Ok(())
    }

    /// Read exactly `buf.len()` bytes into uninitialized memory.
    pub async fn read_buf_exact(&mut self, buf: &mut [MaybeUninit<u8>]) -> std::io::Result<()> {
        let mut filled = 0;
        while filled < buf.len() {
            let avail = self.available();
            if avail > 0 {
                let to_copy = avail.min(buf.len() - filled);
                let src = &self.read_buf[self.read_pos..self.read_pos + to_copy];
                // SAFETY: MaybeUninit<u8> has the same layout as u8.
                // We are writing to the buffer, never reading uninit data.
                let dst = unsafe {
                    std::slice::from_raw_parts_mut(
                        buf[filled..filled + to_copy].as_mut_ptr().cast::<u8>(),
                        to_copy,
                    )
                };
                dst.copy_from_slice(src);
                self.read_pos += to_copy;
                filled += to_copy;
            } else {
                self.fill_buf().await?;
            }
        }
        Ok(())
    }

    // --- Raw (unbuffered) I/O ---

    async fn read_raw(&mut self, buf: Vec<u8>) -> BufResult<usize, Vec<u8>> {
        match &mut self.inner {
            StreamInner::Tcp(r) => r.read(buf).await,
            #[cfg(feature = "compio-tls")]
            StreamInner::Tls(r) => r.read(buf).await,
            #[cfg(unix)]
            StreamInner::Unix(r) => r.read(buf).await,
        }
    }

    // --- Write API (pass-through, no buffering needed) ---

    pub async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let owned = buf.to_vec();
        let BufResult(result, _) = match &mut self.inner {
            StreamInner::Tcp(r) => r.write_all(owned).await,
            #[cfg(feature = "compio-tls")]
            StreamInner::Tls(r) => r.write_all(owned).await,
            #[cfg(unix)]
            StreamInner::Unix(r) => r.write_all(owned).await,
        };
        result
    }

    pub async fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.inner {
            StreamInner::Tcp(r) => r.flush().await,
            #[cfg(feature = "compio-tls")]
            StreamInner::Tls(r) => r.flush().await,
            #[cfg(unix)]
            StreamInner::Unix(r) => r.flush().await,
        }
    }

    // --- Misc ---

    pub fn is_tcp_loopback(&self) -> bool {
        match &self.inner {
            StreamInner::Tcp(r) => r
                .peer_addr()
                .map(|addr| addr.ip().is_loopback())
                .unwrap_or(false),
            #[cfg(feature = "compio-tls")]
            StreamInner::Tls(_) => false,
            #[cfg(unix)]
            StreamInner::Unix(_) => false,
        }
    }
}
