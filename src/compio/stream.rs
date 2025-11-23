use compio::buf::BufResult;
use compio::io::{AsyncReadExt, AsyncWriteExt};
use compio::net::TcpStream;

pub enum Stream {
    Tcp(TcpStream),
    // Tls not yet supported for compio
    // Unix(UnixStream),
}

impl Stream {
    pub fn tcp(stream: TcpStream) -> Self {
        Self::Tcp(stream)
    }

    pub async fn read_exact<B: compio::buf::IoBufMut>(&mut self, buf: B) -> BufResult<(), B> {
        match self {
            Self::Tcp(s) => s.read_exact(buf).await,
        }
    }

    pub async fn write_all<B: compio::buf::IoBuf>(&mut self, buf: B) -> BufResult<(), B> {
        match self {
            Self::Tcp(s) => s.write_all(buf).await,
        }
    }
}
