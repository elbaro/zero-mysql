use std::io::IoSlice;
use std::time::Instant;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWrite, AsyncWriteExt};

const PAYLOAD_SIZE: usize = 1024; // 1KB payload
const NUM_ITERATIONS: usize = 100_000;

/// Current approach: vectored I/O with IoSlice (no copying)
async fn write_payload_vectored<W: AsyncWrite + Unpin>(
    writer: &mut W,
    payload: &[u8],
    headers_buffer: &mut Vec<[u8; 4]>,
    ioslice_buffer: &mut Vec<IoSlice<'static>>,
) -> std::io::Result<()> {
    headers_buffer.clear();
    ioslice_buffer.clear();

    let mut remaining = payload;
    let mut chunk_size = 0;
    let mut sequence_id = 0u8;

    // Build all headers
    while !remaining.is_empty() {
        chunk_size = remaining.len().min(0xFFFFFF);
        let (_chunk, rest) = remaining.split_at(chunk_size);
        remaining = rest;

        // Write header
        let header = write_packet_header_array(sequence_id, chunk_size);
        headers_buffer.push(header);

        sequence_id = sequence_id.wrapping_add(1);
    }

    // If the last chunk was exactly 0xFFFFFF bytes, add an empty packet
    if chunk_size == 0xFFFFFF {
        let header = write_packet_header_array(sequence_id, 0);
        headers_buffer.push(header);
    }

    // Build IoSlice array with all headers and chunks
    remaining = payload;
    for header in headers_buffer.iter() {
        let chunk_size = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;

        ioslice_buffer.push(unsafe {
            std::mem::transmute::<IoSlice<'_>, IoSlice<'static>>(IoSlice::new(header))
        });

        if chunk_size > 0 {
            let chunk;
            (chunk, remaining) = remaining.split_at(chunk_size);
            ioslice_buffer.push(unsafe {
                std::mem::transmute::<IoSlice<'_>, IoSlice<'static>>(IoSlice::new(chunk))
            });
        }
    }

    // Write all chunks at once using vectored I/O
    write_all_vectored_async(writer, ioslice_buffer).await?;

    Ok(())
}

/// New approach: copy everything into Vec<u8> buffer, then write_all()
async fn write_payload_buffered<W: AsyncWrite + Unpin>(
    writer: &mut W,
    payload: &[u8],
    copy_buffer: &mut Vec<u8>,
) -> std::io::Result<()> {
    copy_buffer.clear();

    let mut remaining = payload;
    let mut chunk_size = 0;
    let mut sequence_id = 0u8;

    // Copy headers and payload chunks into the buffer
    while !remaining.is_empty() {
        chunk_size = remaining.len().min(0xFFFFFF);
        let (chunk, rest) = remaining.split_at(chunk_size);
        remaining = rest;

        // Write header directly into buffer
        let header = write_packet_header_array(sequence_id, chunk_size);
        copy_buffer.extend_from_slice(&header);

        // Copy chunk into buffer
        copy_buffer.extend_from_slice(chunk);

        sequence_id = sequence_id.wrapping_add(1);
    }

    // If the last chunk was exactly 0xFFFFFF bytes, add an empty packet
    if chunk_size == 0xFFFFFF {
        let header = write_packet_header_array(sequence_id, 0);
        copy_buffer.extend_from_slice(&header);
    }

    // Write entire buffer at once
    writer.write_all(copy_buffer).await?;

    Ok(())
}

fn write_packet_header_array(sequence_id: u8, length: usize) -> [u8; 4] {
    let len_bytes = (length as u32).to_le_bytes();
    [len_bytes[0], len_bytes[1], len_bytes[2], sequence_id]
}

async fn write_all_vectored_async<W: AsyncWrite + Unpin>(
    writer: &mut W,
    bufs: &mut [IoSlice<'_>],
) -> std::io::Result<()> {
    let mut bufs_idx = 0;

    while bufs_idx < bufs.len() {
        match writer.write_vectored(&bufs[bufs_idx..]).await {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            Ok(mut n) => {
                // Advance through buffers based on bytes written
                while n > 0 && bufs_idx < bufs.len() {
                    let buf_len = bufs[bufs_idx].len();
                    if n >= buf_len {
                        // Fully consumed this buffer
                        n -= buf_len;
                        bufs_idx += 1;
                    } else {
                        // Partially consumed this buffer - advance it
                        bufs[bufs_idx].advance(n);
                        n = 0;
                    }
                }
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    println!("Write Payload Benchmark");
    println!("=======================");
    if PAYLOAD_SIZE >= 1024 * 1024 {
        println!("Payload size: {} MB", PAYLOAD_SIZE / 1024 / 1024);
    } else if PAYLOAD_SIZE >= 1024 {
        println!("Payload size: {} KB", PAYLOAD_SIZE / 1024);
    } else {
        println!("Payload size: {} bytes", PAYLOAD_SIZE);
    }
    println!("Iterations: {}", NUM_ITERATIONS);
    println!("Writing to: /dev/null\n");

    // Create a large payload
    let payload = vec![0xABu8; PAYLOAD_SIZE];

    // Reusable buffers
    let mut headers_buffer = Vec::new();
    let mut ioslice_buffer = Vec::new();
    let mut copy_buffer = Vec::new();

    // Benchmark 1: Vectored I/O (current approach)
    println!("Benchmarking vectored I/O approach...");
    let mut total_vectored = std::time::Duration::ZERO;

    for _ in 0..NUM_ITERATIONS {
        let mut file = OpenOptions::new().write(true).open("/dev/null").await?;

        let start = Instant::now();
        write_payload_vectored(
            &mut file,
            &payload,
            &mut headers_buffer,
            &mut ioslice_buffer,
        )
        .await?;
        file.flush().await?;
        total_vectored += start.elapsed();
    }

    let avg_vectored = total_vectored / NUM_ITERATIONS as u32;
    println!("  Total time: {:?}", total_vectored);
    println!("  Average per iteration: {:?}", avg_vectored);
    if PAYLOAD_SIZE >= 1024 * 1024 {
        println!(
            "  Throughput: {:.2} MB/s\n",
            (PAYLOAD_SIZE as f64 / 1024.0 / 1024.0) / avg_vectored.as_secs_f64()
        );
    } else {
        println!(
            "  Throughput: {:.2} KB/s\n",
            (PAYLOAD_SIZE as f64 / 1024.0) / avg_vectored.as_secs_f64()
        );
    }

    // Benchmark 2: Buffered approach (copy to Vec<u8>)
    println!("Benchmarking buffered (copy to Vec<u8>) approach...");
    let mut total_buffered = std::time::Duration::ZERO;

    for _ in 0..NUM_ITERATIONS {
        let mut file = OpenOptions::new().write(true).open("/dev/null").await?;

        let start = Instant::now();
        write_payload_buffered(&mut file, &payload, &mut copy_buffer).await?;
        file.flush().await?;
        total_buffered += start.elapsed();
    }

    let avg_buffered = total_buffered / NUM_ITERATIONS as u32;
    println!("  Total time: {:?}", total_buffered);
    println!("  Average per iteration: {:?}", avg_buffered);
    if PAYLOAD_SIZE >= 1024 * 1024 {
        println!(
            "  Throughput: {:.2} MB/s\n",
            (PAYLOAD_SIZE as f64 / 1024.0 / 1024.0) / avg_buffered.as_secs_f64()
        );
    } else {
        println!(
            "  Throughput: {:.2} KB/s\n",
            (PAYLOAD_SIZE as f64 / 1024.0) / avg_buffered.as_secs_f64()
        );
    }

    // Comparison
    println!("Comparison:");
    println!("===========");
    if avg_vectored < avg_buffered {
        let speedup = avg_buffered.as_secs_f64() / avg_vectored.as_secs_f64();
        println!(
            "Vectored I/O is {:.2}x FASTER than buffered approach",
            speedup
        );
    } else {
        let speedup = avg_vectored.as_secs_f64() / avg_buffered.as_secs_f64();
        println!(
            "Buffered approach is {:.2}x FASTER than vectored I/O",
            speedup
        );
    }

    Ok(())
}
