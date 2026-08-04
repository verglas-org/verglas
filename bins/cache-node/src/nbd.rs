//! The NBD server: serves attached [`verglas_block::BlockDevice`]s to the Linux
//! kernel NBD client over a single TCP listener.
//!
//! This implements enough of the NBD "fixed newstyle" protocol for the kernel
//! client to attach a device and do IO: the handshake with option haggling
//! (`NBD_OPT_GO`/`NBD_OPT_INFO`/`NBD_OPT_EXPORT_NAME`/`NBD_OPT_ABORT`), and the
//! transmission commands `READ`, `WRITE`, `FLUSH`, `TRIM`, and `DISC`. The export
//! name selects the device: it is the device id the host agent ensured before
//! connecting. A `FLUSH` command and a clean `DISC` both force the device's
//! durability barrier and manifest commit, so a paused or torn-down VM's writes
//! are durable in the backend before teardown proceeds.
//!
//! Extension point (not built): `vhost-user-blk` is the lower-overhead attach
//! surface that would replace NBD for the same [`DeviceRegistry`]; it is noted
//! here and nowhere wired.

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::blockdev::DeviceRegistry;

// Handshake magics and flags (NBD fixed newstyle).
/// `NBDMAGIC`, the first 8 bytes of the server greeting.
const NBD_MAGIC: u64 = 0x4e42444d41474943;
/// `IHAVEOPT`, the option magic in both the greeting and each client option.
const NBD_OPT_MAGIC: u64 = 0x49484156454f5054;
/// The reply magic for structured option replies.
const NBD_REP_MAGIC: u64 = 0x0003e889045565a9;
/// The request magic prefixing every transmission-phase request.
const NBD_REQUEST_MAGIC: u32 = 0x25609513;
/// The reply magic prefixing every simple transmission-phase reply.
const NBD_SIMPLE_REPLY_MAGIC: u32 = 0x67446698;

/// Handshake flag: the server speaks fixed newstyle.
const NBD_FLAG_FIXED_NEWSTYLE: u16 = 1 << 0;
/// Handshake flag: the server omits the 124-byte zero pad after an
/// `NBD_OPT_EXPORT_NAME` reply.
const NBD_FLAG_NO_ZEROES: u16 = 1 << 1;

/// Transmission flag: the export advertises flags (required when any other
/// transmission flag is set).
const NBD_FLAG_HAS_FLAGS: u16 = 1 << 0;
/// Transmission flag: the export honors `NBD_CMD_FLUSH`.
const NBD_FLAG_SEND_FLUSH: u16 = 1 << 2;
/// Transmission flag: the export honors `NBD_CMD_TRIM`.
const NBD_FLAG_SEND_TRIM: u16 = 1 << 5;

// Option types the client may send during haggling.
/// Attach by export name (the legacy path).
const NBD_OPT_EXPORT_NAME: u32 = 1;
/// Abort the handshake.
const NBD_OPT_ABORT: u32 = 2;
/// Request export info without attaching.
const NBD_OPT_INFO: u32 = 6;
/// Request export info and, on success, begin transmission.
const NBD_OPT_GO: u32 = 7;

// Option reply types.
/// The option succeeded (final reply).
const NBD_REP_ACK: u32 = 1;
/// An export-info reply carrying one `NBD_INFO_*` record.
const NBD_REP_INFO: u32 = 3;
/// The option is unsupported.
const NBD_REP_ERR_UNSUP: u32 = 0x8000_0001;
/// The named export is unknown.
const NBD_REP_ERR_UNKNOWN: u32 = 0x8000_0006;

/// The `NBD_INFO_EXPORT` info record type (size + transmission flags).
const NBD_INFO_EXPORT: u16 = 0;

// Transmission command types.
/// Read `length` bytes at `offset`.
const NBD_CMD_READ: u16 = 0;
/// Write `length` bytes at `offset`.
const NBD_CMD_WRITE: u16 = 1;
/// Disconnect (clean shutdown).
const NBD_CMD_DISC: u16 = 2;
/// Flush all writes to durable storage.
const NBD_CMD_FLUSH: u16 = 3;
/// Discard (trim) `length` bytes at `offset`.
const NBD_CMD_TRIM: u16 = 4;

// NBD wire error codes.
/// Generic IO error.
const NBD_EIO: u32 = 5;
/// Invalid argument (bad offset/length).
const NBD_EINVAL: u32 = 22;

/// The transmission flags every export advertises: flush and trim are honored.
const EXPORT_FLAGS: u16 = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_TRIM;

/// Serves NBD connections on `listener` until the process ends. Each connection
/// is handled on its own task; a handler error tears down only that connection.
pub async fn serve(listener: TcpListener, registry: DeviceRegistry) -> std::io::Result<()> {
    loop {
        let (socket, peer) = listener.accept().await?;
        let registry = registry.clone();
        tokio::spawn(async move {
            // TCP_NODELAY: NBD replies are small and latency-sensitive.
            let _ = socket.set_nodelay(true);
            if let Err(error) = handle_connection(socket, registry).await {
                tracing::warn!(%peer, %error, "nbd connection ended with error");
            }
        });
    }
}

/// Drives one NBD connection: handshake, then transmission against the resolved
/// device. Generic over the stream so tests drive it over an in-memory pipe.
pub async fn handle_connection<S>(mut stream: S, registry: DeviceRegistry) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Some(device) = handshake(&mut stream, &registry).await? else {
        return Ok(());
    };
    transmission(&mut stream, &device).await
}

/// Runs the fixed-newstyle handshake and returns the resolved device to serve,
/// or `None` if the client aborted or asked only for info. Errors on a malformed
/// handshake.
async fn handshake<S>(
    stream: &mut S,
    registry: &DeviceRegistry,
) -> std::io::Result<Option<verglas_block::BlockDevice>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Server greeting: magic, option magic, handshake flags.
    stream.write_u64(NBD_MAGIC).await?;
    stream.write_u64(NBD_OPT_MAGIC).await?;
    stream
        .write_u16(NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES)
        .await?;
    stream.flush().await?;

    // Client flags (we do not vary behavior on them beyond the handshake).
    let _client_flags = stream.read_u32().await?;

    loop {
        let magic = stream.read_u64().await?;
        if magic != NBD_OPT_MAGIC {
            return Err(protocol_error("bad option magic"));
        }
        let option = stream.read_u32().await?;
        let len = stream.read_u32().await? as usize;
        let mut data = vec![0u8; len];
        stream.read_exact(&mut data).await?;

        match option {
            NBD_OPT_EXPORT_NAME => {
                let name = String::from_utf8_lossy(&data).into_owned();
                let Some(device) = resolve(registry, &name).await? else {
                    // EXPORT_NAME has no error reply channel — drop the
                    // connection so the client fails loudly.
                    return Ok(None);
                };
                // Reply: size, transmission flags, no zero pad (NO_ZEROES).
                stream.write_u64(device.size_bytes()).await?;
                stream.write_u16(EXPORT_FLAGS).await?;
                stream.flush().await?;
                return Ok(Some(device));
            }
            NBD_OPT_GO | NBD_OPT_INFO => {
                let Some(name) = parse_info_name(&data) else {
                    return Err(protocol_error("malformed GO/INFO option"));
                };
                match resolve(registry, &name).await? {
                    Some(device) => {
                        write_export_info(stream, &device).await?;
                        write_option_reply(stream, option, NBD_REP_ACK, &[]).await?;
                        stream.flush().await?;
                        if option == NBD_OPT_GO {
                            return Ok(Some(device));
                        }
                        // INFO: stay in haggling for the next option.
                    }
                    None => {
                        write_option_reply(stream, option, NBD_REP_ERR_UNKNOWN, &[]).await?;
                        stream.flush().await?;
                    }
                }
            }
            NBD_OPT_ABORT => {
                write_option_reply(stream, option, NBD_REP_ACK, &[]).await?;
                stream.flush().await?;
                return Ok(None);
            }
            _ => {
                write_option_reply(stream, option, NBD_REP_ERR_UNSUP, &[]).await?;
                stream.flush().await?;
            }
        }
    }
}

/// Resolves an export name to a device via the registry, mapping a device-layer
/// error to a protocol IO error.
async fn resolve(
    registry: &DeviceRegistry,
    name: &str,
) -> std::io::Result<Option<verglas_block::BlockDevice>> {
    registry
        .get(name)
        .await
        .map_err(|e| std::io::Error::other(format!("device resolve failed: {e}")))
}

/// Parses the export name out of an `NBD_OPT_GO`/`NBD_OPT_INFO` payload:
/// a 4-byte name length, the name, then the info-request list (ignored).
fn parse_info_name(data: &[u8]) -> Option<String> {
    if data.len() < 4 {
        return None;
    }
    let name_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let name = data.get(4..4 + name_len)?;
    Some(String::from_utf8_lossy(name).into_owned())
}

/// Writes an `NBD_REP_INFO` reply carrying one `NBD_INFO_EXPORT` record (the
/// device size and transmission flags).
async fn write_export_info<S>(
    stream: &mut S,
    device: &verglas_block::BlockDevice,
) -> std::io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut payload = Vec::with_capacity(12);
    payload.extend_from_slice(&NBD_INFO_EXPORT.to_be_bytes());
    payload.extend_from_slice(&device.size_bytes().to_be_bytes());
    payload.extend_from_slice(&EXPORT_FLAGS.to_be_bytes());
    write_option_reply(stream, NBD_OPT_GO, NBD_REP_INFO, &payload).await
}

/// Writes one structured option reply: reply magic, echoed option, reply type,
/// payload length, then payload.
async fn write_option_reply<S>(
    stream: &mut S,
    option: u32,
    reply_type: u32,
    payload: &[u8],
) -> std::io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut header = Vec::with_capacity(20);
    header.extend_from_slice(&NBD_REP_MAGIC.to_be_bytes());
    header.extend_from_slice(&option.to_be_bytes());
    header.extend_from_slice(&reply_type.to_be_bytes());
    header.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    stream.write_all(&header).await?;
    stream.write_all(payload).await
}

/// The transmission loop: reads requests and serves them against `device` until
/// a clean disconnect or a transport error. A clean `DISC` flushes first, so the
/// device's durability barrier runs before the connection closes.
async fn transmission<S>(stream: &mut S, device: &verglas_block::BlockDevice) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let magic = match stream.read_u32().await {
            Ok(magic) => magic,
            // A client that closes without DISC still leaves durable state: its
            // last FLUSH committed. EOF here is a normal end of connection.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        };
        if magic != NBD_REQUEST_MAGIC {
            return Err(protocol_error("bad request magic"));
        }
        let _flags = stream.read_u16().await?;
        let cmd = stream.read_u16().await?;
        let handle = stream.read_u64().await?;
        let offset = stream.read_u64().await?;
        let length = stream.read_u32().await?;

        match cmd {
            NBD_CMD_READ => match device.read_at(offset, length as u64).await {
                Ok(bytes) if bytes.len() == length as usize => {
                    write_reply_header(stream, 0, handle).await?;
                    stream.write_all(&bytes).await?;
                }
                Ok(_) => write_reply_header(stream, NBD_EINVAL, handle).await?,
                Err(_) => write_reply_header(stream, NBD_EIO, handle).await?,
            },
            NBD_CMD_WRITE => {
                let mut buf = vec![0u8; length as usize];
                stream.read_exact(&mut buf).await?;
                match device.write_at(offset, &buf).await {
                    Ok(()) => write_reply_header(stream, 0, handle).await?,
                    Err(_) => write_reply_header(stream, NBD_EIO, handle).await?,
                }
            }
            NBD_CMD_FLUSH => match device.flush().await {
                Ok(_) => write_reply_header(stream, 0, handle).await?,
                Err(_) => write_reply_header(stream, NBD_EIO, handle).await?,
            },
            NBD_CMD_TRIM => match device.trim(offset, length as u64).await {
                Ok(()) => write_reply_header(stream, 0, handle).await?,
                Err(_) => write_reply_header(stream, NBD_EIO, handle).await?,
            },
            NBD_CMD_DISC => {
                // Clean disconnect: force the durability barrier, then close with
                // no reply (the client expects none for DISC).
                let _ = device.flush().await;
                return Ok(());
            }
            _ => write_reply_header(stream, NBD_EINVAL, handle).await?,
        }
        stream.flush().await?;
    }
}

/// Writes a simple-reply header (magic, error, handle).
async fn write_reply_header<S>(stream: &mut S, error: u32, handle: u64) -> std::io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut header = Vec::with_capacity(16);
    header.extend_from_slice(&NBD_SIMPLE_REPLY_MAGIC.to_be_bytes());
    header.extend_from_slice(&error.to_be_bytes());
    header.extend_from_slice(&handle.to_be_bytes());
    stream.write_all(&header).await
}

/// Builds a protocol IO error with a descriptive message.
fn protocol_error(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, format!("nbd: {message}"))
}

#[cfg(test)]
mod tests {
    //! A protocol-level NBD client drives the server over an in-memory duplex
    //! pipe: handshake (`NBD_OPT_GO`), write, read-back, flush, disconnect, then
    //! reconnect and read the flushed bytes back — the acceptance path without a
    //! kernel nbd-client or root.

    use std::sync::Arc;

    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    use verglas_block::{ObjectBackend, ObjectStoreBackend};

    use super::*;
    use crate::blockdev::DeviceRegistry;

    /// A registry over an in-memory object backend, with `device_id` ensured at
    /// `size`. The temp dir is returned so it outlives the test.
    async fn ensured_registry(device_id: &str, size: u64) -> (DeviceRegistry, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let backend: Arc<dyn ObjectBackend> = Arc::new(ObjectStoreBackend::new(object_store));
        let registry = DeviceRegistry::open(dir.path(), backend)
            .await
            .expect("open");
        registry.ensure(device_id, size).await.expect("ensure");
        (registry, dir)
    }

    /// Runs the client handshake via `NBD_OPT_GO` for `export` and returns the
    /// advertised export size.
    async fn client_go<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S, export: &str) -> u64 {
        assert_eq!(stream.read_u64().await.expect("magic"), NBD_MAGIC);
        assert_eq!(stream.read_u64().await.expect("opt magic"), NBD_OPT_MAGIC);
        let _flags = stream.read_u16().await.expect("handshake flags");
        stream.write_u32(1).await.expect("client flags"); // FIXED_NEWSTYLE

        let mut data = Vec::new();
        data.extend_from_slice(&(export.len() as u32).to_be_bytes());
        data.extend_from_slice(export.as_bytes());
        data.extend_from_slice(&0u16.to_be_bytes()); // zero info requests
        stream.write_u64(NBD_OPT_MAGIC).await.expect("opt magic");
        stream.write_u32(NBD_OPT_GO).await.expect("opt go");
        stream.write_u32(data.len() as u32).await.expect("opt len");
        stream.write_all(&data).await.expect("opt data");
        stream.flush().await.expect("flush");

        let mut size = 0u64;
        loop {
            assert_eq!(stream.read_u64().await.expect("rep magic"), NBD_REP_MAGIC);
            let _option = stream.read_u32().await.expect("option");
            let reply_type = stream.read_u32().await.expect("reply type");
            let len = stream.read_u32().await.expect("len") as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).await.expect("payload");
            if reply_type == NBD_REP_INFO {
                // NBD_INFO_EXPORT: u16 type, u64 size, u16 flags.
                size = u64::from_be_bytes(payload[2..10].try_into().expect("size"));
            } else if reply_type == NBD_REP_ACK {
                break;
            }
        }
        size
    }

    /// Sends one transmission request header (and, for WRITE, its data).
    async fn send_request<S: AsyncWrite + Unpin>(
        stream: &mut S,
        cmd: u16,
        offset: u64,
        length: u32,
        data: &[u8],
    ) {
        stream
            .write_u32(NBD_REQUEST_MAGIC)
            .await
            .expect("req magic");
        stream.write_u16(0).await.expect("flags");
        stream.write_u16(cmd).await.expect("cmd");
        stream.write_u64(1).await.expect("handle");
        stream.write_u64(offset).await.expect("offset");
        stream.write_u32(length).await.expect("length");
        if !data.is_empty() {
            stream.write_all(data).await.expect("data");
        }
        stream.flush().await.expect("flush");
    }

    /// Reads a simple reply header and returns its error code.
    async fn read_reply_error<S: AsyncRead + Unpin>(stream: &mut S) -> u32 {
        assert_eq!(
            stream.read_u32().await.expect("reply magic"),
            NBD_SIMPLE_REPLY_MAGIC
        );
        let error = stream.read_u32().await.expect("error");
        let _handle = stream.read_u64().await.expect("handle");
        error
    }

    #[tokio::test]
    async fn write_read_flush_and_reconnect_reads_back() {
        let (registry, _dir) = ensured_registry("vd0", 4096).await;

        // First connection: handshake, write, read back, flush, disconnect.
        {
            let (mut client, server) = tokio::io::duplex(64 * 1024);
            let reg = registry.clone();
            let handler = tokio::spawn(async move { handle_connection(server, reg).await });

            assert_eq!(client_go(&mut client, "vd0").await, 4096);

            send_request(&mut client, NBD_CMD_WRITE, 512, 9, b"hello nbd").await;
            assert_eq!(read_reply_error(&mut client).await, 0);

            send_request(&mut client, NBD_CMD_READ, 512, 9, &[]).await;
            assert_eq!(read_reply_error(&mut client).await, 0);
            let mut got = [0u8; 9];
            client.read_exact(&mut got).await.expect("read data");
            assert_eq!(&got, b"hello nbd");

            // A read of an unwritten range returns zeros.
            send_request(&mut client, NBD_CMD_READ, 0, 4, &[]).await;
            assert_eq!(read_reply_error(&mut client).await, 0);
            let mut zeros = [0xFFu8; 4];
            client.read_exact(&mut zeros).await.expect("zeros");
            assert_eq!(zeros, [0u8; 4]);

            send_request(&mut client, NBD_CMD_FLUSH, 0, 0, &[]).await;
            assert_eq!(read_reply_error(&mut client).await, 0);

            send_request(&mut client, NBD_CMD_DISC, 0, 0, &[]).await;
            handler.await.expect("join").expect("handler ok");
        }

        // The flush committed a new durable version.
        let device = registry.get("vd0").await.expect("get").expect("present");
        assert_eq!(device.version().await, 1, "flush committed a version");

        // Reconnect: a fresh handshake reads the flushed bytes back.
        {
            let (mut client, server) = tokio::io::duplex(64 * 1024);
            let reg = registry.clone();
            let handler = tokio::spawn(async move { handle_connection(server, reg).await });
            assert_eq!(client_go(&mut client, "vd0").await, 4096);
            send_request(&mut client, NBD_CMD_READ, 512, 9, &[]).await;
            assert_eq!(read_reply_error(&mut client).await, 0);
            let mut got = [0u8; 9];
            client.read_exact(&mut got).await.expect("reread");
            assert_eq!(&got, b"hello nbd", "reconnect reads back flushed bytes");
            send_request(&mut client, NBD_CMD_DISC, 0, 0, &[]).await;
            handler.await.expect("join").expect("handler ok");
        }
    }

    #[tokio::test]
    async fn unknown_export_over_go_is_rejected() {
        let (registry, _dir) = ensured_registry("vd0", 4096).await;
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let handler = tokio::spawn(async move { handle_connection(server, registry).await });

        // Handshake for a device that was never ensured: expect an error reply,
        // never a size, and the connection closes cleanly.
        assert_eq!(client.read_u64().await.expect("magic"), NBD_MAGIC);
        assert_eq!(client.read_u64().await.expect("opt magic"), NBD_OPT_MAGIC);
        let _flags = client.read_u16().await.expect("flags");
        client.write_u32(1).await.expect("client flags");
        let export = "missing";
        let mut data = Vec::new();
        data.extend_from_slice(&(export.len() as u32).to_be_bytes());
        data.extend_from_slice(export.as_bytes());
        data.extend_from_slice(&0u16.to_be_bytes());
        client.write_u64(NBD_OPT_MAGIC).await.expect("m");
        client.write_u32(NBD_OPT_GO).await.expect("go");
        client.write_u32(data.len() as u32).await.expect("len");
        client.write_all(&data).await.expect("data");
        client.flush().await.expect("flush");

        assert_eq!(client.read_u64().await.expect("rep magic"), NBD_REP_MAGIC);
        let _option = client.read_u32().await.expect("option");
        let reply_type = client.read_u32().await.expect("type");
        let payload_len = client.read_u32().await.expect("len") as usize;
        let mut payload = vec![0u8; payload_len];
        client.read_exact(&mut payload).await.expect("payload");
        assert_eq!(
            reply_type, NBD_REP_ERR_UNKNOWN,
            "unknown export is rejected"
        );

        // The server stays in option haggling after an error; abort cleanly so
        // the handshake loop returns and the handler task completes.
        client.write_u64(NBD_OPT_MAGIC).await.expect("m");
        client.write_u32(NBD_OPT_ABORT).await.expect("abort");
        client.write_u32(0).await.expect("len");
        client.flush().await.expect("flush");
        handler.await.expect("join").expect("handler ok");
    }
}
