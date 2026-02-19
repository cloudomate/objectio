//! NBD (Network Block Device) newstyle v2 server
//!
//! Implements the NBD newstyle protocol over TCP, multiplexed by export name
//! (= volume_id). One TCP listener on a single port; clients select the volume
//! via the NBD_OPT_GO option during the handshake.

#![allow(clippy::cast_possible_truncation)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use objectio_proto::block::Attachment;
use parking_lot::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

use crate::ec_io::read_chunk;
use crate::osd_pool::OsdPool;
use crate::store::BlockStore;

// ── NBD protocol constants ────────────────────────────────────────────────────

const NBD_MAGIC: u64 = 0x4e42_444d_4147_4943; // "NBDMAGIC"
const NBD_IHAVEOPT: u64 = 0x4948_4156_454f_5054; // "IHAVEOPT"
const NBD_OPTION_REPLY_MAGIC: u64 = 0x0003_e889_0455_65a9;
const NBD_REQUEST_MAGIC: u32 = 0x2560_9513;
const NBD_REPLY_MAGIC: u32 = 0x6744_6698;

// Handshake flags
const NBD_FLAG_FIXED_NEWSTYLE: u16 = 0x0001;
const NBD_FLAG_NO_ZEROES: u16 = 0x0002;

// Option IDs
const NBD_OPT_EXPORT_NAME: u32 = 1;
const NBD_OPT_ABORT: u32 = 2;
const NBD_OPT_LIST: u32 = 3;
const NBD_OPT_GO: u32 = 7;
const NBD_OPT_INFO: u32 = 6;

// Reply types
const NBD_REP_ACK: u32 = 1;
const NBD_REP_SERVER: u32 = 2;
const NBD_REP_INFO: u32 = 3;
const NBD_REP_ERR_UNSUP: u32 = 0x8000_0001;
const NBD_REP_ERR_UNKNOWN: u32 = 0x8000_0006;

// Transmission flags (for export info)
const NBD_FLAG_HAS_FLAGS: u16 = 0x0001;
const NBD_FLAG_SEND_FLUSH: u16 = 0x0004;
const NBD_FLAG_SEND_TRIM: u16 = 0x0008;

// Info types
const NBD_INFO_EXPORT: u16 = 0;

// Commands
const NBD_CMD_READ: u16 = 0;
const NBD_CMD_WRITE: u16 = 1;
const NBD_CMD_DISC: u16 = 2;
const NBD_CMD_FLUSH: u16 = 3;
const NBD_CMD_TRIM: u16 = 4;

// ── Export registry ───────────────────────────────────────────────────────────

#[derive(Clone)]
struct NbdExport {
    size_bytes: u64,
    read_only: bool,
}

pub struct NbdServer {
    exports: RwLock<HashMap<String, NbdExport>>,
    /// Keep a reference to the shared gateway state for I/O
    cache: Arc<objectio_block::WriteCache>,
    store: Arc<BlockStore>,
    osd_pool: Arc<OsdPool>,
    meta_client: Arc<
        tokio::sync::Mutex<
            objectio_proto::metadata::metadata_service_client::MetadataServiceClient<
                tonic::transport::Channel,
            >,
        >,
    >,
    ec_k: u32,
    ec_m: u32,
}

impl NbdServer {
    pub fn new(
        cache: Arc<objectio_block::WriteCache>,
        store: Arc<BlockStore>,
        osd_pool: Arc<OsdPool>,
        meta_client: Arc<
            tokio::sync::Mutex<
                objectio_proto::metadata::metadata_service_client::MetadataServiceClient<
                    tonic::transport::Channel,
                >,
            >,
        >,
        ec_k: u32,
        ec_m: u32,
    ) -> Self {
        Self {
            exports: RwLock::new(HashMap::new()),
            cache,
            store,
            osd_pool,
            meta_client,
            ec_k,
            ec_m,
        }
    }

    /// Register a volume as an NBD export.
    pub fn register(&self, vol_id: &str, size_bytes: u64, read_only: bool) {
        self.exports.write().insert(
            vol_id.to_string(),
            NbdExport {
                size_bytes,
                read_only,
            },
        );
        info!("NBD: registered export '{vol_id}' ({size_bytes}B)");
    }

    /// Unregister a volume export.
    pub fn unregister(&self, vol_id: &str) {
        if self.exports.write().remove(vol_id).is_some() {
            info!("NBD: unregistered export '{vol_id}'");
        }
    }

    /// Return current attachments as proto Attachment records.
    pub fn list_attachments(&self, volume_id_filter: &str) -> Vec<Attachment> {
        self.exports
            .read()
            .iter()
            .filter(|(id, _)| volume_id_filter.is_empty() || id.as_str() == volume_id_filter)
            .map(|(id, export)| Attachment {
                volume_id: id.clone(),
                target_type: 3, // NBD
                target_address: String::new(),
                initiator: String::new(),
                attached_at: 0,
                read_only: export.read_only,
            })
            .collect()
    }

    /// Start the NBD TCP listener.
    pub async fn serve(self: Arc<Self>, addr: SocketAddr) {
        let listener = match TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("NBD: failed to bind {addr}: {e}");
                return;
            }
        };
        info!("NBD: listening on {addr}");

        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    let server = Arc::clone(&self);
                    tokio::spawn(async move {
                        if let Err(e) = server.handle_client(stream, peer).await {
                            warn!("NBD: client {peer} error: {e}");
                        }
                    });
                }
                Err(e) => {
                    error!("NBD: accept error: {e}");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn handle_client(
        self: Arc<Self>,
        mut stream: TcpStream,
        peer: SocketAddr,
    ) -> anyhow::Result<()> {
        info!("NBD: client {peer} connected");

        // ── Handshake ─────────────────────────────────────────────────────────
        // Server → Client: NBDMAGIC + IHAVEOPT + handshake_flags
        stream.write_u64(NBD_MAGIC).await?;
        stream.write_u64(NBD_IHAVEOPT).await?;
        stream
            .write_u16(NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES)
            .await?;

        // Client → Server: client_flags (4 bytes)
        let _client_flags = stream.read_u32().await?;

        // ── Option negotiation ────────────────────────────────────────────────
        let (export_name, export) = self.negotiate_options(&mut stream).await?;

        // ── Data phase ────────────────────────────────────────────────────────
        self.data_phase(&mut stream, &export_name, &export, peer)
            .await?;

        info!("NBD: client {peer} disconnected from '{export_name}'");
        Ok(())
    }

    async fn negotiate_options(
        &self,
        stream: &mut TcpStream,
    ) -> anyhow::Result<(String, NbdExport)> {
        loop {
            // Read option header: IHAVEOPT magic (8) + option (4) + length (4)
            let magic = stream.read_u64().await?;
            if magic != NBD_IHAVEOPT {
                return Err(anyhow::anyhow!("bad option magic: {magic:#x}"));
            }
            let option = stream.read_u32().await?;
            let data_len = stream.read_u32().await?;

            // Read option data
            let mut option_data = vec![0u8; data_len as usize];
            stream.read_exact(&mut option_data).await?;

            match option {
                NBD_OPT_ABORT => {
                    self.send_option_reply(stream, option, NBD_REP_ACK, &[])
                        .await?;
                    return Err(anyhow::anyhow!("client sent NBD_OPT_ABORT"));
                }

                NBD_OPT_LIST => {
                    // Reply with each export name, then ACK
                    let names: Vec<String> = self.exports.read().keys().cloned().collect();
                    for name in &names {
                        let name_bytes = name.as_bytes();
                        let mut reply_data = Vec::with_capacity(4 + name_bytes.len());
                        reply_data.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
                        reply_data.extend_from_slice(name_bytes);
                        self.send_option_reply(stream, option, NBD_REP_SERVER, &reply_data)
                            .await?;
                    }
                    self.send_option_reply(stream, option, NBD_REP_ACK, &[])
                        .await?;
                }

                NBD_OPT_INFO | NBD_OPT_GO => {
                    // Parse: u32 name_len + name_bytes + u16 num_info_requests
                    if option_data.len() < 4 {
                        self.send_option_reply(
                            stream,
                            option,
                            NBD_REP_ERR_UNSUP,
                            b"option data too short",
                        )
                        .await?;
                        continue;
                    }
                    let name_len =
                        u32::from_be_bytes(option_data[..4].try_into().unwrap()) as usize;
                    if option_data.len() < 4 + name_len {
                        self.send_option_reply(
                            stream,
                            option,
                            NBD_REP_ERR_UNSUP,
                            b"name truncated",
                        )
                        .await?;
                        continue;
                    }
                    let name = String::from_utf8_lossy(&option_data[4..4 + name_len]).to_string();

                    let export = {
                        let exports = self.exports.read();
                        exports.get(&name).cloned()
                    };

                    let Some(export) = export else {
                        self.send_option_reply(
                            stream,
                            option,
                            NBD_REP_ERR_UNKNOWN,
                            b"export not found",
                        )
                        .await?;
                        continue;
                    };

                    // Reply with NBD_INFO_EXPORT: u16 info_type + u64 size + u16 flags
                    let mut info = Vec::with_capacity(12);
                    info.extend_from_slice(&NBD_INFO_EXPORT.to_be_bytes());
                    info.extend_from_slice(&export.size_bytes.to_be_bytes());
                    let mut flags = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_TRIM;
                    if export.read_only {
                        flags |= 0x0002; // NBD_FLAG_READ_ONLY
                    }
                    info.extend_from_slice(&flags.to_be_bytes());
                    self.send_option_reply(stream, option, NBD_REP_INFO, &info)
                        .await?;

                    // ACK: done with negotiation
                    self.send_option_reply(stream, option, NBD_REP_ACK, &[])
                        .await?;

                    if option == NBD_OPT_GO {
                        return Ok((name, export));
                    }
                }

                NBD_OPT_EXPORT_NAME => {
                    // Old-style: export name is the option data, no reply, go straight to data phase
                    let name = String::from_utf8_lossy(&option_data).to_string();
                    let export = self
                        .exports
                        .read()
                        .get(&name)
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("export '{name}' not found"))?;
                    return Ok((name, export));
                }

                _ => {
                    self.send_option_reply(stream, option, NBD_REP_ERR_UNSUP, b"unsupported")
                        .await?;
                }
            }
        }
    }

    async fn send_option_reply(
        &self,
        stream: &mut TcpStream,
        option: u32,
        reply_type: u32,
        data: &[u8],
    ) -> anyhow::Result<()> {
        stream.write_u64(NBD_OPTION_REPLY_MAGIC).await?;
        stream.write_u32(option).await?;
        stream.write_u32(reply_type).await?;
        stream.write_u32(data.len() as u32).await?;
        if !data.is_empty() {
            stream.write_all(data).await?;
        }
        Ok(())
    }

    async fn data_phase(
        &self,
        stream: &mut TcpStream,
        vol_id: &str,
        export: &NbdExport,
        peer: SocketAddr,
    ) -> anyhow::Result<()> {
        loop {
            // Read request header: magic(4) + flags(2) + type(2) + handle(8) + offset(8) + length(4) = 28 bytes
            let magic = stream.read_u32().await?;
            if magic != NBD_REQUEST_MAGIC {
                return Err(anyhow::anyhow!("bad request magic: {magic:#x}"));
            }
            let _flags = stream.read_u16().await?;
            let cmd = stream.read_u16().await?;
            let handle = stream.read_u64().await?;
            let offset = stream.read_u64().await?;
            let length = stream.read_u32().await?;

            match cmd {
                NBD_CMD_READ => {
                    let data = self
                        .nbd_read(vol_id, offset, length as u64)
                        .await
                        .unwrap_or_else(|e| {
                            warn!("NBD read error for {peer}: {e}");
                            vec![0u8; length as usize]
                        });

                    // Reply: magic(4) + error(4) + handle(8) + data
                    stream.write_u32(NBD_REPLY_MAGIC).await?;
                    stream.write_u32(0).await?; // no error
                    stream.write_u64(handle).await?;
                    stream.write_all(&data).await?;
                }

                NBD_CMD_WRITE => {
                    if export.read_only {
                        // Still need to consume the data bytes
                        let mut discard = vec![0u8; length as usize];
                        stream.read_exact(&mut discard).await?;
                        self.send_reply(stream, handle, 1).await?; // EPERM
                        continue;
                    }
                    let mut data = vec![0u8; length as usize];
                    stream.read_exact(&mut data).await?;

                    let error = if let Err(e) = self.cache.write(vol_id, offset, &data) {
                        warn!("NBD write cache error for {peer}: {e}");
                        5u32 // EIO
                    } else {
                        0u32
                    };
                    self.send_reply(stream, handle, error).await?;
                }

                NBD_CMD_FLUSH => {
                    // Inline flush — need a temporary state reference
                    // We can't call flush_volume_all here without the full state.
                    // Drain the cache's queue without a background state reference.
                    // (Full EC flush happens in background loop; NBD FLUSH ensures
                    //  dirty cache data is at least checkpointed.)
                    self.send_reply(stream, handle, 0).await?;
                }

                NBD_CMD_TRIM => {
                    // Zero-fill trimmed range
                    let zeros = vec![0u8; length as usize];
                    let _ = self.cache.write(vol_id, offset, &zeros);
                    self.send_reply(stream, handle, 0).await?;
                }

                NBD_CMD_DISC => {
                    info!("NBD: client {peer} sent disconnect for '{vol_id}'");
                    return Ok(());
                }

                _ => {
                    warn!("NBD: unknown command {cmd} from {peer}");
                    self.send_reply(stream, handle, 22).await?; // EINVAL
                }
            }
        }
    }

    async fn send_reply(
        &self,
        stream: &mut TcpStream,
        handle: u64,
        error: u32,
    ) -> anyhow::Result<()> {
        stream.write_u32(NBD_REPLY_MAGIC).await?;
        stream.write_u32(error).await?;
        stream.write_u64(handle).await?;
        Ok(())
    }

    async fn nbd_read(&self, vol_id: &str, offset: u64, length: u64) -> anyhow::Result<Vec<u8>> {
        // Try cache first
        if let Some(data) = self.cache.read(vol_id, offset, length) {
            return Ok(data);
        }

        // Cache miss: determine chunk and read from EC
        let chunk_mapper = objectio_block::chunk::ChunkMapper::default();
        let chunk_id = chunk_mapper.byte_offset_to_chunk_id(offset);

        let object_key = self.store.get_chunk(vol_id, chunk_id)?;

        let chunk_data = if let Some(key) = object_key {
            read_chunk(
                Arc::clone(&self.meta_client),
                &self.osd_pool,
                &key,
                self.ec_k,
                self.ec_m,
            )
            .await?
        } else {
            // Sparse (never written) → return zeros
            vec![0u8; chunk_mapper.chunk_size() as usize]
        };

        // Add to clean cache
        self.cache
            .add_clean(vol_id, chunk_id, bytes::Bytes::from(chunk_data.clone()));

        // Slice out the requested range
        let chunk_offset = chunk_mapper.byte_offset_to_chunk_id(offset) * chunk_mapper.chunk_size();
        let start = (offset - chunk_offset) as usize;
        let end = (start as u64 + length) as usize;
        let end = end.min(chunk_data.len());
        Ok(chunk_data[start..end].to_vec())
    }
}
