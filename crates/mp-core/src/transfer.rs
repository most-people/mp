use std::path::PathBuf;

use async_trait::async_trait;
use cid::Cid;
use peeroxide::SwarmConnection;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    Control, Frame, Holding, MAX_FILE_SIZE, MpError, ObjectStore, Result, decode_frame,
    encode_control, encode_data, parse_file_cid,
};

/// Message-oriented encrypted connection used by the file protocol.
#[async_trait]
pub trait MessageConnection: Send {
    /// Read one plaintext message, preserving the transport boundary.
    async fn read_message(&mut self) -> Result<Option<Vec<u8>>>;

    /// Write one plaintext message.
    async fn write_message(&mut self, message: &[u8]) -> Result<()>;

    /// Gracefully close the write side.
    async fn shutdown(&mut self) -> Result<()>;
}

#[async_trait]
impl MessageConnection for SwarmConnection {
    async fn read_message(&mut self) -> Result<Option<Vec<u8>>> {
        self.peer
            .stream
            .read()
            .await
            .map_err(|error| MpError::Network(error.to_string()))
    }

    async fn write_message(&mut self, message: &[u8]) -> Result<()> {
        self.peer
            .stream
            .write(message)
            .await
            .map_err(|error| MpError::Network(error.to_string()))
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.peer
            .stream
            .shutdown()
            .await
            .map_err(|error| MpError::Network(error.to_string()))
    }
}

/// Receive result after an object has been verified and promoted.
#[derive(Clone, Debug)]
pub struct ReceivedFile {
    /// Persisted holding.
    pub holding: Holding,
    /// Canonical content-addressed object path.
    pub object_path: PathBuf,
}

/// Serve exactly one validated file request over an established connection.
pub async fn serve_file<C>(store: ObjectStore, mut connection: C) -> Result<()>
where
    C: MessageConnection,
{
    let request = match connection.read_message().await? {
        Some(message) => match decode_frame(&message) {
            Ok(Frame::Control(Control::Request { cid, .. })) => match parse_file_cid(&cid) {
                Ok(cid) => cid,
                Err(error) => {
                    return reject(&mut connection, "bad_request", &error.to_string()).await;
                }
            },
            Ok(_) => {
                return reject(
                    &mut connection,
                    "bad_request",
                    "first frame must be a request",
                )
                .await;
            }
            Err(error) => {
                return reject(&mut connection, "bad_request", &error.to_string()).await;
            }
        },
        None => {
            return Err(MpError::Protocol(
                "connection closed before request".to_string(),
            ));
        }
    };

    let store_for_validation = store.clone();
    let request_for_validation = request;
    let holding = match tokio::task::spawn_blocking(move || {
        store_for_validation.find_verified(&request_for_validation)
    })
    .await
    .map_err(|error| MpError::Network(format!("validation task failed: {error}")))?
    {
        Ok(holding) => holding,
        Err(MpError::NotFound(_)) => {
            return reject(&mut connection, "not_found", "object is not held locally").await;
        }
        Err(error) => {
            return reject(&mut connection, "not_found", &error.to_string()).await;
        }
    };

    let cid = request.to_string();
    write_control(
        &mut connection,
        &Control::offer(&cid, &holding.filename, holding.size),
    )
    .await?;

    let mut file = File::open(store.object_path(&request)).await?;
    let mut sent = 0u64;
    let mut buffer = vec![0u8; crate::MAX_DATA_FRAME_SIZE];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        connection
            .write_message(&encode_data(&buffer[..read])?)
            .await?;
        sent += read as u64;
    }
    if sent != holding.size {
        return Err(MpError::Integrity {
            expected: format!("{} bytes", holding.size),
            actual: format!("{sent} bytes"),
        });
    }

    write_control(&mut connection, &Control::complete(cid, sent)).await?;
    connection.shutdown().await
}

/// Request, receive, verify, and atomically persist one complete object.
pub async fn receive_file<C>(
    store: ObjectStore,
    expected_cid: &Cid,
    mut connection: C,
) -> Result<ReceivedFile>
where
    C: MessageConnection,
{
    write_control(&mut connection, &Control::request(expected_cid.to_string())).await?;

    let offer = read_frame(&mut connection).await?;
    let (filename, expected_size) = match offer {
        Frame::Control(Control::Offer {
            cid,
            filename,
            size,
            ..
        }) => {
            require_cid(expected_cid, &cid)?;
            if size > MAX_FILE_SIZE {
                return Err(MpError::FileTooLarge {
                    size,
                    max: MAX_FILE_SIZE,
                });
            }
            (filename, size)
        }
        Frame::Control(Control::Error { code, message, .. }) => {
            return Err(MpError::Protocol(format!("remote {code}: {message}")));
        }
        _ => {
            return Err(MpError::Protocol(
                "expected offer as first response".to_string(),
            ));
        }
    };

    let temp_path = store.temporary_object_path();
    let mut temp_guard = TempDownloadGuard::new(temp_path.clone());
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .await?;
    let mut received = 0u64;

    loop {
        match read_frame(&mut connection).await? {
            Frame::Data(data) => {
                received = received.checked_add(data.len() as u64).ok_or_else(|| {
                    MpError::Protocol("received byte count overflowed".to_string())
                })?;
                if received > expected_size {
                    return Err(MpError::Integrity {
                        expected: format!("{expected_size} bytes"),
                        actual: format!("at least {received} bytes"),
                    });
                }
                output.write_all(&data).await?;
            }
            Frame::Control(Control::Complete { cid, size, .. }) => {
                require_cid(expected_cid, &cid)?;
                if size != expected_size || received != expected_size {
                    return Err(MpError::Integrity {
                        expected: format!("{expected_size} bytes"),
                        actual: format!("offer={size} received={received}"),
                    });
                }
                break;
            }
            Frame::Control(Control::Error { code, message, .. }) => {
                return Err(MpError::Protocol(format!("remote {code}: {message}")));
            }
            Frame::Control(_) => {
                return Err(MpError::Protocol(
                    "unexpected control frame during transfer".to_string(),
                ));
            }
        }
    }

    output.flush().await?;
    output.sync_all().await?;
    drop(output);

    let store_for_commit = store.clone();
    let temp_for_commit = temp_path.clone();
    let cid_for_commit = *expected_cid;
    let holding = tokio::task::spawn_blocking(move || {
        store_for_commit.commit_download(temp_for_commit, &cid_for_commit, &filename, expected_size)
    })
    .await
    .map_err(|error| MpError::Network(format!("commit task failed: {error}")))??;
    temp_guard.disarm();

    Ok(ReceivedFile {
        object_path: store.object_path(expected_cid),
        holding,
    })
}

async fn write_control<C>(connection: &mut C, control: &Control) -> Result<()>
where
    C: MessageConnection,
{
    connection.write_message(&encode_control(control)?).await
}

async fn read_frame<C>(connection: &mut C) -> Result<Frame>
where
    C: MessageConnection,
{
    let message = connection
        .read_message()
        .await?
        .ok_or_else(|| MpError::Protocol("unexpected end of encrypted stream".to_string()))?;
    decode_frame(&message)
}

async fn reject<C>(connection: &mut C, code: &str, message: &str) -> Result<()>
where
    C: MessageConnection,
{
    write_control(connection, &Control::error(code, message)).await?;
    connection.shutdown().await
}

fn require_cid(expected: &Cid, actual: &str) -> Result<()> {
    let actual = parse_file_cid(actual)?;
    if &actual == expected {
        Ok(())
    } else {
        Err(MpError::Integrity {
            expected: expected.to_string(),
            actual: actual.to_string(),
        })
    }
}

struct TempDownloadGuard {
    path: PathBuf,
    armed: bool,
}

impl TempDownloadGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TempDownloadGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use super::*;
    use crate::{calculate_bytes_cid, encode_control};

    struct MemoryConnection {
        tx: mpsc::Sender<Vec<u8>>,
        rx: mpsc::Receiver<Vec<u8>>,
    }

    fn memory_pair() -> (MemoryConnection, MemoryConnection) {
        let (left_tx, right_rx) = mpsc::channel(16);
        let (right_tx, left_rx) = mpsc::channel(16);
        (
            MemoryConnection {
                tx: left_tx,
                rx: left_rx,
            },
            MemoryConnection {
                tx: right_tx,
                rx: right_rx,
            },
        )
    }

    #[async_trait]
    impl MessageConnection for MemoryConnection {
        async fn read_message(&mut self) -> Result<Option<Vec<u8>>> {
            Ok(self.rx.recv().await)
        }

        async fn write_message(&mut self, message: &[u8]) -> Result<()> {
            self.tx
                .send(message.to_vec())
                .await
                .map_err(|_| MpError::Network("memory connection closed".to_string()))
        }

        async fn shutdown(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn transfers_and_persists_a_verified_file() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source.bin");
        std::fs::write(&source, vec![42u8; 130_000]).unwrap();
        let seed_store = ObjectStore::open(temp.path().join("seed")).unwrap();
        let imported = seed_store.import_file(&source).unwrap();
        let receiver_store = ObjectStore::open(temp.path().join("receiver")).unwrap();
        let (seed_connection, receiver_connection) = memory_pair();

        let seed = tokio::spawn(serve_file(seed_store, seed_connection));
        let received = receive_file(
            receiver_store.clone(),
            imported.link.cid(),
            receiver_connection,
        )
        .await
        .unwrap();
        seed.await.unwrap().unwrap();

        assert_eq!(received.holding.cid, imported.holding.cid);
        assert_eq!(
            std::fs::read(received.object_path).unwrap(),
            vec![42u8; 130_000]
        );
        assert_eq!(receiver_store.validate_holdings().unwrap().valid.len(), 1);
    }

    #[tokio::test]
    async fn cid_mismatch_never_creates_a_holding() {
        let temp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(temp.path().join("receiver")).unwrap();
        let expected = calculate_bytes_cid(b"expected").unwrap();
        let (mut sender, receiver) = memory_pair();
        let expected_for_sender = expected;

        let send = tokio::spawn(async move {
            let _request = sender.read_message().await.unwrap().unwrap();
            sender
                .write_message(
                    &encode_control(&Control::offer(
                        expected_for_sender.to_string(),
                        "bad.bin",
                        3,
                    ))
                    .unwrap(),
                )
                .await
                .unwrap();
            sender
                .write_message(&encode_data(b"bad").unwrap())
                .await
                .unwrap();
            sender
                .write_message(
                    &encode_control(&Control::complete(expected_for_sender.to_string(), 3))
                        .unwrap(),
                )
                .await
                .unwrap();
        });

        let result = receive_file(store.clone(), &expected, receiver).await;
        send.await.unwrap();
        assert!(matches!(result, Err(MpError::Integrity { .. })));
        assert!(store.holdings().unwrap().is_empty());
        assert!(
            std::fs::read_dir(temp.path().join("receiver/objects"))
                .unwrap()
                .next()
                .is_none()
        );
    }

    #[tokio::test]
    async fn size_mismatch_removes_the_temporary_file() {
        let temp = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(temp.path().join("receiver")).unwrap();
        let expected = calculate_bytes_cid(b"abc").unwrap();
        let (mut sender, receiver) = memory_pair();
        let expected_for_sender = expected;

        let send = tokio::spawn(async move {
            let _request = sender.read_message().await.unwrap().unwrap();
            sender
                .write_message(
                    &encode_control(&Control::offer(
                        expected_for_sender.to_string(),
                        "short.bin",
                        4,
                    ))
                    .unwrap(),
                )
                .await
                .unwrap();
            sender
                .write_message(&encode_data(b"abc").unwrap())
                .await
                .unwrap();
            sender
                .write_message(
                    &encode_control(&Control::complete(expected_for_sender.to_string(), 4))
                        .unwrap(),
                )
                .await
                .unwrap();
        });

        let result = receive_file(store.clone(), &expected, receiver).await;
        send.await.unwrap();
        assert!(matches!(result, Err(MpError::Integrity { .. })));
        assert!(store.holdings().unwrap().is_empty());
        assert!(
            std::fs::read_dir(temp.path().join("receiver/objects"))
                .unwrap()
                .next()
                .is_none()
        );
    }
}
