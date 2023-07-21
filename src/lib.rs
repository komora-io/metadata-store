use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Read, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicPtr, AtomicU64, Ordering},
    mpsc, Arc, Mutex,
};

use fault_injection::{annotate, fallible, maybe};
use fnv::FnvHashMap;
use inline_array::InlineArray;
use rayon::prelude::*;
use zstd::stream::read::Decoder as ZstdDecoder;
use zstd::stream::write::Encoder as ZstdEncoder;

const WARN: &str = "DO_NOT_PUT_YOUR_FILES_HERE";
const TMP_SUFFIX: &str = ".tmp";
const LOG_PREFIX: &str = "log";
const SNAPSHOT_PREFIX: &str = "snapshot";

const ZSTD_LEVEL: i32 = 3;

pub struct MetadataStore {
    inner: Inner,
    is_shut_down: bool,
}

impl Drop for MetadataStore {
    fn drop(&mut self) {
        if self.is_shut_down {
            return;
        }

        self.shutdown_inner();
        self.is_shut_down = true;
    }
}

struct Recovery {
    recovered: Vec<(u64, NonZeroU64, InlineArray)>,
    id_for_next_log: u64,
    snapshot_size: u64,
}

struct LogAndStats {
    file: fs::File,
    bytes_written: u64,
    log_sequence_number: u64,
}

enum WorkerMessage {
    Shutdown(mpsc::Sender<()>),
    LogReadyToCompact { log_and_stats: LogAndStats },
}

fn get_compactions(
    rx: &mut mpsc::Receiver<WorkerMessage>,
) -> Result<Vec<u64>, Option<mpsc::Sender<()>>> {
    let mut ret = vec![];

    match rx.recv() {
        Ok(WorkerMessage::Shutdown(tx)) => {
            return Err(Some(tx));
        }
        Ok(WorkerMessage::LogReadyToCompact { log_and_stats }) => {
            ret.push(log_and_stats.log_sequence_number);
        }
        Err(e) => {
            log::error!(
                    "metadata store worker thread unable to receive message, unexpected shutdown: {e:?}"
                );
            return Err(None);
        }
    }

    // scoop up any additional logs that have built up while we were busy compacting
    loop {
        match rx.try_recv() {
            Ok(WorkerMessage::Shutdown(tx)) => {
                tx.send(()).unwrap();
                return Err(Some(tx));
            }
            Ok(WorkerMessage::LogReadyToCompact { log_and_stats }) => {
                ret.push(log_and_stats.log_sequence_number);
            }
            Err(_timeout) => return Ok(ret),
        }
    }
}

fn worker(mut rx: mpsc::Receiver<WorkerMessage>, mut last_snapshot_lsn: u64, inner: Inner) {
    loop {
        let err_ptr: *const (io::ErrorKind, String) = inner.global_error.load(Ordering::Acquire);

        if !err_ptr.is_null() {
            log::error!("compaction thread prematurely terminating after global error set");
            return;
        }

        match get_compactions(&mut rx) {
            Ok(log_ids) => {
                assert_eq!(log_ids[0], last_snapshot_lsn + 1);

                let write_res = read_snapshot_and_apply_logs(
                    &inner.storage_directory,
                    log_ids.into_iter().collect(),
                    Some(last_snapshot_lsn),
                );
                match write_res {
                    Err(e) => {
                        set_error(&inner.global_error, &e);
                        log::error!("log compactor thread encountered error - setting global fatal error and shutting down compactions");
                        return;
                    }
                    Ok(recovery) => {
                        inner
                            .snapshot_size
                            .store(recovery.snapshot_size, Ordering::Release);
                        last_snapshot_lsn = recovery.id_for_next_log.checked_sub(1).unwrap();
                    }
                }
            }
            Err(Some(tx)) => {
                drop(inner);
                if let Err(e) = tx.send(()) {
                    log::error!("log compactor failed to send shutdown ack to system: {e:?}");
                }
                return;
            }
            Err(None) => {
                return;
            }
        }
    }
}

fn set_error(global_error: &AtomicPtr<(io::ErrorKind, String)>, error: &io::Error) {
    let kind = error.kind();
    let reason = error.to_string();

    let boxed = Box::new((kind, reason));
    let ptr = Box::into_raw(boxed);

    if global_error
        .compare_exchange(
            std::ptr::null_mut(),
            ptr,
            Ordering::SeqCst,
            Ordering::SeqCst,
        )
        .is_err()
    {
        // global fatal error already installed, drop this one
        unsafe {
            drop(Box::from_raw(ptr));
        }
    }
}

#[derive(Clone)]
struct Inner {
    global_error: Arc<AtomicPtr<(io::ErrorKind, String)>>,
    active_log: Arc<Mutex<LogAndStats>>,
    snapshot_size: Arc<AtomicU64>,
    storage_directory: PathBuf,
    #[allow(unused)]
    directory_lock: Arc<fs::File>,
    worker_outbox: mpsc::Sender<WorkerMessage>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        let error_ptr = self.global_error.load(Ordering::Acquire);
        if !error_ptr.is_null() {
            unsafe {
                drop(Box::from_raw(error_ptr));
            }
        }
    }
}

impl MetadataStore {
    pub fn shutdown(mut self) {
        self.shutdown_inner();
    }

    fn shutdown_inner(&mut self) {
        let (tx, rx) = mpsc::channel();
        if self
            .inner
            .worker_outbox
            .send(WorkerMessage::Shutdown(tx))
            .is_ok()
        {
            let _ = rx.recv();
        }

        self.set_error(&io::Error::new(
            io::ErrorKind::Other,
            "system has been shut down".to_string(),
        ));

        self.is_shut_down = true;
    }

    fn check_error(&self) -> io::Result<()> {
        let err_ptr: *const (io::ErrorKind, String) =
            self.inner.global_error.load(Ordering::Acquire);

        if err_ptr.is_null() {
            Ok(())
        } else {
            let deref: &(io::ErrorKind, String) = unsafe { &*err_ptr };
            Err(io::Error::new(deref.0, deref.1.clone()))
        }
    }

    fn set_error(&self, error: &io::Error) {
        set_error(&self.inner.global_error, error);
    }

    /// Returns the writer handle `MetadataStore`, a sorted array of metadata, and a sorted array
    /// of free keys.
    pub fn recover<P: AsRef<Path>>(
        storage_directory: P,
    ) -> io::Result<(
        // Metadata writer
        MetadataStore,
        // Metadata - key, value, user data
        Vec<(u64, NonZeroU64, InlineArray)>,
    )> {
        use fs2::FileExt;

        let path = storage_directory.as_ref();

        // initialize directories if not present
        if let Err(e) = fs::read_dir(&path) {
            if e.kind() == io::ErrorKind::NotFound {
                fallible!(fs::create_dir_all(&path));
            }
        }

        let _ = fs::File::create(path.join(WARN));

        let directory_lock = fallible!(fs::File::open(path));
        fallible!(directory_lock.try_lock_exclusive());

        let recovery = MetadataStore::recover_inner(&storage_directory)?;

        let new_log = LogAndStats {
            log_sequence_number: recovery.id_for_next_log,
            bytes_written: 0,
            file: fallible!(fs::File::create(log_path(path, recovery.id_for_next_log))),
        };

        let (tx, rx) = mpsc::channel();

        let inner = Inner {
            snapshot_size: Arc::new(recovery.snapshot_size.into()),
            storage_directory: path.into(),
            directory_lock: Arc::new(directory_lock),
            global_error: Default::default(),
            active_log: Arc::new(Mutex::new(new_log)),
            worker_outbox: tx,
        };

        let worker_inner = inner.clone();
        std::thread::spawn(move || {
            worker(
                rx,
                recovery.id_for_next_log.checked_sub(1).unwrap(),
                worker_inner,
            )
        });

        Ok((
            MetadataStore {
                inner,
                is_shut_down: false,
            },
            recovery.recovered,
        ))
    }

    /// Returns the recovered mappings, the id for the next log file, the highest allocated object id, and the set of free ids
    fn recover_inner<P: AsRef<Path>>(storage_directory: P) -> io::Result<Recovery> {
        let path = storage_directory.as_ref();
        log::debug!("opening u64db at {:?}", path);

        // lock the main storage directory
        let mut file_lock_opts = fs::OpenOptions::new();
        file_lock_opts.create(false).read(false).write(false);

        let (log_ids, snapshot_id_opt) = enumerate_logs_and_snapshot(&path)?;

        read_snapshot_and_apply_logs(path, log_ids, snapshot_id_opt)
    }

    /// Write a batch of metadata. `None` for the second half of the outer tuple represents a
    /// deletion.
    pub fn insert_batch<I: IntoIterator<Item = (u64, Option<(NonZeroU64, InlineArray)>)>>(
        &self,
        batch: I,
    ) -> io::Result<()> {
        self.check_error()?;

        let batch_bytes = serialize_batch(batch);

        let mut log = self.inner.active_log.lock().unwrap();

        if let Err(e) = maybe!(log.file.write_all(&batch_bytes)) {
            self.set_error(&e);
            return Err(e);
        }

        if let Err(e) = maybe!(log.file.sync_all()) {
            self.set_error(&e);
            return Err(e);
        }

        log.bytes_written += batch_bytes.len() as u64;

        if log.bytes_written
            > self
                .inner
                .snapshot_size
                .load(Ordering::Acquire)
                .max(64 * 1024)
        {
            let next_offset = log.log_sequence_number + 1;
            let next_path = log_path(&self.inner.storage_directory, next_offset);

            // open new log
            let mut next_log_file_opts = fs::OpenOptions::new();
            next_log_file_opts.create(true).read(true).write(true);

            let next_log_file = match maybe!(next_log_file_opts.open(next_path)) {
                Ok(nlf) => nlf,
                Err(e) => {
                    self.set_error(&e);
                    return Err(e);
                }
            };

            let next_log_and_stats = LogAndStats {
                file: next_log_file,
                log_sequence_number: next_offset,
                bytes_written: 0,
            };

            // replace log
            let old_log_and_stats = std::mem::replace(&mut *log, next_log_and_stats);

            // send to snapshot writer
            self.inner
                .worker_outbox
                .send(WorkerMessage::LogReadyToCompact {
                    log_and_stats: old_log_and_stats,
                })
                .expect("unable to send log to compact to worker");
        }

        Ok(())
    }
}

fn serialize_batch<I: IntoIterator<Item = (u64, Option<(NonZeroU64, InlineArray)>)>>(
    batch: I,
) -> Vec<u8> {
    // we initialize the vector to contain placeholder bytes for the frame length
    let batch_bytes = 0_u64.to_le_bytes().to_vec();

    // write format:
    //  8 byte LE frame length (in bytes, not items)
    //  payload:
    //      zstd encoded 8 byte LE key
    //      zstd encoded 8 byte LE value
    //      repeated for each kv pair
    //  LE encoded crc32 of length + payload raw bytes, XOR 0xAF to make non-zero in empty case
    let mut batch_encoder = ZstdEncoder::new(batch_bytes, ZSTD_LEVEL).unwrap();

    for (k, v_opt) in batch {
        batch_encoder.write_all(&k.to_le_bytes()).unwrap();
        if let Some((v, user_data)) = v_opt {
            batch_encoder.write_all(&v.get().to_le_bytes()).unwrap();

            let user_data_len: u64 = user_data.len() as u64;
            batch_encoder
                .write_all(&user_data_len.to_le_bytes())
                .unwrap();
            batch_encoder.write_all(&user_data).unwrap();
        } else {
            // v
            batch_encoder.write_all(&0_u64.to_le_bytes()).unwrap();
            // user data len
            batch_encoder.write_all(&0_u64.to_le_bytes()).unwrap();
        }
    }

    let mut batch_bytes = batch_encoder.finish().unwrap();

    let batch_len = batch_bytes.len().checked_sub(8).unwrap();
    batch_bytes[..8].copy_from_slice(&batch_len.to_le_bytes());

    let hash: u32 = crc32fast::hash(&batch_bytes) ^ 0xAF;
    let hash_bytes: [u8; 4] = hash.to_le_bytes();
    batch_bytes.extend_from_slice(&hash_bytes);

    batch_bytes
}

fn read_frame(
    file: &mut fs::File,
    reusable_frame_buffer: &mut Vec<u8>,
) -> io::Result<Vec<(u64, (u64, InlineArray))>> {
    let mut frame_size_buf: [u8; 8] = [0; 8];
    // TODO only break if UnexpectedEof, otherwise propagate
    fallible!(file.read_exact(&mut frame_size_buf));

    let len_u64: u64 = u64::from_le_bytes(frame_size_buf);
    // TODO make sure len < max len
    let len: usize = usize::try_from(len_u64).unwrap();

    reusable_frame_buffer.clear();
    reusable_frame_buffer.reserve(len + 12);
    unsafe {
        reusable_frame_buffer.set_len(len + 12);
    }
    reusable_frame_buffer[..8].copy_from_slice(&frame_size_buf);

    fallible!(file.read_exact(&mut reusable_frame_buffer[8..]));

    let crc_actual = crc32fast::hash(&reusable_frame_buffer[..len + 8]) ^ 0xAF;
    let crc_recorded = u32::from_le_bytes([
        reusable_frame_buffer[len + 8],
        reusable_frame_buffer[len + 9],
        reusable_frame_buffer[len + 10],
        reusable_frame_buffer[len + 11],
    ]);

    if crc_actual != crc_recorded {
        log::warn!("encountered incorrect crc for batch in log");
        return Err(annotate!(io::Error::new(
            io::ErrorKind::InvalidData,
            "crc mismatch for read of batch frame",
        )));
    }

    let mut ret = vec![];

    let mut decoder = ZstdDecoder::new(&reusable_frame_buffer[8..len + 8])
        .expect("failed to create zstd decoder");

    let mut k_buf: [u8; 8] = [0; 8];
    let mut v_buf: [u8; 8] = [0; 8];
    let mut user_data_len_buf: [u8; 8] = [0; 8];
    let mut user_data_buf = vec![];
    loop {
        let first_read_res = decoder.read_exact(&mut k_buf);
        if let Err(e) = first_read_res {
            if e.kind() != io::ErrorKind::UnexpectedEof {
                return Err(e);
            } else {
                break;
            }
        }
        decoder
            .read_exact(&mut v_buf)
            .expect("we expect reads from crc-verified buffers to succeed");
        decoder
            .read_exact(&mut user_data_len_buf)
            .expect("we expect reads from crc-verified buffers to succeed");

        let k = u64::from_le_bytes(k_buf);
        let v = u64::from_le_bytes(v_buf);

        let user_data_len_raw = u64::from_le_bytes(user_data_len_buf);
        let user_data_len = usize::try_from(user_data_len_raw).unwrap();
        user_data_buf.reserve(user_data_len);
        unsafe {
            user_data_buf.set_len(user_data_len);
        }

        decoder
            .read_exact(&mut user_data_buf)
            .expect("we expect reads from crc-verified buffers to succeed");

        let user_data = InlineArray::from(&*user_data_buf);

        ret.push((k, (v, user_data)));
    }

    Ok(ret)
}

// returns the deduplicated data in this log, along with an optional offset where a
// final torn write occurred.
fn read_log(directory_path: &Path, lsn: u64) -> io::Result<FnvHashMap<u64, (u64, InlineArray)>> {
    log::info!("reading log {lsn}");
    let mut ret = FnvHashMap::default();

    let mut file = fallible!(fs::File::open(log_path(directory_path, lsn)));

    let mut reusable_frame_buffer: Vec<u8> = vec![];

    while let Ok(frame) = read_frame(&mut file, &mut reusable_frame_buffer) {
        for (k, v) in frame.into_iter() {
            ret.insert(k, v);
        }
    }

    log::info!("recovered {} items in log {}", ret.len(), lsn);

    Ok(ret)
}

/// returns the data from the snapshot as well as the size of the snapshot
fn read_snapshot(
    directory_path: &Path,
    lsn: u64,
) -> io::Result<(FnvHashMap<u64, (NonZeroU64, InlineArray)>, u64)> {
    log::info!("reading snapshot {lsn}");
    let mut reusable_frame_buffer: Vec<u8> = vec![];
    let mut file = fallible!(fs::File::open(snapshot_path(directory_path, lsn, false)));
    let size = fallible!(file.metadata()).len();
    let raw_frame = read_frame(&mut file, &mut reusable_frame_buffer)?;

    let frame: FnvHashMap<u64, (NonZeroU64, InlineArray)> = raw_frame
        .into_iter()
        .map(|(k, (v, user_data))| (k, (NonZeroU64::new(v).unwrap(), user_data)))
        .collect();

    log::info!("recovered {} items in snapshot {}", frame.len(), lsn);

    Ok((frame, size))
}

fn log_path(directory_path: &Path, id: u64) -> PathBuf {
    directory_path.join(format!("{LOG_PREFIX}_{:016x}", id))
}

fn snapshot_path(directory_path: &Path, id: u64, temporary: bool) -> PathBuf {
    if temporary {
        directory_path.join(format!("{SNAPSHOT_PREFIX}_{:016x}{TMP_SUFFIX}", id))
    } else {
        directory_path.join(format!("{SNAPSHOT_PREFIX}_{:016x}", id))
    }
}

fn enumerate_logs_and_snapshot(directory_path: &Path) -> io::Result<(BTreeSet<u64>, Option<u64>)> {
    let mut logs = BTreeSet::new();
    let mut snapshot: Option<u64> = None;

    for dir_entry_res in fallible!(fs::read_dir(directory_path)) {
        let dir_entry = fallible!(dir_entry_res);
        let file_name = if let Ok(f) = dir_entry.file_name().into_string() {
            f
        } else {
            log::warn!(
                "skipping unexpected file with non-unicode name {:?}",
                dir_entry.file_name()
            );
            continue;
        };

        if file_name.ends_with(TMP_SUFFIX) {
            log::warn!("removing incomplete snapshot rewrite {file_name:?}");
            fallible!(fs::remove_file(directory_path.join(file_name)));
        } else if file_name.starts_with(LOG_PREFIX) {
            let start = LOG_PREFIX.len() + 1;
            let stop = start + 16;

            if let Ok(id) = u64::from_str_radix(&file_name[start..stop], 16) {
                logs.insert(id);
            } else {
                todo!()
            }
        } else if file_name.starts_with(SNAPSHOT_PREFIX) {
            let start = SNAPSHOT_PREFIX.len() + 1;
            let stop = start + 16;

            if let Ok(id) = u64::from_str_radix(&file_name[start..stop], 16) {
                if let Some(snap_id) = snapshot {
                    if snap_id < id {
                        log::warn!(
                            "removing stale snapshot {id} that is superceded by snapshot {id}"
                        );

                        fallible!(fs::remove_file(file_name));

                        snapshot = Some(id);
                    }
                } else {
                    snapshot = Some(id);
                }
            } else {
                todo!()
            }
        }
    }

    let snap_id = snapshot.unwrap_or(0);
    for stale_log_id in logs.range(..=snap_id) {
        let file_name = log_path(directory_path, *stale_log_id);

        log::warn!("removing stale log {file_name:?} that is contained within snapshot {snap_id}");

        fallible!(fs::remove_file(file_name));
    }
    logs.retain(|l| *l > snap_id);

    Ok((logs, snapshot))
}

fn read_snapshot_and_apply_logs(
    path: &Path,
    log_ids: BTreeSet<u64>,
    snapshot_id_opt: Option<u64>,
) -> io::Result<Recovery> {
    let snapshot: FnvHashMap<u64, (NonZeroU64, InlineArray)> =
        if let Some(snapshot_id) = snapshot_id_opt {
            read_snapshot(&path, snapshot_id)?.0
        } else {
            Default::default()
        };

    let mut recovered = snapshot;

    let mut max_log_id = snapshot_id_opt.unwrap_or(0);

    let log_data_res: io::Result<Vec<(u64, FnvHashMap<u64, (u64, InlineArray)>)>> =
        (&log_ids) //.iter().collect::<Vec<_>>())
            .into_par_iter()
            .map(move |log_id| {
                if let Some(snapshot_id) = snapshot_id_opt {
                    assert!(*log_id > snapshot_id);
                }

                let log_datum = read_log(&path, *log_id)?;

                Ok((*log_id, log_datum))
            })
            .collect();

    for (log_id, log_datum) in log_data_res? {
        max_log_id = max_log_id.max(log_id);

        for (k, (v, user_data)) in log_datum {
            if v == 0 {
                recovered.remove(&k);
            } else {
                recovered.insert(k, (NonZeroU64::new(v).unwrap(), user_data));
            }
        }
    }

    let mut recovered: Vec<(u64, NonZeroU64, InlineArray)> = recovered
        .into_iter()
        .map(|(k, (v, user_data))| (k, v, user_data))
        .collect();

    recovered.par_sort_unstable();

    // write fresh snapshot with recovered data
    let new_snapshot_data = serialize_batch(
        recovered
            .iter()
            .map(|(k, v, user_data)| (*k, Some((*v, user_data.clone())))),
    );
    let snapshot_size = new_snapshot_data.len() as u64;

    let new_snapshot_tmp_path = snapshot_path(path, max_log_id, true);
    log::info!("writing snapshot to {new_snapshot_tmp_path:?}");

    let mut snapshot_file_opts = fs::OpenOptions::new();
    snapshot_file_opts.create(true).read(false).write(true);

    let mut snapshot_file = fallible!(snapshot_file_opts.open(&new_snapshot_tmp_path));

    fallible!(snapshot_file.write_all(&new_snapshot_data));
    drop(new_snapshot_data);

    fallible!(snapshot_file.sync_all());

    let new_snapshot_path = snapshot_path(path, max_log_id, false);
    log::info!("renaming written snapshot to {new_snapshot_path:?}");
    fallible!(fs::rename(new_snapshot_tmp_path, new_snapshot_path));
    fallible!(fs::File::open(path).and_then(|directory| directory.sync_all()));

    for log_id in &log_ids {
        let log_path = log_path(path, *log_id);
        fallible!(fs::remove_file(log_path));
    }

    if let Some(old_snapshot_id) = snapshot_id_opt {
        let old_snapshot_path = snapshot_path(path, old_snapshot_id, false);
        fallible!(fs::remove_file(old_snapshot_path));
    }

    Ok(Recovery {
        recovered,
        id_for_next_log: max_log_id + 1,
        snapshot_size,
    })
}
