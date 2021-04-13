#![deny(missing_docs, rust_2018_idioms)]

//! A utility for reading from a stream and processing it by chunks in parallel.
//!
//! See [`read_stream_and_process_chunks_in_parallel`]() for details.

use std::io::{self, Read};
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::thread;

/// An error during reading or processing of a stream.
#[derive(Debug)]
pub enum Error<E> {
    /// An error occurred while reading from the source.
    Read(io::Error),

    /// An error was returned by a processing function.
    Process {
        /// The offset of the chunk that was being processed which led to the error.
        chunk_offset: u64,

        /// The error returned by the processing function.
        error: E,
    },
}

impl<E: std::fmt::Display> std::fmt::Display for Error<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Read(e) => write!(f, "error while reading: {}", e),
            Error::Process { chunk_offset, error } => write!(f,
                "error while processing data at chunk offset {}: {}", chunk_offset, error),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for Error<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Error::Read(ref e) => e,
            Error::Process { chunk_offset: _, ref error } => error,
        })
    }
}

fn start_worker_threads<E: Send + 'static>(
    num_threads: usize,
    work_rx: mpsc::Receiver<(u64, Vec<u8>)>,
    job_tx: mpsc::Sender<(u64, E)>,
    f: Arc<impl Fn(u64, &[u8]) -> Result<(), E> + Send + Sync + 'static>,
) -> Vec<thread::JoinHandle<()>> {
    let mut threads = vec![];
    let work_rx = Arc::new(Mutex::new(work_rx));
    for _ in 0 .. num_threads {
        let thread_work_rx = work_rx.clone();
        let thread_job_tx = job_tx.clone();
        let f = f.clone();

        threads.push(thread::spawn(move || {
            loop {
                let (offset, data) = {
                    let rx = thread_work_rx.lock().unwrap();

                    match rx.recv() {
                        Ok(result) => result,
                        Err(_) => {
                            // Sender end of the channel disconnected. Main thread must be done
                            // and waiting for us.
                            return;
                        }
                    }
                };

                if let Err(e) = f(offset, &data) {
                    // Job returned an error. Pass it to the main loop so it can stop early.
                    thread_job_tx.send((offset, e)).unwrap();
                }
            }
        }));
    }
    threads
}

/// Read from a stream and process it by chunks in parallel.
///
/// Reads are done sequentially on the current thread, in chunks of the given size, then the given
/// function is run on them in parallel on the given number of threads.
///
/// If any of the processing functions returns an error, reading and processing will be stopped as
/// soon as possible and the error returned to the caller. Note that because processing is
/// happening in parallel, it is possible for the processing and/or reading to go past the chunk
/// that causes an error, but it will stop soon thereafter.
///
/// Any read errors on the source will also stop further progress, but similarly, any ongoing
/// processing will need to finish before this function returns.
///
/// The error returned to the caller is only the first one encountered.
///
/// # Example:
/// ```
/// use parallel_reader::read_stream_and_process_chunks_in_parallel;
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
/// use std::io::Cursor;
///
/// let source = Cursor::new(vec![0u8; 12345]);
/// let num_bytes = Arc::new(AtomicU64::new(0));
/// let num_bytes_clone = num_bytes.clone();
/// let result = read_stream_and_process_chunks_in_parallel(source, 1024, 4, Arc::new(
///     move |_offset, data: &[u8]| -> Result<(), ()> {
///         // Trivial worker: just sum up the number of bytes in the data.
///         num_bytes_clone.fetch_add(data.len() as u64, SeqCst);
///         Ok(())
///     }));
/// assert!(result.is_ok());
/// assert_eq!(12345, num_bytes.load(SeqCst));
/// ```
pub fn read_stream_and_process_chunks_in_parallel<E: Send + 'static>(
    mut reader: impl Read,
    chunk_size: usize,
    num_threads: usize,
    f: Arc<impl Fn(u64, &[u8]) -> Result<(), E> + Send + Sync + 'static>,
) -> Result<(), Error<E>> {
    assert!(num_threads > 0, "non-zero number of threads required");

    // Channel for sending work as (offset, data) pairs to the worker threads.  It's bounded by the
    // number of workers, to ensure we don't read ahead of the work too far.
    let (work_tx, work_rx) = mpsc::sync_channel::<(u64, Vec<u8>)>(num_threads);

    // If a job returns an error result, it will be sent to this channel. This is used to stop
    // reading early if any job fails.
    let (job_tx, job_rx) = mpsc::channel::<(u64, E)>();

    // Start up workers.
    let threads = start_worker_threads(num_threads, work_rx, job_tx, f);

    // Read the file in chunks and pass work to worker threads.
    let mut offset = 0u64;
    let loop_result = loop {
        // Check if any job sent anything. They only send if there's an error, so if we get
        // something, stop the loop and pass the error up.
        match job_rx.try_recv() {
            Ok((chunk_offset, error)) => break Err(Error::Process { chunk_offset, error }),
            Err(mpsc::TryRecvError::Empty) => (),
            Err(mpsc::TryRecvError::Disconnected) => unreachable!("we hold the sender open"),
        }

        // TODO(wfraser) it'd be nice to re-use these buffers somehow
        let mut buf = vec![0u8; chunk_size];
        match reader.read(&mut buf) {
            Ok(0) => {
                break Ok(());
            }
            Ok(n) => {
                buf.truncate(n);
                work_tx.send((offset, buf)).expect("failed to send work to threads");
                offset += n as u64;
            }
            Err(e) => {
                break Err(Error::Read(e));
            }
        }
    };

    // Close the work channel. This'll cause the workers to exit next time they try to recv from it.
    drop(work_tx);

    // Loop is finished; wait for outstanding jobs to stop.
    for thread in threads {
        thread.join().expect("failed to join on worker thread");
    }

    if let Err(e) = loop_result {
        // Something stopped the loop prematurely: either a job failed or a read error occurred.
        // Return this error.
        return Err(e);
    }

    // Otherwise, the loop finished, but some job may have failed towards the end so check the
    // channel as well.
    match job_rx.recv() {
        Ok((chunk_offset, error)) => {
            // Some job returned an error.
            Err(Error::Process { chunk_offset, error })
        }
        Err(mpsc::RecvError) => {
            // No jobs returned any errors.
            Ok(())
        }
    }
}
