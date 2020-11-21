use std::io::{self, Read};
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::thread;

#[derive(Debug)]
pub enum Error<E> {
    /// An error occurred while reading from the source.
    Read(io::Error),

    /// An error was returned by a processing function.
    Process { chunk_offset: u64, error: E },
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
                            eprintln!("thread exiting");
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

    drop(work_rx);
    threads
}

pub fn parallel_chunked_read<E: Send + 'static>(
    mut reader: impl Read,
    chunk_size: usize,
    num_threads: usize,
    f: Arc<impl Fn(u64, &[u8]) -> Result<(), E> + Send + Sync + 'static>,
) -> Result<(), Error<E>> {
    // Channel for sending work as (offset, data) pairs to the worker threads.  It's bounded by the
    // number of workers, to ensure we don't read ahead of the work too far.
    let (work_tx, work_rx) = mpsc::sync_channel::<(u64, Vec<u8>)>(num_threads);

    // If a job returns an error result, it will be sent to this channel. This is used to stop
    // reading early if any job fails.
    let (job_tx, job_rx) = mpsc::channel::<(u64, E)>();

    // Start up workers.
    let threads = start_worker_threads(num_threads, work_rx, job_tx.clone(), f);

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

        let mut buf = vec![0u8; chunk_size];
        match reader.read(&mut buf) {
            Ok(0) => {
                break Ok(());
            }
            Ok(n) => {
                buf.truncate(n);
                work_tx.send((offset, buf)).unwrap();
                offset += n as u64;
            }
            Err(e) => {
                break Err(Error::Read(e));
            }
        }
    };

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
    match job_rx.try_recv() {
        Ok((chunk_offset, error)) => {
            // Some job returned an error.
            Err(Error::Process { chunk_offset, error })
        }
        Err(mpsc::TryRecvError::Empty) => {
            // No jobs returned any errors.
            Ok(())
        }
        Err(mpsc::TryRecvError::Disconnected) => unreachable!("we hold the sender open"),
    }
}
