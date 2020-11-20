use std::io::{self, Read};
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::thread;

#[derive(Debug)]
pub enum ParallelChunkedReadError<E> {
    Read(io::Error),
    Process(E),
}

pub fn parallel_chunked_read<E: Send + 'static>(
    mut reader: impl Read,
    chunk_size: usize,
    num_threads: usize,
    f: Arc<impl Fn(u64, &[u8]) -> Result<(), E> + Send + Sync + 'static>,
) -> Result<(), ParallelChunkedReadError<E>> {
    // Channel for sending work as (offset, data) pairs to the worker threads.  It's bounded by the
    // number of workers, to ensure we don't read ahead of the work too far.
    let (work_tx, work_rx) = mpsc::sync_channel::<(u64, Vec<u8>)>(num_threads);

    // If a job returns an error result, it will be sent to this channel. This is used to stop
    // reading early if any job fails.
    let (job_tx, job_rx) = mpsc::channel::<E>();

    // Start up workers.
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
                    thread_job_tx.send(e).unwrap();
                }
            }
        }));
    }

    drop(work_rx);

    // Read the file in chunks and pass work to worker threads.
    let mut offset = 0u64;
    let loop_result = loop {
        // Check if any job sent anything. They only send if there's an error, so if we get
        // something, stop the loop and pass the error up.
        match job_rx.try_recv() {
            Ok(e) => break Err(ParallelChunkedReadError::Process(e)),
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
                break Err(ParallelChunkedReadError::Read(e));
            }
        }
    };

    drop(work_tx);

    // Loop is finished; wait for outstanding jobs to stop.
    for thread in threads {
        thread.join().expect("failed to join on worker thread");
    }
    //self.threadpool.join();

    if let Err(e) = loop_result {
        // Something stopped the loop prematurely: either a job failed or a read error occurred.
        // Return this error.
        return Err(e);
    }

    // Otherwise, the loop finished, but some job may have failed towards the end so check the
    // channel as well.
    match job_rx.try_recv() {
        Ok(e) => {
            // Some job returned an error.
            Err(ParallelChunkedReadError::Process(e))
        }
        Err(mpsc::TryRecvError::Empty) => {
            // No jobs returned any errors.
            Ok(())
        }
        Err(mpsc::TryRecvError::Disconnected) => unreachable!("we hold the sender open"),
    }
}
